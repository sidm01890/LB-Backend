"""
Excel Generation MongoDB Service
Handles all Excel Generation operations using MongoDB instead of MySQL
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from bson import ObjectId
from bson.errors import InvalidId
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from app.config.mongodb import get_mongodb_collection, get_mongodb_database
import enum

logger = logging.getLogger(__name__)


class ExcelGenerationStatus(str, enum.Enum):
    """Excel generation status enum"""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class ExcelGenerationService:
    """Service for Excel Generation operations using MongoDB"""
    
    COLLECTION_NAME = "excel_generations"
    
    @staticmethod
    def _get_collection():
        """Get MongoDB collection for excel_generations"""
        try:
            return get_mongodb_collection(ExcelGenerationService.COLLECTION_NAME)
        except Exception as e:
            logger.error(f"❌ Error getting MongoDB collection '{ExcelGenerationService.COLLECTION_NAME}': {e}")
            raise ConnectionError(f"MongoDB is not connected: {e}")
    
    @staticmethod
    def _create_indexes():
        """Create indexes for better query performance"""
        try:
            collection = ExcelGenerationService._get_collection()
            
            # Create indexes
            collection.create_index("created_at", background=True)
            collection.create_index("status", background=True)
            collection.create_index("store_code", background=True)
            collection.create_index([("status", 1), ("created_at", -1)], background=True)
            collection.create_index([("status", 1), ("created_at", -1), ("store_code", 1)], background=True)
            
            logger.info(f"✅ MongoDB indexes created for '{ExcelGenerationService.COLLECTION_NAME}'")
        except Exception as e:
            logger.warning(f"⚠️ Failed to create indexes for excel_generations: {e}")
    
    @staticmethod
    def _format_datetime(dt: Optional[datetime]) -> Optional[str]:
        """Format datetime to match Node.js format (with .000Z)"""
        if dt is None:
            return None
        # Ensure UTC timezone if naive datetime
        if dt.tzinfo is None:
            from datetime import timezone
            dt = dt.replace(tzinfo=timezone.utc)
        # Format with milliseconds and Z timezone indicator
        return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    
    @staticmethod
    def _to_dict(doc: Dict[str, Any]) -> Dict[str, Any]:
        """Convert MongoDB document to dictionary format matching MySQL model"""
        # Convert ObjectId to string
        doc_id = str(doc.get("_id", ""))
        
        # Normalize status to lowercase to match Node.js
        status_value = doc.get("status", "").lower() if doc.get("status") else "pending"
        
        return {
            "id": doc_id,
            "store_code": doc.get("store_code", ""),
            "start_date": ExcelGenerationService._format_datetime(doc.get("start_date")),
            "end_date": ExcelGenerationService._format_datetime(doc.get("end_date")),
            "status": status_value,
            "progress": doc.get("progress", 0),
            "message": doc.get("message"),
            "filename": doc.get("filename"),
            "error": doc.get("error"),
            "created_at": ExcelGenerationService._format_datetime(doc.get("created_at")),
            "updated_at": ExcelGenerationService._format_datetime(doc.get("updated_at"))
        }
    
    @staticmethod
    async def create(**kwargs) -> Dict[str, Any]:
        """Create a new Excel generation record"""
        try:
            collection = ExcelGenerationService._get_collection()
            
            # Ensure status is stored as string value
            status_value = kwargs.get('status', ExcelGenerationStatus.PENDING.value)
            if isinstance(status_value, ExcelGenerationStatus):
                status_value = status_value.value
            elif isinstance(status_value, str):
                status_value = status_value.upper()
            else:
                status_value = ExcelGenerationStatus.PENDING.value
            
            # Prepare document
            now = datetime.utcnow()
            document = {
                "store_code": kwargs.get("store_code", ""),
                "start_date": kwargs.get("start_date"),
                "end_date": kwargs.get("end_date"),
                "status": status_value,
                "progress": kwargs.get("progress", 0),
                "message": kwargs.get("message"),
                "filename": kwargs.get("filename"),
                "error": kwargs.get("error"),
                "created_at": now,
                "updated_at": now
            }
            
            # Add metadata if provided (for new generate-excel endpoint)
            if "metadata" in kwargs:
                document["metadata"] = kwargs["metadata"]
            
            # Insert document
            result = collection.insert_one(document)
            
            # Get the inserted document
            inserted_doc = collection.find_one({"_id": result.inserted_id})
            
            logger.info(f"✅ Created Excel generation record with ID: {result.inserted_id}")
            
            return ExcelGenerationService._to_dict(inserted_doc)
            
        except Exception as e:
            logger.error(f"❌ Error creating excel_generation record: {e}", exc_info=True)
            raise
    
    @staticmethod
    async def get_by_id(generation_id: str) -> Optional[Dict[str, Any]]:
        """Get Excel generation record by ID"""
        try:
            collection = ExcelGenerationService._get_collection()
            
            # Try to convert to ObjectId
            try:
                object_id = ObjectId(generation_id)
                doc = collection.find_one({"_id": object_id})
            except (InvalidId, TypeError):
                # If not a valid ObjectId, return None
                logger.warning(f"⚠️ Invalid generation_id format: {generation_id}")
                return None
            
            if doc:
                return ExcelGenerationService._to_dict(doc)
            return None
            
        except Exception as e:
            logger.error(f"❌ Error getting excel_generation by id: {e}")
            return None
    
    @staticmethod
    async def update_status(
        generation_id: str,
        status: ExcelGenerationStatus,
        progress: Optional[int] = None,
        message: Optional[str] = None,
        filename: Optional[str] = None,
        error: Optional[str] = None
    ) -> bool:
        """Update Excel generation status"""
        try:
            collection = ExcelGenerationService._get_collection()
            
            # Convert enum to string value
            status_value = status.value if isinstance(status, ExcelGenerationStatus) else status.upper()
            
            # Try to convert to ObjectId
            try:
                object_id = ObjectId(generation_id)
            except (InvalidId, TypeError):
                logger.error(f"❌ Invalid generation_id format: {generation_id}")
                return False
            
            # Build update document
            update_data = {
                "status": status_value,
                "updated_at": datetime.utcnow()
            }
            
            if progress is not None:
                update_data["progress"] = progress
            if message is not None:
                update_data["message"] = message
            if filename is not None:
                update_data["filename"] = filename
            if error is not None:
                update_data["error"] = error
            
            # Update document
            result = collection.update_one(
                {"_id": object_id},
                {"$set": update_data}
            )
            
            if result.modified_count > 0:
                logger.debug(f"✅ Updated Excel generation {generation_id} status to {status_value}")
                return True
            else:
                logger.warning(f"⚠️ No document found or updated for generation_id: {generation_id}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error updating excel_generation status: {e}", exc_info=True)
            return False
    
    @staticmethod
    async def get_all(
        limit: int = 100,
        offset: int = 0,
        status: Optional[str] = None,
        store_code_pattern: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Get all Excel generation records with optional filtering"""
        try:
            collection = ExcelGenerationService._get_collection()
            
            # Build query
            query = {}
            
            # Apply filters
            if status:
                # Handle comma-separated statuses
                status_list = [s.strip().upper() for s in status.split(',')]
                query["status"] = {"$in": status_list}
            
            if store_code_pattern:
                query["store_code"] = {"$regex": store_code_pattern, "$options": "i"}
            
            if start_date:
                if "created_at" not in query:
                    query["created_at"] = {}
                query["created_at"]["$gte"] = start_date
            
            if end_date:
                if "created_at" not in query:
                    query["created_at"] = {}
                query["created_at"]["$lte"] = end_date
            
            # Execute query with sorting, pagination
            cursor = collection.find(query).sort("created_at", -1).skip(offset).limit(limit)
            
            # Convert to list of dictionaries
            results = []
            for doc in cursor:
                results.append(ExcelGenerationService._to_dict(doc))
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Error getting all excel_generation records: {e}", exc_info=True)
            return []
    
    @staticmethod
    async def count_all(
        status: Optional[str] = None,
        store_code_pattern: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> int:
        """Count Excel generation records with optional filtering"""
        try:
            collection = ExcelGenerationService._get_collection()
            
            # Build query (same as get_all)
            query = {}
            
            if status:
                status_list = [s.strip().upper() for s in status.split(',')]
                query["status"] = {"$in": status_list}
            
            if store_code_pattern:
                query["store_code"] = {"$regex": store_code_pattern, "$options": "i"}
            
            if start_date:
                if "created_at" not in query:
                    query["created_at"] = {}
                query["created_at"]["$gte"] = start_date
            
            if end_date:
                if "created_at" not in query:
                    query["created_at"] = {}
                query["created_at"]["$lte"] = end_date
            
            count = collection.count_documents(query)
            return count
            
        except Exception as e:
            logger.error(f"❌ Error counting excel_generation records: {e}")
            return 0
    
    @staticmethod
    async def mark_stale_pending_as_failed(threshold_minutes: int = 30) -> int:
        """Mark pending jobs older than threshold as failed"""
        try:
            collection = ExcelGenerationService._get_collection()
            
            threshold_time = datetime.utcnow() - timedelta(minutes=threshold_minutes)
            
            # Find and update stale pending jobs
            result = collection.update_many(
                {
                    "status": ExcelGenerationStatus.PENDING.value,
                    "created_at": {"$lt": threshold_time}
                },
                {
                    "$set": {
                        "status": ExcelGenerationStatus.FAILED.value,
                        "message": f"Job timed out after {threshold_minutes} minutes without processing",
                        "error": "Job was never picked up by worker process and timed out",
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            if result.modified_count > 0:
                logger.info(f"✅ Marked {result.modified_count} stale pending jobs as failed")
                return result.modified_count
            return 0
            
        except Exception as e:
            logger.error(f"❌ Error marking stale pending jobs as failed: {e}", exc_info=True)
            return 0
    
    @staticmethod
    def initialize_indexes():
        """Initialize indexes - call this on application startup"""
        ExcelGenerationService._create_indexes()

