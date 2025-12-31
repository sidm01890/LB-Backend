"""
Excel Generation model for tracking Excel generation jobs
Now uses MongoDB instead of MySQL
"""

from datetime import datetime
import enum
import logging
from typing import Optional, Dict, Any, List
from app.services.excel_generation_service import (
    ExcelGenerationService,
    ExcelGenerationStatus as ServiceExcelGenerationStatus
)

logger = logging.getLogger(__name__)


class ExcelGenerationStatus(str, enum.Enum):
    """Excel generation status enum"""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class ExcelGeneration:
    """
    Excel Generation model - MongoDB-based
    Maintains same interface as SQLAlchemy model for backward compatibility
    """
    
    def __init__(self, **kwargs):
        """Initialize ExcelGeneration instance from dictionary"""
        self.id = kwargs.get("id")
        self.store_code = kwargs.get("store_code")
        self.start_date = kwargs.get("start_date")
        self.end_date = kwargs.get("end_date")
        self.status = kwargs.get("status")
        self.progress = kwargs.get("progress", 0)
        self.message = kwargs.get("message")
        self.filename = kwargs.get("filename")
        self.error = kwargs.get("error")
        self.created_at = kwargs.get("created_at")
        self.updated_at = kwargs.get("updated_at")
    
    @classmethod
    async def create(cls, db=None, **kwargs):
        """
        Create a new Excel generation record
        Note: db parameter is kept for backward compatibility but not used (MongoDB doesn't need it)
        """
        try:
            # Convert status enum if needed
            if 'status' in kwargs:
                if isinstance(kwargs['status'], ExcelGenerationStatus):
                    kwargs['status'] = ServiceExcelGenerationStatus(kwargs['status'].value)
                elif isinstance(kwargs['status'], str):
                    kwargs['status'] = ServiceExcelGenerationStatus(kwargs['status'].upper())
            else:
                kwargs['status'] = ServiceExcelGenerationStatus.PENDING
            
            # Create record in MongoDB
            result_dict = await ExcelGenerationService.create(**kwargs)
            
            # Return instance matching old interface
            return cls(**result_dict)
        except Exception as e:
            logger.error(f"Error creating excel_generation record: {e}")
            raise
    
    @classmethod
    async def get_by_id(cls, db=None, generation_id=None):
        """
        Get Excel generation record by ID
        Note: db parameter is kept for backward compatibility but not used
        """
        try:
            # Convert int to string if needed (for backward compatibility)
            if isinstance(generation_id, int):
                # MongoDB uses ObjectId strings, but we'll handle this in the service
                generation_id = str(generation_id)
            
            result_dict = await ExcelGenerationService.get_by_id(generation_id)
            
            if result_dict:
                return cls(**result_dict)
            return None
        except Exception as e:
            logger.error(f"Error getting excel_generation by id: {e}")
            return None
    
    @classmethod
    async def update_status(cls, db=None, generation_id=None, status: ExcelGenerationStatus = None, 
                           progress: int = None, message: str = None, filename: str = None, error: str = None):
        """
        Update Excel generation status
        Note: db parameter is kept for backward compatibility but not used
        """
        try:
            # Convert generation_id to string if needed
            if isinstance(generation_id, int):
                generation_id = str(generation_id)
            
            # Convert status enum to service enum
            if isinstance(status, ExcelGenerationStatus):
                service_status = ServiceExcelGenerationStatus(status.value)
            else:
                service_status = ServiceExcelGenerationStatus(status.upper() if status else "PENDING")
            
            return await ExcelGenerationService.update_status(
                generation_id=generation_id,
                status=service_status,
                progress=progress,
                message=message,
                filename=filename,
                error=error
            )
        except Exception as e:
            logger.error(f"Error updating excel_generation status: {e}")
            return False
    
    @classmethod
    async def get_all(cls, db=None, limit: int = 100, offset: int = 0, 
                      status: str = None, store_code_pattern: str = None,
                      start_date: datetime = None, end_date: datetime = None):
        """
        Get all Excel generation records with optional filtering
        Note: db parameter is kept for backward compatibility but not used
        """
        try:
            result_dicts = await ExcelGenerationService.get_all(
                limit=limit,
                offset=offset,
                status=status,
                store_code_pattern=store_code_pattern,
                start_date=start_date,
                end_date=end_date
            )
            
            # Convert dictionaries to ExcelGeneration instances
            return [cls(**result_dict) for result_dict in result_dicts]
        except Exception as e:
            logger.error(f"Error getting all excel_generation records: {e}")
            return []
    
    @classmethod
    async def count_all(cls, db=None, status: str = None, 
                       store_code_pattern: str = None,
                       start_date: datetime = None, end_date: datetime = None):
        """
        Count Excel generation records with optional filtering
        Note: db parameter is kept for backward compatibility but not used
        """
        try:
            return await ExcelGenerationService.count_all(
                status=status,
                store_code_pattern=store_code_pattern,
                start_date=start_date,
                end_date=end_date
            )
        except Exception as e:
            logger.error(f"Error counting excel_generation records: {e}")
            return 0
    
    @classmethod
    async def mark_stale_pending_as_failed(cls, db=None, threshold_minutes: int = 30):
        """
        Mark pending jobs older than threshold as failed
        Note: db parameter is kept for backward compatibility but not used
        """
        try:
            return await ExcelGenerationService.mark_stale_pending_as_failed(threshold_minutes)
        except Exception as e:
            logger.error(f"Error marking stale pending jobs as failed: {e}")
            return 0
    
    def to_dict(self):
        """Convert record to dictionary"""
        def format_datetime(dt):
            """Format datetime to match Node.js format (with .000Z)"""
            if dt is None:
                return None
            # Handle string datetime (already formatted)
            if isinstance(dt, str):
                return dt
            # Ensure UTC timezone if naive datetime
            if dt.tzinfo is None:
                from datetime import timezone
                dt = dt.replace(tzinfo=timezone.utc)
            # Format with milliseconds and Z timezone indicator
            return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        
        # Normalize status to lowercase to match Node.js
        status_value = self.status
        if isinstance(status_value, ExcelGenerationStatus):
            status_value = status_value.value.lower()
        elif isinstance(status_value, str):
            status_value = status_value.lower()
        
        return {
            "id": self.id,
            "store_code": self.store_code,
            "start_date": format_datetime(self.start_date) if isinstance(self.start_date, datetime) else self.start_date,
            "end_date": format_datetime(self.end_date) if isinstance(self.end_date, datetime) else self.end_date,
            "status": status_value,
            "progress": self.progress,
            "message": self.message,
            "filename": self.filename,
            "error": self.error,
            "created_at": format_datetime(self.created_at) if isinstance(self.created_at, datetime) else self.created_at,
            "updated_at": format_datetime(self.updated_at) if isinstance(self.updated_at, datetime) else self.updated_at
        }

