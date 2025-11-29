"""
Excel Generation model for tracking Excel generation jobs
"""

from sqlalchemy import Column, Integer, String, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from datetime import datetime
import enum
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()


class ExcelGenerationStatus(str, enum.Enum):
    """Excel generation status enum"""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class ExcelGeneration(Base):
    """Excel Generation model"""
    __tablename__ = "excel_generations"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_code = Column(String(255), nullable=False)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    status = Column(String(50), default=ExcelGenerationStatus.PENDING.value, nullable=False)
    progress = Column(Integer, default=0, nullable=False)
    message = Column(Text, nullable=True)
    filename = Column(Text, nullable=True)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=True, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=True, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new Excel generation record"""
        try:
            # Ensure status is stored as string value
            if 'status' in kwargs:
                if isinstance(kwargs['status'], ExcelGenerationStatus):
                    kwargs['status'] = kwargs['status'].value
                elif isinstance(kwargs['status'], str):
                    # Ensure uppercase
                    kwargs['status'] = kwargs['status'].upper()
            else:
                kwargs['status'] = ExcelGenerationStatus.PENDING.value
            
            record = cls(**kwargs)
            db.add(record)
            await db.commit()
            await db.refresh(record)
            return record
        except Exception as e:
            await db.rollback()
            logger.error(f"Error creating excel_generation record: {e}")
            raise
    
    @classmethod
    async def get_by_id(cls, db: AsyncSession, generation_id: int):
        """Get Excel generation record by ID"""
        try:
            query = select(cls).where(cls.id == generation_id)
            result = await db.execute(query)
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting excel_generation by id: {e}")
            return None
    
    @classmethod
    async def update_status(cls, db: AsyncSession, generation_id: int, status: ExcelGenerationStatus, 
                           progress: int = None, message: str = None, filename: str = None, error: str = None):
        """Update Excel generation status"""
        try:
            # Convert enum to string value
            status_value = status.value if isinstance(status, ExcelGenerationStatus) else status.upper()
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
            
            query = update(cls).where(cls.id == generation_id).values(**update_data)
            await db.execute(query)
            await db.commit()
            return True
        except Exception as e:
            await db.rollback()
            logger.error(f"Error updating excel_generation status: {e}")
            return False
    
    @classmethod
    async def get_all(cls, db: AsyncSession, limit: int = 100, offset: int = 0, 
                      status: str = None, store_code_pattern: str = None,
                      start_date: datetime = None, end_date: datetime = None):
        """Get all Excel generation records with optional filtering"""
        try:
            query = select(cls)
            
            # Apply filters
            if status:
                # Handle comma-separated statuses
                status_list = [s.strip().upper() for s in status.split(',')]
                from sqlalchemy import or_
                query = query.where(or_(*[cls.status == s for s in status_list]))
            
            if store_code_pattern:
                query = query.where(cls.store_code.like(f'%{store_code_pattern}%'))
            
            if start_date:
                query = query.where(cls.created_at >= start_date)
            
            if end_date:
                query = query.where(cls.created_at <= end_date)
            
            # Order and paginate
            query = query.order_by(cls.created_at.desc()).limit(limit).offset(offset)
            
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all excel_generation records: {e}")
            return []
    
    @classmethod
    async def count_all(cls, db: AsyncSession, status: str = None, 
                       store_code_pattern: str = None,
                       start_date: datetime = None, end_date: datetime = None):
        """Count Excel generation records with optional filtering"""
        try:
            from sqlalchemy import func, or_
            query = select(func.count(cls.id))
            
            # Apply same filters as get_all
            if status:
                status_list = [s.strip().upper() for s in status.split(',')]
                query = query.where(or_(*[cls.status == s for s in status_list]))
            
            if store_code_pattern:
                query = query.where(cls.store_code.like(f'%{store_code_pattern}%'))
            
            if start_date:
                query = query.where(cls.created_at >= start_date)
            
            if end_date:
                query = query.where(cls.created_at <= end_date)
            
            result = await db.execute(query)
            return result.scalar() or 0
        except Exception as e:
            logger.error(f"Error counting excel_generation records: {e}")
            return 0
    
    @classmethod
    async def mark_stale_pending_as_failed(cls, db: AsyncSession, threshold_minutes: int = 30):
        """Mark pending jobs older than threshold as failed"""
        try:
            from datetime import timedelta
            threshold_time = datetime.utcnow() - timedelta(minutes=threshold_minutes)
            
            # Find stale pending jobs
            query = select(cls).where(
                cls.status == ExcelGenerationStatus.PENDING.value,
                cls.created_at < threshold_time
            )
            result = await db.execute(query)
            stale_jobs = result.scalars().all()
            
            if stale_jobs:
                stale_ids = [job.id for job in stale_jobs]
                # Update them
                update_query = update(cls).where(
                    cls.id.in_(stale_ids)
                ).values(
                    status=ExcelGenerationStatus.FAILED.value,
                    message=f"Job timed out after {threshold_minutes} minutes without processing",
                    error="Job was never picked up by worker process and timed out",
                    updated_at=datetime.utcnow()
                )
                await db.execute(update_query)
                await db.commit()
                logger.info(f"Marked {len(stale_ids)} stale pending jobs as failed (IDs: {stale_ids})")
                return len(stale_ids)
            return 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error marking stale pending jobs as failed: {e}")
            return 0
    
    def to_dict(self):
        """Convert record to dictionary"""
        def format_datetime(dt):
            """Format datetime to match Node.js format (with .000Z)"""
            if dt is None:
                return None
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
            "start_date": format_datetime(self.start_date),
            "end_date": format_datetime(self.end_date),
            "status": status_value,
            "progress": self.progress,
            "message": self.message,
            "filename": self.filename,
            "error": self.error,
            "created_at": format_datetime(self.created_at),
            "updated_at": format_datetime(self.updated_at)
        }

