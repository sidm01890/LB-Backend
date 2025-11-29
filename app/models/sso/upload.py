"""
Upload model
"""

from sqlalchemy import Column, Integer, String, Text, BigInteger, DateTime, Index
from sqlalchemy.dialects.mysql import ENUM
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()


class Upload(Base):
    __tablename__ = "upload_logs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(255), nullable=False)
    filepath = Column(Text, nullable=False)
    filesize = Column(BigInteger, nullable=False)
    filetype = Column(String(10), nullable=False)
    upload_type = Column(String(50), nullable=False, comment="Type of upload: orders, transactions, reconciliation, etc.")
    status = Column(ENUM("uploaded", "processing", "completed", "failed"), nullable=False, default="uploaded")
    message = Column(Text, nullable=True)
    processed_data = Column(Text, nullable=True, comment="Stores processed data and summary information")
    error_details = Column(Text, nullable=True, comment="Detailed error information if processing failed")
    created_at = Column(DateTime, nullable=True, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=True, default=datetime.utcnow)
    
    __table_args__ = (
        Index('status', 'status'),
        Index('upload_type', 'upload_type'),
        Index('created_at', 'created_at'),
    )
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new upload log entry"""
        try:
            upload = cls(**kwargs)
            db.add(upload)
            await db.commit()
            await db.refresh(upload)
            return upload
        except Exception as e:
            await db.rollback()
            logger.error(f"Error creating upload log: {e}")
            raise
    
    @classmethod
    async def get_by_id(cls, db: AsyncSession, upload_id: int):
        """Get upload by ID"""
        try:
            result = await db.execute(select(cls).where(cls.id == upload_id))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting upload by ID: {e}")
            return None
    
    @classmethod
    async def get_by_filename(cls, db: AsyncSession, filename: str):
        """Get upload by filename"""
        try:
            result = await db.execute(select(cls).where(cls.filename == filename))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting upload by filename: {e}")
            return None
    
    @classmethod
    async def get_all(cls, db: AsyncSession, skip: int = 0, limit: int = 100):
        """Get all uploads with pagination"""
        try:
            result = await db.execute(
                select(cls)
                .offset(skip)
                .limit(limit)
                .order_by(cls.created_at.desc())
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all uploads: {e}")
            return []
    
    @classmethod
    async def get_by_status(cls, db: AsyncSession, status: str, skip: int = 0, limit: int = 100):
        """Get uploads by status"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.status == status)
                .offset(skip)
                .limit(limit)
                .order_by(cls.created_at.desc())
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting uploads by status: {e}")
            return []
    
    @classmethod
    async def get_by_upload_type(cls, db: AsyncSession, upload_type: str, skip: int = 0, limit: int = 100):
        """Get uploads by upload type"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.upload_type == upload_type)
                .offset(skip)
                .limit(limit)
                .order_by(cls.created_at.desc())
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting uploads by upload type: {e}")
            return []
    
    @classmethod
    async def get_by_date_range(cls, db: AsyncSession, start_date: datetime, end_date: datetime, skip: int = 0, limit: int = 100):
        """Get uploads by date range"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.created_at >= start_date)
                .where(cls.created_at <= end_date)
                .offset(skip)
                .limit(limit)
                .order_by(cls.created_at.desc())
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting uploads by date range: {e}")
            return []
    
    @classmethod
    async def update(cls, db: AsyncSession, upload_id: int, **kwargs):
        """Update upload"""
        try:
            kwargs['updated_at'] = datetime.utcnow()
            result = await db.execute(
                update(cls)
                .where(cls.id == upload_id)
                .values(**kwargs)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error updating upload: {e}")
            raise
    
    @classmethod
    async def delete(cls, db: AsyncSession, upload_id: int):
        """Delete upload"""
        try:
            result = await db.execute(
                delete(cls).where(cls.id == upload_id)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error deleting upload: {e}")
            raise
    
    def to_dict(self):
        """Convert upload to dictionary"""
        return {
            "id": self.id,
            "filename": self.filename,
            "filepath": self.filepath,
            "filesize": self.filesize,
            "filetype": self.filetype,
            "upload_type": self.upload_type,
            "status": self.status,
            "message": self.message,
            "processed_data": self.processed_data,
            "error_details": self.error_details,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
