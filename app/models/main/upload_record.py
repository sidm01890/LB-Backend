"""
Upload Record model for main database
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, Float
from sqlalchemy.ext.asyncio import AsyncSession
from app.config.database import Base
from datetime import datetime
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


class UploadRecord(Base):
    """Upload Record model"""
    __tablename__ = "upload_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String(255), nullable=False)
    filepath = Column(String(500), nullable=False)
    filesize = Column(Integer, nullable=False)
    filetype = Column(String(10), nullable=False)
    upload_type = Column(String(50), nullable=False)
    status = Column(String(20), default="uploaded", nullable=False)
    message = Column(Text, nullable=True)
    processed_data = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new upload record"""
        try:
            upload_record = cls(**kwargs)
            db.add(upload_record)
            await db.commit()
            await db.refresh(upload_record)
            return upload_record
        except Exception as e:
            logger.error(f"Error creating upload record: {e}")
            await db.rollback()
            raise
    
    @classmethod
    async def get_by_id(cls, db: AsyncSession, upload_id: int):
        """Get upload record by ID"""
        try:
            from sqlalchemy import select
            result = await db.execute(select(cls).where(cls.id == upload_id))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting upload record by ID: {e}")
            return None
    
    @classmethod
    async def get_all_with_pagination(cls, db: AsyncSession, page: int = 1, limit: int = 10, 
                                     status: Optional[str] = None, upload_type: Optional[str] = None):
        """Get all upload records with pagination and filters"""
        try:
            from sqlalchemy import select, func
            from sqlalchemy.orm import selectinload
            
            # Build query
            query = select(cls)
            count_query = select(func.count(cls.id))
            
            # Apply filters
            if status:
                query = query.where(cls.status == status)
                count_query = count_query.where(cls.status == status)
            
            if upload_type:
                query = query.where(cls.upload_type == upload_type)
                count_query = count_query.where(cls.upload_type == upload_type)
            
            # Order by created_at desc
            query = query.order_by(cls.created_at.desc())
            
            # Apply pagination
            offset = (page - 1) * limit
            query = query.offset(offset).limit(limit)
            
            # Execute queries
            result = await db.execute(query)
            uploads = result.scalars().all()
            
            count_result = await db.execute(count_query)
            total_count = count_result.scalar()
            
            return uploads, total_count
            
        except Exception as e:
            logger.error(f"Error getting upload records: {e}")
            return [], 0
    
    @classmethod
    async def update(cls, db: AsyncSession, upload_id: int, **kwargs):
        """Update upload record"""
        try:
            from sqlalchemy import select, update
            result = await db.execute(
                update(cls)
                .where(cls.id == upload_id)
                .values(**kwargs, updated_at=datetime.utcnow())
                .returning(cls)
            )
            await db.commit()
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error updating upload record: {e}")
            await db.rollback()
            raise
    
    @classmethod
    async def delete(cls, db: AsyncSession, upload_id: int):
        """Delete upload record"""
        try:
            from sqlalchemy import delete
            await db.execute(delete(cls).where(cls.id == upload_id))
            await db.commit()
            return True
        except Exception as e:
            logger.error(f"Error deleting upload record: {e}")
            await db.rollback()
            raise
