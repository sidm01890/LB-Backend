"""
Tools model for SSO database
"""

from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text
from sqlalchemy.ext.asyncio import AsyncSession
from app.config.database import Base
from datetime import datetime
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class Tools(Base):
    """Tools model"""
    __tablename__ = "tools"
    
    id = Column(Integer, primary_key=True, index=True)
    tool_name = Column(String(255), nullable=False)
    tool_logo = Column(String(500), nullable=True)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new tool"""
        try:
            tool = cls(**kwargs)
            db.add(tool)
            await db.commit()
            await db.refresh(tool)
            return tool
        except Exception as e:
            logger.error(f"Error creating tool: {e}")
            await db.rollback()
            raise
    
    @classmethod
    async def get_by_id(cls, db: AsyncSession, tool_id: int):
        """Get tool by ID"""
        try:
            from sqlalchemy import select
            result = await db.execute(select(cls).where(cls.id == tool_id))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting tool by ID: {e}")
            return None
    
    @classmethod
    async def get_all(cls, db: AsyncSession, skip: int = 0, limit: int = 100):
        """Get all tools"""
        try:
            from sqlalchemy import select
            result = await db.execute(
                select(cls).offset(skip).limit(limit)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all tools: {e}")
            return []
