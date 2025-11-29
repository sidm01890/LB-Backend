"""
Modules model for SSO database
"""

from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, ForeignKey
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import relationship
from app.config.database import Base
from datetime import datetime
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class Modules(Base):
    """Modules model"""
    __tablename__ = "modules"
    
    id = Column(Integer, primary_key=True, index=True)
    module_name = Column(String(255), nullable=False)
    tool_id = Column(Integer, ForeignKey("tools.id"), nullable=False)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    tool = relationship("Tools", back_populates="modules")
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new module"""
        try:
            module = cls(**kwargs)
            db.add(module)
            await db.commit()
            await db.refresh(module)
            return module
        except Exception as e:
            logger.error(f"Error creating module: {e}")
            await db.rollback()
            raise
    
    @classmethod
    async def get_by_id(cls, db: AsyncSession, module_id: int):
        """Get module by ID"""
        try:
            from sqlalchemy import select
            result = await db.execute(select(cls).where(cls.id == module_id))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting module by ID: {e}")
            return None
    
    @classmethod
    async def get_all(cls, db: AsyncSession, skip: int = 0, limit: int = 100):
        """Get all modules"""
        try:
            from sqlalchemy import select
            result = await db.execute(
                select(cls).offset(skip).limit(limit)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all modules: {e}")
            return []
