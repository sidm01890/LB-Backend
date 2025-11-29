"""
Module model
"""

from sqlalchemy import Column, Integer, String, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.orm import relationship
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()


class Module(Base):
    __tablename__ = "modules"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    module_name = Column(String(255), nullable=False)
    tool_id = Column(Integer, ForeignKey("tools.id"), nullable=False)
    
    # Relationships
    tool = relationship("Tool", back_populates="modules")
    permissions = relationship("Permission", back_populates="module")
    organization_tools = relationship("OrganizationTool", back_populates="module")
    group_module_mappings = relationship("GroupModuleMapping", back_populates="module")
    user_module_mappings = relationship("UserModuleMapping", back_populates="module")
    
    __table_args__ = (
        Index('tool_id', 'tool_id'),
    )
    
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
            await db.rollback()
            logger.error(f"Error creating module: {e}")
            raise
    
    @classmethod
    async def get_by_id(cls, db: AsyncSession, module_id: int):
        """Get module by ID"""
        try:
            result = await db.execute(select(cls).where(cls.id == module_id))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting module by ID: {e}")
            return None
    
    @classmethod
    async def get_by_name(cls, db: AsyncSession, module_name: str):
        """Get module by name"""
        try:
            result = await db.execute(select(cls).where(cls.module_name == module_name))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting module by name: {e}")
            return None
    
    @classmethod
    async def get_all(cls, db: AsyncSession, skip: int = 0, limit: int = 100):
        """Get all modules with pagination"""
        try:
            result = await db.execute(
                select(cls)
                .offset(skip)
                .limit(limit)
                .order_by(cls.module_name)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all modules: {e}")
            return []
    
    @classmethod
    async def get_by_tool_id(cls, db: AsyncSession, tool_id: int):
        """Get modules by tool ID"""
        try:
            result = await db.execute(
                select(cls).where(cls.tool_id == tool_id)
                .order_by(cls.module_name)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting modules by tool ID: {e}")
            return []
    
    @classmethod
    async def update(cls, db: AsyncSession, module_id: int, **kwargs):
        """Update module"""
        try:
            result = await db.execute(
                update(cls)
                .where(cls.id == module_id)
                .values(**kwargs)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error updating module: {e}")
            raise
    
    @classmethod
    async def delete(cls, db: AsyncSession, module_id: int):
        """Delete module"""
        try:
            result = await db.execute(
                delete(cls).where(cls.id == module_id)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error deleting module: {e}")
            raise
    
    def to_dict(self):
        """Convert module to dictionary"""
        return {
            "id": self.id,
            "module_name": self.module_name,
            "tool_id": self.tool_id
        }