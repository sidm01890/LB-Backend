"""
Permission model
"""

from sqlalchemy import Column, Integer, String, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.orm import relationship
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()


class Permission(Base):
    __tablename__ = "permissions"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    permission_name = Column(String(255), nullable=False)
    permission_code = Column(String(255), nullable=False, unique=True)
    module_id = Column(Integer, ForeignKey("modules.id"), nullable=False)
    tool_id = Column(Integer, ForeignKey("tools.id"), nullable=False)
    
    # Relationships
    module = relationship("Module", back_populates="permissions")
    tool = relationship("Tool", back_populates="permissions")
    group_module_mappings = relationship("GroupModuleMapping", back_populates="permission")
    user_module_mappings = relationship("UserModuleMapping", back_populates="permission")
    
    __table_args__ = (
        Index('permission_code', 'permission_code', unique=True),
        Index('module_id', 'module_id'),
        Index('tool_id', 'tool_id'),
    )
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new permission"""
        try:
            permission = cls(**kwargs)
            db.add(permission)
            await db.commit()
            await db.refresh(permission)
            return permission
        except Exception as e:
            await db.rollback()
            logger.error(f"Error creating permission: {e}")
            raise
    
    @classmethod
    async def get_by_id(cls, db: AsyncSession, permission_id: int):
        """Get permission by ID"""
        try:
            result = await db.execute(select(cls).where(cls.id == permission_id))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting permission by ID: {e}")
            return None
    
    @classmethod
    async def get_by_code(cls, db: AsyncSession, permission_code: str):
        """Get permission by code"""
        try:
            result = await db.execute(select(cls).where(cls.permission_code == permission_code))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting permission by code: {e}")
            return None
    
    @classmethod
    async def get_by_name(cls, db: AsyncSession, permission_name: str):
        """Get permission by name"""
        try:
            result = await db.execute(select(cls).where(cls.permission_name == permission_name))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting permission by name: {e}")
            return None
    
    @classmethod
    async def get_all(cls, db: AsyncSession, skip: int = 0, limit: int = 100):
        """Get all permissions with pagination"""
        try:
            result = await db.execute(
                select(cls)
                .offset(skip)
                .limit(limit)
                .order_by(cls.permission_name)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all permissions: {e}")
            return []
    
    @classmethod
    async def get_by_module_id(cls, db: AsyncSession, module_id: int):
        """Get permissions by module ID"""
        try:
            result = await db.execute(
                select(cls).where(cls.module_id == module_id)
                .order_by(cls.permission_name)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting permissions by module ID: {e}")
            return []
    
    @classmethod
    async def get_by_tool_id(cls, db: AsyncSession, tool_id: int):
        """Get permissions by tool ID"""
        try:
            result = await db.execute(
                select(cls).where(cls.tool_id == tool_id)
                .order_by(cls.permission_name)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting permissions by tool ID: {e}")
            return []
    
    @classmethod
    async def update(cls, db: AsyncSession, permission_id: int, **kwargs):
        """Update permission"""
        try:
            result = await db.execute(
                update(cls)
                .where(cls.id == permission_id)
                .values(**kwargs)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error updating permission: {e}")
            raise
    
    @classmethod
    async def delete(cls, db: AsyncSession, permission_id: int):
        """Delete permission"""
        try:
            result = await db.execute(
                delete(cls).where(cls.id == permission_id)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error deleting permission: {e}")
            raise
    
    def to_dict(self):
        """Convert permission to dictionary"""
        return {
            "id": self.id,
            "permission_name": self.permission_name,
            "permission_code": self.permission_code,
            "module_id": self.module_id,
            "tool_id": self.tool_id
        }