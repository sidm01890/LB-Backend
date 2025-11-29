"""
UserModuleMapping model
"""

from sqlalchemy import Column, Integer, BigInteger, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.orm import relationship
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()


class UserModuleMapping(Base):
    __tablename__ = "user_module_mapping"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("user_details.id"), nullable=False)
    module_id = Column(Integer, ForeignKey("modules.id"), nullable=False)
    permission_id = Column(Integer, ForeignKey("permissions.id"), nullable=False)
    is_active = Column(Boolean, nullable=True, default=True)
    
    # Relationships
    user = relationship("UserDetails", back_populates="user_module_mappings")
    module = relationship("Module", back_populates="user_module_mappings")
    permission = relationship("Permission", back_populates="user_module_mappings")
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new user module mapping"""
        try:
            mapping = cls(**kwargs)
            db.add(mapping)
            await db.commit()
            await db.refresh(mapping)
            return mapping
        except Exception as e:
            await db.rollback()
            logger.error(f"Error creating user module mapping: {e}")
            raise
    
    @classmethod
    async def get_by_id(cls, db: AsyncSession, mapping_id: int):
        """Get user module mapping by ID"""
        try:
            result = await db.execute(select(cls).where(cls.id == mapping_id))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting user module mapping by ID: {e}")
            return None
    
    @classmethod
    async def get_by_user(cls, db: AsyncSession, user_id: int):
        """Get user module mappings by user ID"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.user_id == user_id)
                .where(cls.is_active == True)
                .order_by(cls.module_id)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting user module mappings by user: {e}")
            return []
    
    @classmethod
    async def get_by_module(cls, db: AsyncSession, module_id: int):
        """Get user module mappings by module ID"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.module_id == module_id)
                .where(cls.is_active == True)
                .order_by(cls.user_id)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting user module mappings by module: {e}")
            return []
    
    @classmethod
    async def get_by_user_and_module(cls, db: AsyncSession, user_id: int, module_id: int):
        """Get user module mapping by user and module ID"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.user_id == user_id)
                .where(cls.module_id == module_id)
                .where(cls.is_active == True)
            )
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting user module mapping by user and module: {e}")
            return None
    
    @classmethod
    async def get_by_user_and_permission(cls, db: AsyncSession, user_id: int, permission_id: int):
        """Get user module mappings by user and permission ID"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.user_id == user_id)
                .where(cls.permission_id == permission_id)
                .where(cls.is_active == True)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting user module mappings by user and permission: {e}")
            return []
    
    @classmethod
    async def get_all(cls, db: AsyncSession, skip: int = 0, limit: int = 100):
        """Get all user module mappings with pagination"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.is_active == True)
                .offset(skip)
                .limit(limit)
                .order_by(cls.user_id, cls.module_id)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all user module mappings: {e}")
            return []
    
    @classmethod
    async def update(cls, db: AsyncSession, mapping_id: int, **kwargs):
        """Update user module mapping"""
        try:
            result = await db.execute(
                update(cls)
                .where(cls.id == mapping_id)
                .values(**kwargs)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error updating user module mapping: {e}")
            raise
    
    @classmethod
    async def delete(cls, db: AsyncSession, mapping_id: int):
        """Soft delete user module mapping (set is_active to False)"""
        try:
            result = await db.execute(
                update(cls)
                .where(cls.id == mapping_id)
                .values(is_active=False)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error deleting user module mapping: {e}")
            raise
    
    @classmethod
    async def hard_delete(cls, db: AsyncSession, mapping_id: int):
        """Hard delete user module mapping"""
        try:
            result = await db.execute(
                delete(cls).where(cls.id == mapping_id)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error hard deleting user module mapping: {e}")
            raise
    
    @classmethod
    async def delete_by_user(cls, db: AsyncSession, user_id: int):
        """Delete all user module mappings for a user"""
        try:
            result = await db.execute(
                update(cls)
                .where(cls.user_id == user_id)
                .values(is_active=False)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error deleting user module mappings by user: {e}")
            raise
    
    def to_dict(self):
        """Convert user module mapping to dictionary"""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "module_id": self.module_id,
            "permission_id": self.permission_id,
            "is_active": self.is_active
        }
