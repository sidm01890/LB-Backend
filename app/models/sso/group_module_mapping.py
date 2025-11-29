"""
GroupModuleMapping model
"""

from sqlalchemy import Column, BigInteger, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.orm import relationship
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()


class GroupModuleMapping(Base):
    __tablename__ = "group_module_mapping"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    group_id = Column(BigInteger, ForeignKey("groups.id"), nullable=False)
    module_id = Column(Integer, ForeignKey("modules.id"), nullable=False)
    permission_id = Column(Integer, ForeignKey("permissions.id"), nullable=True)
    
    # Relationships
    group = relationship("Group", back_populates="group_module_mappings")
    module = relationship("Module", back_populates="group_module_mappings")
    permission = relationship("Permission", back_populates="group_module_mappings")
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new group module mapping"""
        try:
            mapping = cls(**kwargs)
            db.add(mapping)
            await db.commit()
            await db.refresh(mapping)
            return mapping
        except Exception as e:
            await db.rollback()
            logger.error(f"Error creating group module mapping: {e}")
            raise
    
    @classmethod
    async def get_by_id(cls, db: AsyncSession, mapping_id: int):
        """Get group module mapping by ID"""
        try:
            result = await db.execute(select(cls).where(cls.id == mapping_id))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting group module mapping by ID: {e}")
            return None
    
    @classmethod
    async def get_by_group(cls, db: AsyncSession, group_id: int):
        """Get group module mappings by group ID"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.group_id == group_id)
                .order_by(cls.module_id)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting group module mappings by group: {e}")
            return []
    
    @classmethod
    async def get_by_module(cls, db: AsyncSession, module_id: int):
        """Get group module mappings by module ID"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.module_id == module_id)
                .order_by(cls.group_id)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting group module mappings by module: {e}")
            return []
    
    @classmethod
    async def get_by_group_and_module(cls, db: AsyncSession, group_id: int, module_id: int):
        """Get group module mapping by group and module ID"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.group_id == group_id)
                .where(cls.module_id == module_id)
            )
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting group module mapping by group and module: {e}")
            return None
    
    @classmethod
    async def get_all(cls, db: AsyncSession, skip: int = 0, limit: int = 100):
        """Get all group module mappings with pagination"""
        try:
            result = await db.execute(
                select(cls)
                .offset(skip)
                .limit(limit)
                .order_by(cls.group_id, cls.module_id)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all group module mappings: {e}")
            return []
    
    @classmethod
    async def update(cls, db: AsyncSession, mapping_id: int, **kwargs):
        """Update group module mapping"""
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
            logger.error(f"Error updating group module mapping: {e}")
            raise
    
    @classmethod
    async def delete(cls, db: AsyncSession, mapping_id: int):
        """Delete group module mapping"""
        try:
            result = await db.execute(
                delete(cls).where(cls.id == mapping_id)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error deleting group module mapping: {e}")
            raise
    
    @classmethod
    async def delete_by_group(cls, db: AsyncSession, group_id: int):
        """Delete all group module mappings for a group"""
        try:
            result = await db.execute(
                delete(cls).where(cls.group_id == group_id)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error deleting group module mappings by group: {e}")
            raise
    
    def to_dict(self):
        """Convert group module mapping to dictionary"""
        return {
            "id": self.id,
            "group_id": self.group_id,
            "module_id": self.module_id,
            "permission_id": self.permission_id
        }
