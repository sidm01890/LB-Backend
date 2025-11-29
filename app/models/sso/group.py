"""
Group model
"""

from sqlalchemy import Column, BigInteger, String, Integer, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.orm import relationship
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()


class Group(Base):
    __tablename__ = "groups"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    group_name = Column(String(255), nullable=False)
    tool_id = Column(Integer, ForeignKey("tools.id"), nullable=False)
    organization_id = Column(BigInteger, nullable=False)
    created_by = Column(String(255), nullable=True)
    updated_by = Column(String(255), nullable=True)
    
    # Relationships
    tool = relationship("Tool", back_populates="groups")
    group_module_mappings = relationship("GroupModuleMapping", back_populates="group")
    users = relationship("UserDetails", back_populates="group")
    
    __table_args__ = (
        Index('tool_id', 'tool_id'),
    )
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new group"""
        try:
            group = cls(**kwargs)
            db.add(group)
            await db.commit()
            await db.refresh(group)
            return group
        except Exception as e:
            await db.rollback()
            logger.error(f"Error creating group: {e}")
            raise
    
    @classmethod
    async def get_by_id(cls, db: AsyncSession, group_id: int):
        """Get group by ID"""
        try:
            result = await db.execute(select(cls).where(cls.id == group_id))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting group by ID: {e}")
            return None
    
    @classmethod
    async def get_by_name(cls, db: AsyncSession, group_name: str):
        """Get group by name"""
        try:
            result = await db.execute(select(cls).where(cls.group_name == group_name))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting group by name: {e}")
            return None
    
    @classmethod
    async def get_all(cls, db: AsyncSession, skip: int = 0, limit: int = 100):
        """Get all groups with pagination"""
        try:
            result = await db.execute(
                select(cls)
                .offset(skip)
                .limit(limit)
                .order_by(cls.group_name)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all groups: {e}")
            return []
    
    @classmethod
    async def get_by_organization(cls, db: AsyncSession, organization_id: int):
        """Get groups by organization ID"""
        try:
            result = await db.execute(
                select(cls).where(cls.organization_id == organization_id)
                .order_by(cls.group_name)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting groups by organization: {e}")
            return []
    
    @classmethod
    async def get_by_tool_id(cls, db: AsyncSession, tool_id: int):
        """Get groups by tool ID"""
        try:
            result = await db.execute(
                select(cls).where(cls.tool_id == tool_id)
                .order_by(cls.group_name)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting groups by tool ID: {e}")
            return []
    
    @classmethod
    async def update(cls, db: AsyncSession, group_id: int, **kwargs):
        """Update group"""
        try:
            result = await db.execute(
                update(cls)
                .where(cls.id == group_id)
                .values(**kwargs)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error updating group: {e}")
            raise
    
    @classmethod
    async def delete(cls, db: AsyncSession, group_id: int):
        """Delete group"""
        try:
            result = await db.execute(
                delete(cls).where(cls.id == group_id)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error deleting group: {e}")
            raise
    
    def to_dict(self):
        """Convert group to dictionary"""
        return {
            "id": self.id,
            "group_name": self.group_name,
            "tool_id": self.tool_id,
            "organization_id": self.organization_id,
            "created_by": self.created_by,
            "updated_by": self.updated_by
        }