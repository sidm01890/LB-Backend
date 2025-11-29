"""
Tool model
"""

from sqlalchemy import Column, Integer, String, Boolean, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.orm import relationship
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()


class Tool(Base):
    __tablename__ = "tools"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    tool_name = Column(String(255), nullable=False, unique=True)
    tool_logo = Column(String(255), nullable=True)
    tool_url = Column(String(255), nullable=True)
    tool_status = Column(Boolean, nullable=True, default=True)
    
    # Relationships
    modules = relationship("Module", back_populates="tool")
    groups = relationship("Group", back_populates="tool")
    permissions = relationship("Permission", back_populates="tool")
    organization_tools = relationship("OrganizationTool", back_populates="tool")
    
    __table_args__ = (
        Index('tool_name', 'tool_name', unique=True),
    )
    
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
            await db.rollback()
            logger.error(f"Error creating tool: {e}")
            raise
    
    @classmethod
    async def get_by_id(cls, db: AsyncSession, tool_id: int):
        """Get tool by ID"""
        try:
            result = await db.execute(select(cls).where(cls.id == tool_id))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting tool by ID: {e}")
            return None
    
    @classmethod
    async def get_by_name(cls, db: AsyncSession, tool_name: str):
        """Get tool by name"""
        try:
            result = await db.execute(select(cls).where(cls.tool_name == tool_name))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting tool by name: {e}")
            return None
    
    @classmethod
    async def get_all(cls, db: AsyncSession, skip: int = 0, limit: int = 100):
        """Get all tools with pagination"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.tool_status == True)
                .offset(skip)
                .limit(limit)
                .order_by(cls.tool_name)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all tools: {e}")
            return []
    
    @classmethod
    async def update(cls, db: AsyncSession, tool_id: int, **kwargs):
        """Update tool"""
        try:
            result = await db.execute(
                update(cls)
                .where(cls.id == tool_id)
                .values(**kwargs)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error updating tool: {e}")
            raise
    
    @classmethod
    async def delete(cls, db: AsyncSession, tool_id: int):
        """Soft delete tool (set status to False)"""
        try:
            result = await db.execute(
                update(cls)
                .where(cls.id == tool_id)
                .values(tool_status=False)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error deleting tool: {e}")
            raise
    
    @classmethod
    async def hard_delete(cls, db: AsyncSession, tool_id: int):
        """Hard delete tool"""
        try:
            result = await db.execute(
                delete(cls).where(cls.id == tool_id)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error hard deleting tool: {e}")
            raise
    
    def to_dict(self):
        """Convert tool to dictionary"""
        return {
            "id": self.id,
            "tool_name": self.tool_name,
            "tool_logo": self.tool_logo,
            "tool_url": self.tool_url,
            "tool_status": self.tool_status
        }