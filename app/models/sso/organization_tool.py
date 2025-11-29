"""
OrganizationTool model
"""

from sqlalchemy import Column, BigInteger, Integer, Boolean, DateTime, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.orm import relationship
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()


class OrganizationTool(Base):
    __tablename__ = "organization_tool"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    organization_id = Column(BigInteger, ForeignKey("organization.id"), nullable=False)
    tool_id = Column(Integer, ForeignKey("tools.id"), nullable=False)
    module_id = Column(Integer, ForeignKey("modules.id"), nullable=False)
    status = Column(Boolean, nullable=False, default=True, comment="0: inactive, 1: active")
    created_by = Column(Integer, nullable=False)
    created_date = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_by = Column(Integer, nullable=True)
    updated_date = Column(DateTime, nullable=True)
    
    # Relationships
    organization = relationship("Organization", back_populates="organization_tools")
    tool = relationship("Tool", back_populates="organization_tools")
    module = relationship("Module", back_populates="organization_tools")
    
    __table_args__ = (
        Index('tool_id', 'tool_id'),
    )
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new organization tool mapping"""
        try:
            organization_tool = cls(**kwargs)
            db.add(organization_tool)
            await db.commit()
            await db.refresh(organization_tool)
            return organization_tool
        except Exception as e:
            await db.rollback()
            logger.error(f"Error creating organization tool: {e}")
            raise
    
    @classmethod
    async def get_by_id(cls, db: AsyncSession, org_tool_id: int):
        """Get organization tool by ID"""
        try:
            result = await db.execute(select(cls).where(cls.id == org_tool_id))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting organization tool by ID: {e}")
            return None
    
    @classmethod
    async def get_by_organization(cls, db: AsyncSession, organization_id: int):
        """Get organization tools by organization ID"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.organization_id == organization_id)
                .where(cls.status == True)
                .order_by(cls.created_date.desc())
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting organization tools by organization: {e}")
            return []
    
    @classmethod
    async def get_by_tool(cls, db: AsyncSession, tool_id: int):
        """Get organization tools by tool ID"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.tool_id == tool_id)
                .where(cls.status == True)
                .order_by(cls.created_date.desc())
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting organization tools by tool: {e}")
            return []
    
    @classmethod
    async def get_by_organization_and_tool(cls, db: AsyncSession, organization_id: int, tool_id: int):
        """Get organization tool by organization and tool ID"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.organization_id == organization_id)
                .where(cls.tool_id == tool_id)
                .where(cls.status == True)
            )
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting organization tool by organization and tool: {e}")
            return None
    
    @classmethod
    async def get_all(cls, db: AsyncSession, skip: int = 0, limit: int = 100):
        """Get all organization tools with pagination"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.status == True)
                .offset(skip)
                .limit(limit)
                .order_by(cls.created_date.desc())
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all organization tools: {e}")
            return []
    
    @classmethod
    async def update(cls, db: AsyncSession, org_tool_id: int, **kwargs):
        """Update organization tool"""
        try:
            kwargs['updated_date'] = datetime.utcnow()
            result = await db.execute(
                update(cls)
                .where(cls.id == org_tool_id)
                .values(**kwargs)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error updating organization tool: {e}")
            raise
    
    @classmethod
    async def delete(cls, db: AsyncSession, org_tool_id: int):
        """Soft delete organization tool (set status to False)"""
        try:
            result = await db.execute(
                update(cls)
                .where(cls.id == org_tool_id)
                .values(status=False, updated_date=datetime.utcnow())
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error deleting organization tool: {e}")
            raise
    
    @classmethod
    async def hard_delete(cls, db: AsyncSession, org_tool_id: int):
        """Hard delete organization tool"""
        try:
            result = await db.execute(
                delete(cls).where(cls.id == org_tool_id)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error hard deleting organization tool: {e}")
            raise
    
    def to_dict(self):
        """Convert organization tool to dictionary"""
        return {
            "id": self.id,
            "organization_id": self.organization_id,
            "tool_id": self.tool_id,
            "module_id": self.module_id,
            "status": self.status,
            "created_by": self.created_by,
            "created_date": self.created_date.isoformat() if self.created_date else None,
            "updated_by": self.updated_by,
            "updated_date": self.updated_date.isoformat() if self.updated_date else None
        }
