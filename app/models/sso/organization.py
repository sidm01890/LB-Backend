"""
Organization model
"""

from sqlalchemy import Column, BigInteger, String, Text, Boolean, DateTime, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.orm import relationship
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()


class Organization(Base):
    __tablename__ = "organization"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    organization_unit_name = Column(String(255), unique=True, nullable=True)
    organization_full_name = Column(String(255), nullable=True)
    domain_name = Column(String(255), nullable=False)
    address = Column(Text, nullable=False)
    logo_url = Column(String(255), nullable=True)
    status = Column(Boolean, nullable=False, default=True, comment="0: inactive, 1: active")
    created_by = Column(String(255), nullable=False)
    updated_by = Column(String(255), nullable=False)
    created_date = Column(DateTime, nullable=True, default=datetime.utcnow)
    updated_date = Column(DateTime, nullable=True, default=datetime.utcnow)
    
    # Relationships
    organization_tools = relationship("OrganizationTool", back_populates="organization")
    users = relationship("UserDetails", back_populates="organization")
    
    __table_args__ = (
        Index('organization_unit_name', 'organization_unit_name', unique=True),
    )
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new organization"""
        try:
            organization = cls(**kwargs)
            db.add(organization)
            await db.commit()
            await db.refresh(organization)
            return organization
        except Exception as e:
            await db.rollback()
            logger.error(f"Error creating organization: {e}")
            raise
    
    @classmethod
    async def get_by_id(cls, db: AsyncSession, organization_id: int):
        """Get organization by ID"""
        try:
            result = await db.execute(select(cls).where(cls.id == organization_id))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting organization by ID: {e}")
            return None
    
    @classmethod
    async def get_by_unit_name(cls, db: AsyncSession, unit_name: str):
        """Get organization by unit name"""
        try:
            result = await db.execute(select(cls).where(cls.organization_unit_name == unit_name))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting organization by unit name: {e}")
            return None
    
    @classmethod
    async def get_all(cls, db: AsyncSession, skip: int = 0, limit: int = 100):
        """Get all organizations with pagination"""
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
            logger.error(f"Error getting all organizations: {e}")
            return []
    
    @classmethod
    async def update(cls, db: AsyncSession, organization_id: int, **kwargs):
        """Update organization"""
        try:
            kwargs['updated_date'] = datetime.utcnow()
            result = await db.execute(
                update(cls)
                .where(cls.id == organization_id)
                .values(**kwargs)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error updating organization: {e}")
            raise
    
    @classmethod
    async def delete(cls, db: AsyncSession, organization_id: int):
        """Soft delete organization (set status to False)"""
        try:
            result = await db.execute(
                update(cls)
                .where(cls.id == organization_id)
                .values(status=False, updated_date=datetime.utcnow())
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error deleting organization: {e}")
            raise
    
    @classmethod
    async def hard_delete(cls, db: AsyncSession, organization_id: int):
        """Hard delete organization"""
        try:
            result = await db.execute(
                delete(cls).where(cls.id == organization_id)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error hard deleting organization: {e}")
            raise
    
    def to_dict(self):
        """Convert organization to dictionary"""
        return {
            "id": self.id,
            "organization_unit_name": self.organization_unit_name,
            "organization_full_name": self.organization_full_name,
            "domain_name": self.domain_name,
            "address": self.address,
            "logo_url": self.logo_url,
            "status": self.status,
            "created_by": self.created_by,
            "updated_by": self.updated_by,
            "created_date": self.created_date.isoformat() if self.created_date else None,
            "updated_date": self.updated_date.isoformat() if self.updated_date else None
        }