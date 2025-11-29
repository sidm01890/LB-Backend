"""
User Details model for SSO database
"""

from sqlalchemy import Column, BigInteger, String, Boolean, DateTime, Text, Integer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import relationship
from app.config.database import Base
from datetime import datetime
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


class UserDetails(Base):
    """User Details model"""
    __tablename__ = "user_details"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True, index=True)
    username = Column(String(255), unique=True, index=True, nullable=False)
    password = Column(String(255), nullable=False)
    raw_password = Column(String(255), nullable=True)
    name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False)
    mobile = Column(String(255), nullable=True)
    active = Column(Boolean, nullable=False, default=True)
    level = Column(String(255), nullable=False)
    role_name = Column(Integer, nullable=False)
    user_label = Column(String(255), nullable=True)
    parent_username = Column(String(255), nullable=True)
    organization_id = Column(BigInteger, nullable=True)
    group_id = Column(BigInteger, nullable=True)
    created_by = Column(String(255), nullable=False)
    updated_by = Column(String(255), nullable=False)
    created_date = Column(DateTime, nullable=True, default=datetime.utcnow)
    updated_date = Column(DateTime, nullable=True, default=datetime.utcnow, onupdate=datetime.utcnow)
    access_token = Column(String(2555), nullable=True)
    refresh_token = Column(String(2555), nullable=True)
    otp_attempts = Column(Integer, nullable=True)
    otp_resend_count = Column(Integer, nullable=True)
    reset_otp = Column(String(6), nullable=True)
    reset_otp_expires = Column(DateTime, nullable=True)
    
    # Relationships
    # Note: Relationships will be added after all models are loaded to avoid circular imports
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new user"""
        try:
            user = cls(**kwargs)
            db.add(user)
            await db.commit()
            await db.refresh(user)
            return user
        except Exception as e:
            logger.error(f"Error creating user: {e}")
            await db.rollback()
            raise
    
    @classmethod
    async def get_by_id(cls, db: AsyncSession, user_id: int):
        """Get user by ID"""
        try:
            from sqlalchemy import select
            result = await db.execute(select(cls).where(cls.id == user_id))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting user by ID: {e}")
            return None
    
    @classmethod
    async def get_by_username(cls, db: AsyncSession, username: str):
        """Get user by username"""
        try:
            from sqlalchemy import select
            result = await db.execute(select(cls).where(cls.username == username))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting user by username: {e}")
            return None
    
    @classmethod
    async def get_by_email(cls, db: AsyncSession, email: str):
        """Get user by email"""
        try:
            from sqlalchemy import select
            result = await db.execute(select(cls).where(cls.email == email))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting user by email: {e}")
            return None
    
    @classmethod
    async def get_all(cls, db: AsyncSession, skip: int = 0, limit: int = 100):
        """Get all users with pagination"""
        try:
            from sqlalchemy import select
            result = await db.execute(
                select(cls).offset(skip).limit(limit)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all users: {e}")
            return []
    
    @classmethod
    async def get_all_by_organization(cls, db: AsyncSession, organization_id: int):
        """Get all users by organization ID"""
        try:
            from sqlalchemy import select
            result = await db.execute(
                select(cls).where(cls.organization_id == organization_id)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting users by organization: {e}")
            return []
    
    @classmethod
    async def update(cls, db: AsyncSession, user_id: int, **kwargs):
        """Update user"""
        try:
            from sqlalchemy import select, update
            result = await db.execute(
                update(cls)
                .where(cls.id == user_id)
                .values(**kwargs, updated_at=datetime.utcnow())
                .returning(cls)
            )
            await db.commit()
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error updating user: {e}")
            await db.rollback()
            raise
    
    @classmethod
    async def delete(cls, db: AsyncSession, user_id: int):
        """Delete user"""
        try:
            from sqlalchemy import delete
            await db.execute(delete(cls).where(cls.id == user_id))
            await db.commit()
            return True
        except Exception as e:
            logger.error(f"Error deleting user: {e}")
            await db.rollback()
            raise
