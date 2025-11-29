"""
AuditLog model
"""

from sqlalchemy import Column, BigInteger, String, Text, DateTime, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()


class AuditLog(Base):
    __tablename__ = "audit_log"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    username = Column(String(45), nullable=False)
    user_email = Column(String(255), nullable=False)
    system_ip = Column(String(45), nullable=True)
    role = Column(String(100), nullable=False)
    action = Column(String(255), nullable=False)
    action_details = Column(String(300), nullable=True)
    request = Column(Text, nullable=True)
    response = Column(Text, nullable=True)
    remarks = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=True, default=datetime.utcnow)
    
    __table_args__ = (
        Index('created_at', 'created_at'),
    )
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new audit log entry"""
        try:
            audit_log = cls(**kwargs)
            db.add(audit_log)
            await db.commit()
            await db.refresh(audit_log)
            return audit_log
        except Exception as e:
            await db.rollback()
            logger.error(f"Error creating audit log: {e}")
            raise
    
    @classmethod
    async def get_by_id(cls, db: AsyncSession, audit_log_id: int):
        """Get audit log by ID"""
        try:
            result = await db.execute(select(cls).where(cls.id == audit_log_id))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting audit log by ID: {e}")
            return None
    
    @classmethod
    async def get_all(cls, db: AsyncSession, skip: int = 0, limit: int = 100):
        """Get all audit logs with pagination"""
        try:
            result = await db.execute(
                select(cls)
                .offset(skip)
                .limit(limit)
                .order_by(cls.created_at.desc())
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all audit logs: {e}")
            return []
    
    @classmethod
    async def get_by_username(cls, db: AsyncSession, username: str, skip: int = 0, limit: int = 100):
        """Get audit logs by username"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.username == username)
                .offset(skip)
                .limit(limit)
                .order_by(cls.created_at.desc())
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting audit logs by username: {e}")
            return []
    
    @classmethod
    async def get_by_user_email(cls, db: AsyncSession, user_email: str, skip: int = 0, limit: int = 100):
        """Get audit logs by user email"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.user_email == user_email)
                .offset(skip)
                .limit(limit)
                .order_by(cls.created_at.desc())
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting audit logs by user email: {e}")
            return []
    
    @classmethod
    async def get_by_action(cls, db: AsyncSession, action: str, skip: int = 0, limit: int = 100):
        """Get audit logs by action"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.action == action)
                .offset(skip)
                .limit(limit)
                .order_by(cls.created_at.desc())
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting audit logs by action: {e}")
            return []
    
    @classmethod
    async def get_by_date_range(cls, db: AsyncSession, start_date: datetime, end_date: datetime, skip: int = 0, limit: int = 100):
        """Get audit logs by date range"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.created_at >= start_date)
                .where(cls.created_at <= end_date)
                .offset(skip)
                .limit(limit)
                .order_by(cls.created_at.desc())
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting audit logs by date range: {e}")
            return []
    
    @classmethod
    async def update(cls, db: AsyncSession, audit_log_id: int, **kwargs):
        """Update audit log"""
        try:
            result = await db.execute(
                update(cls)
                .where(cls.id == audit_log_id)
                .values(**kwargs)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error updating audit log: {e}")
            raise
    
    @classmethod
    async def delete(cls, db: AsyncSession, audit_log_id: int):
        """Delete audit log"""
        try:
            result = await db.execute(
                delete(cls).where(cls.id == audit_log_id)
            )
            await db.commit()
            return result.rowcount > 0
        except Exception as e:
            await db.rollback()
            logger.error(f"Error deleting audit log: {e}")
            raise
    
    def to_dict(self):
        """Convert audit log to dictionary"""
        return {
            "id": self.id,
            "username": self.username,
            "user_email": self.user_email,
            "system_ip": self.system_ip,
            "role": self.role,
            "action": self.action,
            "action_details": self.action_details,
            "request": self.request,
            "response": self.response,
            "remarks": self.remarks,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }