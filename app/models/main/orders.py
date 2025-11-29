"""
Orders model for Main database
"""

from sqlalchemy import Column, Integer, String, DateTime, Float, Text, Boolean
from sqlalchemy.ext.asyncio import AsyncSession
from app.config.database import Base
from datetime import datetime
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class Orders(Base):
    """Orders model"""
    __tablename__ = "orders"
    
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String(255), unique=True, index=True, nullable=False)
    customer_name = Column(String(255), nullable=True)
    order_amount = Column(Float, nullable=True)
    order_date = Column(DateTime, nullable=True)
    status = Column(String(100), nullable=True)
    payment_method = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new order"""
        try:
            order = cls(**kwargs)
            db.add(order)
            await db.commit()
            await db.refresh(order)
            return order
        except Exception as e:
            logger.error(f"Error creating order: {e}")
            await db.rollback()
            raise
    
    @classmethod
    async def get_by_id(cls, db: AsyncSession, order_id: int):
        """Get order by ID"""
        try:
            from sqlalchemy import select
            result = await db.execute(select(cls).where(cls.id == order_id))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting order by ID: {e}")
            return None
    
    @classmethod
    async def get_all(cls, db: AsyncSession, skip: int = 0, limit: int = 100):
        """Get all orders"""
        try:
            from sqlalchemy import select
            result = await db.execute(
                select(cls).offset(skip).limit(limit)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all orders: {e}")
            return []
