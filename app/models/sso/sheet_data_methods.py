"""
Additional methods for sheet data models
"""

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from typing import List
import logging

logger = logging.getLogger(__name__)

def add_sheet_data_methods():
    """Add missing methods to all sheet data models"""
    
    # Add methods to ZomatoPosVs3poData
    async def get_by_store_codes_zomato_pos_vs_3po(cls, db: AsyncSession, store_codes: List[str], limit: int = 100):
        """Get records by store codes"""
        try:
            query = select(cls).where(cls.store_name.in_(store_codes)).limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting records by store codes: {e}")
            return []
    
    async def truncate_table_zomato_pos_vs_3po(cls, db: AsyncSession):
        """Truncate table"""
        try:
            await db.execute(text(f"TRUNCATE TABLE {cls.__tablename__}"))
            await db.commit()
        except Exception as e:
            logger.error(f"Error truncating table: {e}")
            await db.rollback()
            raise
    
    # Add methods to Zomato3poVsPosData
    async def get_by_store_codes_zomato_3po_vs_pos(cls, db: AsyncSession, store_codes: List[str], limit: int = 100):
        """Get records by store codes"""
        try:
            query = select(cls).where(cls.store_name.in_(store_codes)).limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting records by store codes: {e}")
            return []
    
    async def truncate_table_zomato_3po_vs_pos(cls, db: AsyncSession):
        """Truncate table"""
        try:
            await db.execute(text(f"TRUNCATE TABLE {cls.__tablename__}"))
            await db.commit()
        except Exception as e:
            logger.error(f"Error truncating table: {e}")
            await db.rollback()
            raise
    
    # Add methods to Zomato3poVsPosRefundData
    async def get_by_store_codes_zomato_3po_vs_pos_refund(cls, db: AsyncSession, store_codes: List[str], limit: int = 100):
        """Get records by store codes"""
        try:
            query = select(cls).where(cls.store_name.in_(store_codes)).limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting records by store codes: {e}")
            return []
    
    async def truncate_table_zomato_3po_vs_pos_refund(cls, db: AsyncSession):
        """Truncate table"""
        try:
            await db.execute(text(f"TRUNCATE TABLE {cls.__tablename__}"))
            await db.commit()
        except Exception as e:
            logger.error(f"Error truncating table: {e}")
            await db.rollback()
            raise
    
    # Add methods to OrdersNotInPosData
    async def get_by_store_codes_orders_not_in_pos(cls, db: AsyncSession, store_codes: List[str], limit: int = 100):
        """Get records by store codes"""
        try:
            query = select(cls).where(cls.store_name.in_(store_codes)).limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting records by store codes: {e}")
            return []
    
    async def truncate_table_orders_not_in_pos(cls, db: AsyncSession):
        """Truncate table"""
        try:
            await db.execute(text(f"TRUNCATE TABLE {cls.__tablename__}"))
            await db.commit()
        except Exception as e:
            logger.error(f"Error truncating table: {e}")
            await db.rollback()
            raise
    
    # Add methods to OrdersNotIn3poData
    async def get_by_store_codes_orders_not_in_3po(cls, db: AsyncSession, store_codes: List[str], limit: int = 100):
        """Get records by store codes"""
        try:
            query = select(cls).where(cls.store_name.in_(store_codes)).limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting records by store codes: {e}")
            return []
    
    async def truncate_table_orders_not_in_3po(cls, db: AsyncSession):
        """Truncate table"""
        try:
            await db.execute(text(f"TRUNCATE TABLE {cls.__tablename__}"))
            await db.commit()
        except Exception as e:
            logger.error(f"Error truncating table: {e}")
            await db.rollback()
            raise
