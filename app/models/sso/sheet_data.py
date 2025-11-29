"""
Sheet Data models
"""

from sqlalchemy import Column, String, Date, Numeric, DateTime, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from datetime import datetime
from typing import List
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()


class ZomatoPosVs3poData(Base):
    """Zomato POS vs 3PO Data model"""
    __tablename__ = "zomato_pos_vs_3po_data"
    
    id = Column(String(255), primary_key=True)
    pos_order_id = Column(String(255), nullable=True)
    zomato_order_id = Column(String(255), nullable=True)
    order_date = Column(Date, nullable=True)
    store_name = Column(String(255), nullable=True)
    
    # Net Amount fields
    pos_net_amount = Column(Numeric(15, 2), nullable=True)
    zomato_net_amount = Column(Numeric(15, 2), nullable=True)
    pos_vs_zomato_net_amount_delta = Column(Numeric(15, 2), nullable=True)
    
    # Tax fields
    pos_tax_paid_by_customer = Column(Numeric(15, 2), nullable=True)
    zomato_tax_paid_by_customer = Column(Numeric(15, 2), nullable=True)
    pos_vs_zomato_tax_paid_by_customer_delta = Column(Numeric(15, 2), nullable=True)
    
    # Commission fields
    pos_commission_value = Column(Numeric(15, 2), nullable=True)
    zomato_commission_value = Column(Numeric(15, 2), nullable=True)
    pos_vs_zomato_commission_value_delta = Column(Numeric(15, 2), nullable=True)
    
    # PG fields
    pos_pg_applied_on = Column(Numeric(15, 2), nullable=True)
    zomato_pg_applied_on = Column(Numeric(15, 2), nullable=True)
    pos_vs_zomato_pg_applied_on_delta = Column(Numeric(15, 2), nullable=True)
    
    pos_pg_charge = Column(Numeric(15, 2), nullable=True)
    zomato_pg_charge = Column(Numeric(15, 2), nullable=True)
    pos_vs_zomato_pg_charge_delta = Column(Numeric(15, 2), nullable=True)
    
    # Tax Zomato Fee fields
    pos_taxes_zomato_fee = Column(Numeric(15, 2), nullable=True)
    zomato_taxes_zomato_fee = Column(Numeric(15, 2), nullable=True)
    pos_vs_zomato_taxes_zomato_fee_delta = Column(Numeric(15, 2), nullable=True)
    
    # TDS fields
    pos_tds_amount = Column(Numeric(15, 2), nullable=True)
    zomato_tds_amount = Column(Numeric(15, 2), nullable=True)
    pos_vs_zomato_tds_amount_delta = Column(Numeric(15, 2), nullable=True)
    
    # Final Amount fields
    pos_final_amount = Column(Numeric(15, 2), nullable=True)
    zomato_final_amount = Column(Numeric(15, 2), nullable=True)
    pos_vs_zomato_final_amount_delta = Column(Numeric(15, 2), nullable=True)
    
    # Reconciliation fields
    reconciled_status = Column(String(255), nullable=True)
    reconciled_amount = Column(Numeric(15, 2), nullable=True)
    unreconciled_amount = Column(Numeric(15, 2), nullable=True)
    pos_vs_zomato_reason = Column(String(255), nullable=True)
    order_status_pos = Column(String(255), nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True)
    
    __table_args__ = (
        Index('order_date', 'order_date'),
        Index('store_name', 'store_name'),
    )
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new record"""
        try:
            record = cls(**kwargs)
            db.add(record)
            await db.commit()
            await db.refresh(record)
            return record
        except Exception as e:
            await db.rollback()
            logger.error(f"Error creating zomato_pos_vs_3po_data: {e}")
            raise
    
    @classmethod
    async def get_by_date_range(cls, db: AsyncSession, start_date: str, end_date: str, store_codes: list = None):
        """Get records by date range and store codes"""
        try:
            query = select(cls).where(cls.order_date >= start_date).where(cls.order_date <= end_date)
            if store_codes:
                query = query.where(cls.store_name.in_(store_codes))
            query = query.order_by(cls.order_date.asc())
            
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting zomato_pos_vs_3po_data by date range: {e}")
            return []
    
    @classmethod
    async def get_all(cls, db: AsyncSession, skip: int = 0, limit: int = 100):
        """Get all records with pagination"""
        try:
            result = await db.execute(
                select(cls)
                .offset(skip)
                .limit(limit)
                .order_by(cls.order_date.desc())
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all zomato_pos_vs_3po_data: {e}")
            return []
    
    @classmethod
    async def get_by_store_codes(cls, db: AsyncSession, store_codes: List[str], limit: int = 100):
        """Get records by store codes"""
        try:
            from sqlalchemy import select
            query = select(cls).where(cls.store_name.in_(store_codes)).limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting records by store codes: {e}")
            return []
    
    @classmethod
    async def truncate_table(cls, db: AsyncSession):
        """Truncate table"""
        try:
            from sqlalchemy import text
            await db.execute(text(f"TRUNCATE TABLE {cls.__tablename__}"))
            await db.commit()
        except Exception as e:
            logger.error(f"Error truncating table: {e}")
            await db.rollback()
            raise

    def to_dict(self):
        """Convert record to dictionary"""
        return {
            "id": self.id,
            "pos_order_id": self.pos_order_id,
            "zomato_order_id": self.zomato_order_id,
            "order_date": self.order_date.isoformat() if self.order_date else None,
            "store_name": self.store_name,
            "pos_net_amount": float(self.pos_net_amount) if self.pos_net_amount else None,
            "zomato_net_amount": float(self.zomato_net_amount) if self.zomato_net_amount else None,
            "pos_vs_zomato_net_amount_delta": float(self.pos_vs_zomato_net_amount_delta) if self.pos_vs_zomato_net_amount_delta else None,
            "reconciled_status": self.reconciled_status,
            "reconciled_amount": float(self.reconciled_amount) if self.reconciled_amount else None,
            "unreconciled_amount": float(self.unreconciled_amount) if self.unreconciled_amount else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }


class Zomato3poVsPosData(Base):
    """Zomato 3PO vs POS Data model"""
    __tablename__ = "zomato_3po_vs_pos_data"
    
    id = Column(String(255), primary_key=True)
    zomato_order_id = Column(String(255), nullable=True)
    pos_order_id = Column(String(255), nullable=True)
    order_date = Column(Date, nullable=True)
    store_name = Column(String(255), nullable=True)
    
    # Net Amount fields
    zomato_net_amount = Column(Numeric(15, 2), nullable=True)
    pos_net_amount = Column(Numeric(15, 2), nullable=True)
    zomato_vs_pos_net_amount_delta = Column(Numeric(15, 2), nullable=True)
    
    # Tax fields
    zomato_tax_paid_by_customer = Column(Numeric(15, 2), nullable=True)
    pos_tax_paid_by_customer = Column(Numeric(15, 2), nullable=True)
    zomato_vs_pos_tax_paid_by_customer_delta = Column(Numeric(15, 2), nullable=True)
    
    # Commission fields
    zomato_commission_value = Column(Numeric(15, 2), nullable=True)
    pos_commission_value = Column(Numeric(15, 2), nullable=True)
    zomato_vs_pos_commission_value_delta = Column(Numeric(15, 2), nullable=True)
    
    # PG fields
    zomato_pg_applied_on = Column(Numeric(15, 2), nullable=True)
    pos_pg_applied_on = Column(Numeric(15, 2), nullable=True)
    zomato_vs_pos_pg_applied_on_delta = Column(Numeric(15, 2), nullable=True)
    
    zomato_pg_charge = Column(Numeric(15, 2), nullable=True)
    pos_pg_charge = Column(Numeric(15, 2), nullable=True)
    zomato_vs_pos_pg_charge_delta = Column(Numeric(15, 2), nullable=True)
    
    # Tax Zomato Fee fields
    zomato_taxes_zomato_fee = Column(Numeric(15, 2), nullable=True)
    pos_taxes_zomato_fee = Column(Numeric(15, 2), nullable=True)
    zomato_vs_pos_taxes_zomato_fee_delta = Column(Numeric(15, 2), nullable=True)
    
    # TDS fields
    zomato_tds_amount = Column(Numeric(15, 2), nullable=True)
    pos_tds_amount = Column(Numeric(15, 2), nullable=True)
    zomato_vs_pos_tds_amount_delta = Column(Numeric(15, 2), nullable=True)
    
    # Final Amount fields
    zomato_final_amount = Column(Numeric(15, 2), nullable=True)
    pos_final_amount = Column(Numeric(15, 2), nullable=True)
    zomato_vs_pos_final_amount_delta = Column(Numeric(15, 2), nullable=True)
    
    # Calculated Zomato fields
    calculated_zomato_net_amount = Column(Numeric(15, 2), nullable=True)
    calculated_zomato_tax_paid_by_customer = Column(Numeric(15, 2), nullable=True)
    calculated_zomato_commission_value = Column(Numeric(15, 2), nullable=True)
    calculated_zomato_pg_applied_on = Column(Numeric(15, 2), nullable=True)
    calculated_zomato_pg_charge = Column(Numeric(15, 2), nullable=True)
    calculated_zomato_taxes_zomato_fee = Column(Numeric(15, 2), nullable=True)
    calculated_zomato_tds_amount = Column(Numeric(15, 2), nullable=True)
    calculated_zomato_final_amount = Column(Numeric(15, 2), nullable=True)
    
    # Fixed fields
    fixed_credit_note_amount = Column(Numeric(15, 2), nullable=True)
    fixed_pro_discount_passthrough = Column(Numeric(15, 2), nullable=True)
    fixed_customer_discount = Column(Numeric(15, 2), nullable=True)
    fixed_rejection_penalty_charge = Column(Numeric(15, 2), nullable=True)
    fixed_user_credits_charge = Column(Numeric(15, 2), nullable=True)
    fixed_promo_recovery_adj = Column(Numeric(15, 2), nullable=True)
    fixed_icecream_handling = Column(Numeric(15, 2), nullable=True)
    fixed_icecream_deductions = Column(Numeric(15, 2), nullable=True)
    fixed_order_support_cost = Column(Numeric(15, 2), nullable=True)
    fixed_merchant_delivery_charge = Column(Numeric(15, 2), nullable=True)
    
    # Reconciliation fields
    reconciled_status = Column(String(255), nullable=True)
    reconciled_amount = Column(Numeric(15, 2), nullable=True)
    unreconciled_amount = Column(Numeric(15, 2), nullable=True)
    zomato_vs_pos_reason = Column(String(255), nullable=True)
    order_status_zomato = Column(String(255), nullable=True)
    
    # Action field (sale, addition, refund)
    action = Column(String(50), nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True)
    
    __table_args__ = (
        Index('order_date', 'order_date'),
        Index('store_name', 'store_name'),
    )
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new record"""
        try:
            record = cls(**kwargs)
            db.add(record)
            await db.commit()
            await db.refresh(record)
            return record
        except Exception as e:
            await db.rollback()
            logger.error(f"Error creating zomato_3po_vs_pos_data: {e}")
            raise
    
    @classmethod
    async def get_by_date_range(cls, db: AsyncSession, start_date: str, end_date: str, store_codes: list = None):
        """Get records by date range and store codes"""
        try:
            query = select(cls).where(cls.order_date >= start_date).where(cls.order_date <= end_date)
            if store_codes:
                query = query.where(cls.store_name.in_(store_codes))
            query = query.order_by(cls.order_date.asc())
            
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting zomato_3po_vs_pos_data by date range: {e}")
            return []
    
    def to_dict(self):
        """Convert record to dictionary"""
        return {
            "id": self.id,
            "zomato_order_id": self.zomato_order_id,
            "pos_order_id": self.pos_order_id,
            "order_date": self.order_date.isoformat() if self.order_date else None,
            "store_name": self.store_name,
            "zomato_net_amount": float(self.zomato_net_amount) if self.zomato_net_amount else None,
            "pos_net_amount": float(self.pos_net_amount) if self.pos_net_amount else None,
            "zomato_vs_pos_net_amount_delta": float(self.zomato_vs_pos_net_amount_delta) if self.zomato_vs_pos_net_amount_delta else None,
            "reconciled_status": self.reconciled_status,
            "reconciled_amount": float(self.reconciled_amount) if self.reconciled_amount else None,
            "unreconciled_amount": float(self.unreconciled_amount) if self.unreconciled_amount else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
    
    @classmethod
    async def get_by_store_codes(cls, db: AsyncSession, store_codes: List[str], limit: int = 100):
        """Get records by store codes"""
        try:
            from sqlalchemy import select
            query = select(cls).where(cls.store_name.in_(store_codes)).limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting records by store codes: {e}")
            return []
    
    @classmethod
    async def truncate_table(cls, db: AsyncSession):
        """Truncate table"""
        try:
            from sqlalchemy import text
            await db.execute(text(f"TRUNCATE TABLE {cls.__tablename__}"))
            await db.commit()
        except Exception as e:
            logger.error(f"Error truncating table: {e}")
            await db.rollback()
            raise


class Zomato3poVsPosRefundData(Base):
    """Zomato 3PO vs POS Refund Data model"""
    __tablename__ = "zomato_3po_vs_pos_refund_data"
    
    id = Column(String(255), primary_key=True)
    zomato_order_id = Column(String(255), nullable=True)
    pos_order_id = Column(String(255), nullable=True)
    order_date = Column(Date, nullable=True)
    store_name = Column(String(255), nullable=True)
    
    # Similar structure to Zomato3poVsPosData but for refunds
    zomato_net_amount = Column(Numeric(15, 2), nullable=True)
    pos_net_amount = Column(Numeric(15, 2), nullable=True)
    zomato_vs_pos_net_amount_delta = Column(Numeric(15, 2), nullable=True)
    
    # All other fields similar to Zomato3poVsPosData
    reconciled_status = Column(String(255), nullable=True)
    reconciled_amount = Column(Numeric(15, 2), nullable=True)
    unreconciled_amount = Column(Numeric(15, 2), nullable=True)
    zomato_vs_pos_reason = Column(String(255), nullable=True)
    order_status_zomato = Column(String(255), nullable=True)
    
    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True)
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new record"""
        try:
            record = cls(**kwargs)
            db.add(record)
            await db.commit()
            await db.refresh(record)
            return record
        except Exception as e:
            await db.rollback()
            logger.error(f"Error creating zomato_3po_vs_pos_refund_data: {e}")
            raise
    
    def to_dict(self):
        """Convert record to dictionary"""
        return {
            "id": self.id,
            "zomato_order_id": self.zomato_order_id,
            "pos_order_id": self.pos_order_id,
            "order_date": self.order_date.isoformat() if self.order_date else None,
            "store_name": self.store_name,
            "reconciled_status": self.reconciled_status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
    
    @classmethod
    async def get_by_store_codes(cls, db: AsyncSession, store_codes: List[str], limit: int = 100):
        """Get records by store codes"""
        try:
            from sqlalchemy import select
            query = select(cls).where(cls.store_name.in_(store_codes)).limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting records by store codes: {e}")
            return []
    
    @classmethod
    async def truncate_table(cls, db: AsyncSession):
        """Truncate table"""
        try:
            from sqlalchemy import text
            await db.execute(text(f"TRUNCATE TABLE {cls.__tablename__}"))
            await db.commit()
        except Exception as e:
            logger.error(f"Error truncating table: {e}")
            await db.rollback()
            raise


class OrdersNotInPosData(Base):
    """Orders Not in POS Data model"""
    __tablename__ = "orders_not_in_pos_data"
    
    id = Column(String(255), primary_key=True)
    zomato_order_id = Column(String(255), nullable=True)
    order_date = Column(Date, nullable=True)
    store_name = Column(String(255), nullable=True)
    
    # Zomato-only fields
    zomato_net_amount = Column(Numeric(15, 2), nullable=True)
    zomato_tax_paid_by_customer = Column(Numeric(15, 2), nullable=True)
    zomato_commission_value = Column(Numeric(15, 2), nullable=True)
    zomato_pg_applied_on = Column(Numeric(15, 2), nullable=True)
    zomato_pg_charge = Column(Numeric(15, 2), nullable=True)
    zomato_taxes_zomato_fee = Column(Numeric(15, 2), nullable=True)
    zomato_tds_amount = Column(Numeric(15, 2), nullable=True)
    zomato_final_amount = Column(Numeric(15, 2), nullable=True)
    
    # Calculated fields
    calculated_zomato_net_amount = Column(Numeric(15, 2), nullable=True)
    calculated_zomato_tax_paid_by_customer = Column(Numeric(15, 2), nullable=True)
    calculated_zomato_commission_value = Column(Numeric(15, 2), nullable=True)
    calculated_zomato_pg_applied_on = Column(Numeric(15, 2), nullable=True)
    calculated_zomato_pg_charge = Column(Numeric(15, 2), nullable=True)
    calculated_zomato_taxes_zomato_fee = Column(Numeric(15, 2), nullable=True)
    calculated_zomato_tds_amount = Column(Numeric(15, 2), nullable=True)
    calculated_zomato_final_amount = Column(Numeric(15, 2), nullable=True)
    
    # Fixed fields
    fixed_credit_note_amount = Column(Numeric(15, 2), nullable=True)
    fixed_pro_discount_passthrough = Column(Numeric(15, 2), nullable=True)
    fixed_customer_discount = Column(Numeric(15, 2), nullable=True)
    fixed_rejection_penalty_charge = Column(Numeric(15, 2), nullable=True)
    fixed_user_credits_charge = Column(Numeric(15, 2), nullable=True)
    fixed_promo_recovery_adj = Column(Numeric(15, 2), nullable=True)
    fixed_icecream_handling = Column(Numeric(15, 2), nullable=True)
    fixed_icecream_deductions = Column(Numeric(15, 2), nullable=True)
    fixed_order_support_cost = Column(Numeric(15, 2), nullable=True)
    fixed_merchant_delivery_charge = Column(Numeric(15, 2), nullable=True)
    
    # Reconciliation fields
    reconciled_status = Column(String(255), nullable=True)
    reconciled_amount = Column(Numeric(15, 2), nullable=True)
    unreconciled_amount = Column(Numeric(15, 2), nullable=True)
    zomato_vs_pos_reason = Column(String(255), nullable=True)
    order_status_zomato = Column(String(255), nullable=True)
    
    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True)
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new record"""
        try:
            record = cls(**kwargs)
            db.add(record)
            await db.commit()
            await db.refresh(record)
            return record
        except Exception as e:
            await db.rollback()
            logger.error(f"Error creating orders_not_in_pos_data: {e}")
            raise
    
    def to_dict(self):
        """Convert record to dictionary"""
        return {
            "id": self.id,
            "zomato_order_id": self.zomato_order_id,
            "order_date": self.order_date.isoformat() if self.order_date else None,
            "store_name": self.store_name,
            "zomato_net_amount": float(self.zomato_net_amount) if self.zomato_net_amount else None,
            "reconciled_status": self.reconciled_status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
    
    @classmethod
    async def get_by_store_codes(cls, db: AsyncSession, store_codes: List[str], limit: int = 100):
        """Get records by store codes"""
        try:
            from sqlalchemy import select
            query = select(cls).where(cls.store_name.in_(store_codes)).limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting records by store codes: {e}")
            return []
    
    @classmethod
    async def truncate_table(cls, db: AsyncSession):
        """Truncate table"""
        try:
            from sqlalchemy import text
            await db.execute(text(f"TRUNCATE TABLE {cls.__tablename__}"))
            await db.commit()
        except Exception as e:
            logger.error(f"Error truncating table: {e}")
            await db.rollback()
            raise


class OrdersNotIn3poData(Base):
    """Orders Not in 3PO Data model"""
    __tablename__ = "orders_not_in_3po_data"
    
    id = Column(String(255), primary_key=True)
    pos_order_id = Column(String(255), nullable=True)
    order_date = Column(Date, nullable=True)
    store_name = Column(String(255), nullable=True)
    
    # POS-only fields
    pos_net_amount = Column(Numeric(15, 2), nullable=True)
    pos_tax_paid_by_customer = Column(Numeric(15, 2), nullable=True)
    pos_commission_value = Column(Numeric(15, 2), nullable=True)
    pos_pg_applied_on = Column(Numeric(15, 2), nullable=True)
    pos_pg_charge = Column(Numeric(15, 2), nullable=True)
    pos_taxes_zomato_fee = Column(Numeric(15, 2), nullable=True)
    pos_tds_amount = Column(Numeric(15, 2), nullable=True)
    pos_final_amount = Column(Numeric(15, 2), nullable=True)
    
    # Reconciliation fields
    reconciled_status = Column(String(255), nullable=True)
    reconciled_amount = Column(Numeric(15, 2), nullable=True)
    unreconciled_amount = Column(Numeric(15, 2), nullable=True)
    pos_vs_zomato_reason = Column(String(255), nullable=True)
    order_status_pos = Column(String(255), nullable=True)
    
    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True)
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new record"""
        try:
            record = cls(**kwargs)
            db.add(record)
            await db.commit()
            await db.refresh(record)
            return record
        except Exception as e:
            await db.rollback()
            logger.error(f"Error creating orders_not_in_3po_data: {e}")
            raise
    
    @classmethod
    async def get_by_store_codes(cls, db: AsyncSession, store_codes: List[str], limit: int = 100):
        """Get records by store codes"""
        try:
            from sqlalchemy import select
            query = select(cls).where(cls.store_name.in_(store_codes)).limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting records by store codes: {e}")
            return []
    
    @classmethod
    async def truncate_table(cls, db: AsyncSession):
        """Truncate table"""
        try:
            from sqlalchemy import text
            await db.execute(text(f"TRUNCATE TABLE {cls.__tablename__}"))
            await db.commit()
        except Exception as e:
            logger.error(f"Error truncating table: {e}")
            await db.rollback()
            raise

    def to_dict(self):
        """Convert record to dictionary"""
        return {
            "id": self.id,
            "pos_order_id": self.pos_order_id,
            "order_date": self.order_date.isoformat() if self.order_date else None,
            "store_name": self.store_name,
            "pos_net_amount": float(self.pos_net_amount) if self.pos_net_amount else None,
            "reconciled_status": self.reconciled_status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
    
    @classmethod
    async def get_by_store_codes(cls, db: AsyncSession, store_codes: List[str], limit: int = 100):
        """Get records by store codes"""
        try:
            from sqlalchemy import select
            query = select(cls).where(cls.store_name.in_(store_codes)).limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting records by store codes: {e}")
            return []
    
    @classmethod
    async def truncate_table(cls, db: AsyncSession):
        """Truncate table"""
        try:
            from sqlalchemy import text
            await db.execute(text(f"TRUNCATE TABLE {cls.__tablename__}"))
            await db.commit()
        except Exception as e:
            logger.error(f"Error truncating table: {e}")
            await db.rollback()
            raise
