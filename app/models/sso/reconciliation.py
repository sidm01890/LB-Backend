"""
Reconciliation models
"""

from sqlalchemy import Column, String, Date, Numeric, DateTime, Index, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from datetime import datetime
from typing import List
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()


class ZomatoVsPosSummary(Base):
    """Zomato vs POS Summary model"""
    __tablename__ = "zomato_vs_pos_summary"
    
    id = Column(String(255), primary_key=True)
    pos_order_id = Column(String(255), nullable=True)
    zomato_order_id = Column(String(255), nullable=True)
    order_date = Column(Date, nullable=True)
    store_name = Column(String(255), nullable=True)
    
    # Net Amount fields
    pos_net_amount = Column(Numeric(15, 2), nullable=True)
    zomato_net_amount = Column(Numeric(15, 2), nullable=True)
    pos_vs_zomato_net_amount_delta = Column(Numeric(15, 2), nullable=True)
    zomato_vs_pos_net_amount_delta = Column(Numeric(15, 2), nullable=True)
    
    # Tax fields
    pos_tax_paid_by_customer = Column(Numeric(15, 2), nullable=True)
    zomato_tax_paid_by_customer = Column(Numeric(15, 2), nullable=True)
    pos_vs_zomato_tax_paid_by_customer_delta = Column(Numeric(15, 2), nullable=True)
    zomato_vs_pos_tax_paid_by_customer_delta = Column(Numeric(15, 2), nullable=True)
    
    # Commission fields
    pos_commission_value = Column(Numeric(15, 2), nullable=True)
    zomato_commission_value = Column(Numeric(15, 2), nullable=True)
    pos_vs_zomato_commission_value_delta = Column(Numeric(15, 2), nullable=True)
    zomato_vs_pos_commission_value_delta = Column(Numeric(15, 2), nullable=True)
    
    # PG fields
    pos_pg_applied_on = Column(Numeric(15, 2), nullable=True)
    zomato_pg_applied_on = Column(Numeric(15, 2), nullable=True)
    pos_vs_zomato_pg_applied_on_delta = Column(Numeric(15, 2), nullable=True)
    zomato_vs_pos_pg_applied_on_delta = Column(Numeric(15, 2), nullable=True)
    
    pos_pg_charge = Column(Numeric(15, 2), nullable=True)
    zomato_pg_charge = Column(Numeric(15, 2), nullable=True)
    pos_vs_zomato_pg_charge_delta = Column(Numeric(15, 2), nullable=True)
    zomato_vs_pos_pg_charge_delta = Column(Numeric(15, 2), nullable=True)
    
    # Tax Zomato Fee fields
    pos_taxes_zomato_fee = Column(Numeric(15, 2), nullable=True)
    zomato_taxes_zomato_fee = Column(Numeric(15, 2), nullable=True)
    pos_vs_zomato_taxes_zomato_fee_delta = Column(Numeric(15, 2), nullable=True)
    zomato_vs_pos_taxes_zomato_fee_delta = Column(Numeric(15, 2), nullable=True)
    
    # TDS fields
    pos_tds_amount = Column(Numeric(15, 2), nullable=True)
    zomato_tds_amount = Column(Numeric(15, 2), nullable=True)
    pos_vs_zomato_tds_amount_delta = Column(Numeric(15, 2), nullable=True)
    zomato_vs_pos_tds_amount_delta = Column(Numeric(15, 2), nullable=True)
    
    # Final Amount fields
    pos_final_amount = Column(Numeric(15, 2), nullable=True)
    zomato_final_amount = Column(Numeric(15, 2), nullable=True)
    pos_vs_zomato_final_amount_delta = Column(Numeric(15, 2), nullable=True)
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
    reconciled_status = Column(String(50), nullable=True)
    reconciled_amount = Column(Numeric(15, 2), nullable=True)
    unreconciled_amount = Column(Numeric(15, 2), nullable=True)
    pos_vs_zomato_reason = Column(Text, nullable=True)
    zomato_vs_pos_reason = Column(Text, nullable=True)
    order_status_zomato = Column(String(255), nullable=True)
    order_status_pos = Column(String(255), nullable=True)
    
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
            logger.error(f"Error creating zomato_vs_pos_summary: {e}")
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
            logger.error(f"Error getting zomato_vs_pos_summary by date range: {e}")
            return []
    
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
            "reconciled_status": self.reconciled_status,
            "reconciled_amount": float(self.reconciled_amount) if self.reconciled_amount else None,
            "unreconciled_amount": float(self.unreconciled_amount) if self.unreconciled_amount else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
    
    @classmethod
    async def get_all(cls, db: AsyncSession, limit: int = 100):
        """Get all dashboard records"""
        try:
            query = select(cls).limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all dashboard records: {e}")
            return []
    
    @classmethod
    async def get_count(cls, db: AsyncSession):
        """Get count of records"""
        try:
            from sqlalchemy import func
            result = await db.execute(select(func.count()).select_from(cls))
            return result.scalar() or 0
        except Exception as e:
            logger.error(f"Error getting count: {e}")
            return 0
    
    @classmethod
    async def get_all(cls, db: AsyncSession, limit: int = 100):
        """Get all summary records"""
        try:
            query = select(cls).limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all summary records: {e}")
            return []


class ThreepoDashboard(Base):
    """3PO Dashboard model"""
    __tablename__ = "threepo_dashboard"
    
    id = Column(String(255), primary_key=True)
    bank = Column(String(255), nullable=True)
    booked = Column(Numeric(15, 2), nullable=True)
    business_date = Column(String(255), nullable=True)
    category = Column(String(255), nullable=True)
    delta_promo = Column(Numeric(15, 2), nullable=True)
    payment_type = Column(String(255), nullable=True)
    
    # POS fields
    pos_charges = Column(Numeric(15, 2), nullable=True)
    pos_commission = Column(Numeric(15, 2), nullable=True)
    pos_discounts = Column(Numeric(15, 2), nullable=True)
    pos_freebies = Column(Numeric(15, 2), nullable=True)
    pos_receivables = Column(Numeric(15, 2), nullable=True)
    pos_sales = Column(Numeric(15, 2), nullable=True)
    pos_vs_three_po = Column(Numeric(15, 2), nullable=True)
    
    # 3PO fields
    three_po_charges = Column(Numeric(15, 2), nullable=True)
    three_po_commission = Column(Numeric(15, 2), nullable=True)
    three_po_discounts = Column(Numeric(15, 2), nullable=True)
    three_po_freebies = Column(Numeric(15, 2), nullable=True)
    three_po_receivables = Column(Numeric(15, 2), nullable=True)
    three_po_sales = Column(Numeric(15, 2), nullable=True)
    
    # Other fields
    promo = Column(Numeric(15, 2), nullable=True)
    receivables_vs_receipts = Column(Numeric(15, 2), nullable=True)
    reconciled = Column(Numeric(15, 2), nullable=True)
    store_code = Column(String(255), nullable=True)
    tender_name = Column(String(255), nullable=True)
    un_reconciled = Column(Numeric(15, 2), nullable=True)
    
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
            logger.error(f"Error creating threepo_dashboard: {e}")
            raise
    
    @classmethod
    async def get_all(cls, db: AsyncSession, limit: int = 100):
        """Get all records"""
        try:
            query = select(cls).limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all threepo_dashboard: {e}")
            return []
    
    @classmethod
    async def get_by_date_range(cls, db: AsyncSession, start_date: str, end_date: str, store_codes: list = None):
        """Get records by date range and store codes"""
        try:
            query = select(cls).where(cls.business_date >= start_date).where(cls.business_date <= end_date)
            if store_codes:
                query = query.where(cls.store_code.in_(store_codes))
            query = query.order_by(cls.business_date.asc())
            
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting threepo_dashboard by date range: {e}")
            return []
    
    def to_dict(self):
        """Convert record to dictionary"""
        return {
            "id": self.id,
            "bank": self.bank,
            "booked": float(self.booked) if self.booked else None,
            "business_date": self.business_date,
            "category": self.category,
            "store_code": self.store_code,
            "tender_name": self.tender_name,
            "pos_sales": float(self.pos_sales) if self.pos_sales else None,
            "three_po_sales": float(self.three_po_sales) if self.three_po_sales else None,
            "reconciled": float(self.reconciled) if self.reconciled else None,
            "un_reconciled": float(self.un_reconciled) if self.un_reconciled else None
        }


class Store(Base):
    """Store model"""
    __tablename__ = "store"
    
    id = Column(String(255), primary_key=True)
    created_date = Column(String(255), nullable=True)
    updated_date = Column(String(255), nullable=True)
    created_by = Column(String(255), nullable=True, default="SYSTEM_GENERATED")
    updated_by = Column(String(255), nullable=True, default="SYSTEM_GENERATED")
    address = Column(String(255), nullable=True)
    bandwidth = Column(String(255), nullable=True)
    circuit_id = Column(String(255), nullable=True)
    city = Column(String(255), nullable=True)
    contact_number = Column(String(255), nullable=True)
    eotf_status = Column(String(255), nullable=True)
    store_code = Column(String(255), nullable=True)
    store_name = Column(String(255), nullable=True)
    store_type = Column(String(255), nullable=True)
    zone = Column(String(255), nullable=True)
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new store"""
        try:
            store = cls(**kwargs)
            db.add(store)
            await db.commit()
            await db.refresh(store)
            return store
        except Exception as e:
            await db.rollback()
            logger.error(f"Error creating store: {e}")
            raise
    
    @classmethod
    async def get_by_city(cls, db: AsyncSession, city: str):
        """Get stores by city"""
        try:
            result = await db.execute(
                select(cls).where(cls.city == city)
                .order_by(cls.store_name)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting stores by city: {e}")
            return []
    
    @classmethod
    async def get_by_zone(cls, db: AsyncSession, zone: str):
        """Get stores by zone"""
        try:
            result = await db.execute(
                select(cls).where(cls.zone == zone)
                .order_by(cls.store_name)
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting stores by zone: {e}")
            return []
    
    def to_dict(self):
        """Convert store to dictionary"""
        return {
            "id": self.id,
            "store_code": self.store_code,
            "store_name": self.store_name,
            "city": self.city,
            "zone": self.zone,
            "address": self.address,
            "contact_number": self.contact_number,
            "store_type": self.store_type,
            "eotf_status": self.eotf_status,
            "created_date": self.created_date,
            "updated_date": self.updated_date
        }
    
    @classmethod
    async def get_cities(cls, db: AsyncSession):
        """Get unique cities from stores"""
        try:
            from sqlalchemy import func, distinct
            query = select(distinct(cls.city), cls.zone).where(cls.city.isnot(None))
            result = await db.execute(query)
            cities = []
            for row in result:
                cities.append({
                    "id": row.city,
                    "name": row.city,
                    "state": row.zone,
                    "country": "India"
                })
            return cities
        except Exception as e:
            logger.error(f"Error getting cities: {e}")
            return []
    
    @classmethod
    async def get_by_city_ids(cls, db: AsyncSession, city_ids: List[str]):
        """Get stores by city IDs"""
        try:
            query = select(cls).where(cls.city.in_(city_ids))
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting stores by city IDs: {e}")
            return []
    
    @classmethod
    async def get_all(cls, db: AsyncSession, limit: int = 100):
        """Get all stores"""
        try:
            query = select(cls).limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all stores: {e}")
            return []


class Trm(Base):
    """TRM (Transaction Reconciliation Management) model"""
    __tablename__ = "trm"
    
    uid = Column(String(255), primary_key=True, unique=True)
    zone = Column(String(128), nullable=True)
    store_name = Column(String(128), nullable=True)
    city = Column(String(128), nullable=True)
    pos = Column(String(128), nullable=True)
    hardware_model = Column(String(128), nullable=True)
    hardware_id = Column(String(128), nullable=True)
    acquirer = Column(String(128), nullable=True)
    tid = Column(String(128), nullable=True)
    mid = Column(String(128), nullable=True)
    batch_no = Column(String(128), nullable=True)
    transaction_date = Column(Date, nullable=True)
    transaction_time = Column(String(128), nullable=True)
    card_number = Column(String(128), nullable=True)
    transaction_type = Column(String(128), nullable=True)
    amount = Column(Numeric(15, 2), nullable=True)
    currency = Column(String(128), nullable=True)
    auth_code = Column(String(128), nullable=True)
    rrn = Column(String(128), nullable=True)
    status = Column(String(128), nullable=True)
    
    @classmethod
    async def create(cls, db: AsyncSession, **kwargs):
        """Create a new TRM record"""
        try:
            trm = cls(**kwargs)
            db.add(trm)
            await db.commit()
            await db.refresh(trm)
            return trm
        except Exception as e:
            await db.rollback()
            logger.error(f"Error creating TRM: {e}")
            raise
    
    @classmethod
    async def get_by_date_range(cls, db: AsyncSession, start_date: str, end_date: str):
        """Get TRM records by date range"""
        try:
            result = await db.execute(
                select(cls)
                .where(cls.transaction_date >= start_date)
                .where(cls.transaction_date <= end_date)
                .order_by(cls.transaction_date.desc())
            )
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting TRM by date range: {e}")
            return []
    
    def to_dict(self):
        """Convert TRM to dictionary"""
        return {
            "uid": self.uid,
            "zone": self.zone,
            "store_name": self.store_name,
            "city": self.city,
            "pos": self.pos,
            "transaction_date": self.transaction_date.isoformat() if self.transaction_date else None,
            "transaction_time": self.transaction_time,
            "amount": float(self.amount) if self.amount else None,
            "currency": self.currency,
            "status": self.status
        }
    
    @classmethod
    async def get_all(cls, db: AsyncSession, limit: int = 100):
        """Get all TRM records"""
        try:
            query = select(cls).limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all TRM records: {e}")
            return []
    
    @classmethod
    async def get_receivable_data(cls, db: AsyncSession, limit: int = 1000):
        """Get receivable data"""
        try:
            query = select(cls).where(cls.reconciled_status == "receivable").limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting receivable data: {e}")
            return []
    
    @classmethod
    async def get_count(cls, db: AsyncSession):
        """Get count of records"""
        try:
            from sqlalchemy import func
            result = await db.execute(select(func.count()).select_from(cls))
            return result.scalar() or 0
        except Exception as e:
            logger.error(f"Error getting count: {e}")
            return 0
