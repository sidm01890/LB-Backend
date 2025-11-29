"""
Reconciliation models for Main database
"""

from sqlalchemy import Column, String, Date, Numeric, DateTime, Index, Text, Integer
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
    
    # Reconciliation fields
    reconciled_status = Column(String(255), nullable=True)
    reconciled_amount = Column(Numeric(15, 2), nullable=True)
    unreconciled_amount = Column(Numeric(15, 2), nullable=True)
    
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
            logger.error(f"Error creating zomato_vs_pos_summary: {e}")
            raise
    
    @classmethod
    async def get_all(cls, db: AsyncSession, limit: int = 100):
        """Get all records"""
        try:
            query = select(cls).limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all zomato_vs_pos_summary: {e}")
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
    async def get_receivable_data(cls, db: AsyncSession, limit: int = 1000):
        """Get receivable data"""
        try:
            query = select(cls).where(cls.reconciled_status == "receivable").limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting receivable data: {e}")
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


class ThreepoDashboard(Base):
    """3PO Dashboard model"""
    __tablename__ = "threepo_dashboard"
    
    id = Column(String(255), primary_key=True)
    bank = Column(String(255), nullable=True)
    booked = Column(Numeric(15, 2), nullable=True)
    business_date = Column(String(255), nullable=True)  # String, not Date in actual table
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
    async def get_by_date_range(cls, db: AsyncSession, start_date: str, end_date: str, store_codes: list = None):
        """Get records by date range and store codes"""
        try:
            # business_date is stored as a string in the database, so we compare as strings
            # Ensure dates are in YYYY-MM-DD format for string comparison
            if isinstance(start_date, str):
                start_date_str = start_date
            else:
                start_date_str = start_date.strftime('%Y-%m-%d') if hasattr(start_date, 'strftime') else str(start_date)
            
            if isinstance(end_date, str):
                end_date_str = end_date
            else:
                end_date_str = end_date.strftime('%Y-%m-%d') if hasattr(end_date, 'strftime') else str(end_date)
            
            query = select(cls).where(
                cls.business_date >= start_date_str
            ).where(
                cls.business_date <= end_date_str
            )
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
            "business_date": self.business_date,  # Already a string
            "category": self.category,
            "delta_promo": float(self.delta_promo) if self.delta_promo else None,
            "payment_type": self.payment_type,
            "pos_charges": float(self.pos_charges) if self.pos_charges else None,
            "pos_commission": float(self.pos_commission) if self.pos_commission else None,
            "pos_discounts": float(self.pos_discounts) if self.pos_discounts else None,
            "pos_freebies": float(self.pos_freebies) if self.pos_freebies else None,
            "pos_receivables": float(self.pos_receivables) if self.pos_receivables else None,
            "pos_sales": float(self.pos_sales) if self.pos_sales else None,
            "pos_vs_three_po": float(self.pos_vs_three_po) if self.pos_vs_three_po else None,
            "three_po_charges": float(self.three_po_charges) if self.three_po_charges else None,
            "three_po_commission": float(self.three_po_commission) if self.three_po_commission else None,
            "three_po_discounts": float(self.three_po_discounts) if self.three_po_discounts else None,
            "three_po_freebies": float(self.three_po_freebies) if self.three_po_freebies else None,
            "three_po_receivables": float(self.three_po_receivables) if self.three_po_receivables else None,
            "three_po_sales": float(self.three_po_sales) if self.three_po_sales else None,
            "promo": float(self.promo) if self.promo else None,
            "receivables_vs_receipts": float(self.receivables_vs_receipts) if self.receivables_vs_receipts else None,
            "reconciled": float(self.reconciled) if self.reconciled else None,
            "store_code": self.store_code,
            "tender_name": self.tender_name,
            "un_reconciled": float(self.un_reconciled) if self.un_reconciled else None
        }


class Store(Base):
    """Store model"""
    __tablename__ = "store"
    
    id = Column(String(255), primary_key=True)
    store_code = Column(String(255), nullable=True)
    store_name = Column(String(255), nullable=True)
    city = Column(String(255), nullable=True)
    zone = Column(String(255), nullable=True)
    address = Column(Text, nullable=True)
    contact_number = Column(String(255), nullable=True)
    store_type = Column(String(255), nullable=True)
    eotf_status = Column(String(255), nullable=True)
    created_date = Column(String(255), nullable=True)
    updated_date = Column(String(255), nullable=True)
    
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
            logger.error(f"Error creating store: {e}")
            raise
    
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
    
    @classmethod
    async def get_cities(cls, db: AsyncSession):
        """Get unique cities from stores"""
        try:
            from sqlalchemy import distinct
            query = select(distinct(cls.city)).where(cls.city.isnot(None))
            result = await db.execute(query)
            cities = []
            for row in result:
                cities.append({
                    "id": row.city,
                    "name": row.city,
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

    def to_dict(self):
        """Convert store to dictionary"""
        return {
            "id": self.id,
            "store_code": self.store_code,
            "store_name": self.store_name,
            "city": self.city,
            "address": self.address,
            "contact_number": self.contact_number,
            "store_type": self.store_type,
            "eotf_status": self.eotf_status,
            "created_date": self.created_date,
            "updated_date": self.updated_date
        }


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
            record = cls(**kwargs)
            db.add(record)
            await db.commit()
            await db.refresh(record)
            return record
        except Exception as e:
            await db.rollback()
            logger.error(f"Error creating TRM record: {e}")
            raise
    
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
    async def get_by_date_range(cls, db: AsyncSession, start_date: str, end_date: str):
        """Get TRM records by date range"""
        try:
            query = (
                select(cls)
                .where(cls.transaction_date >= start_date)
                .where(cls.transaction_date <= end_date)
                .order_by(cls.transaction_date.desc())
            )
            result = await db.execute(query)
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


class SummarisedTrmData(Base):
    """Summarised TRM Data model - Intermediate table for TRM reconciliation"""
    __tablename__ = "summarised_trm_data"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    trm_name = Column(String(100), nullable=True)
    trm_uid = Column(String(255), nullable=True)
    store_name = Column(String(255), nullable=True)
    acquirer = Column(String(100), nullable=True)
    payment_mode = Column(String(100), nullable=True)
    card_issuer = Column(String(100), nullable=True)
    card_type = Column(String(100), nullable=True)
    card_network = Column(String(100), nullable=True)
    card_colour = Column(String(50), nullable=True)
    transaction_id = Column(String(255), nullable=True)
    transaction_type_detail = Column(String(255), nullable=True)
    amount = Column(Numeric(15, 2), nullable=True)
    currency = Column(String(10), nullable=True)
    transaction_date = Column(String(255), nullable=True)  # Stored as string in DD/MM/YYYY format
    rrn = Column(String(255), nullable=True)
    cloud_ref_id = Column(String(255), nullable=True, index=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('ix_summarised_trm_data_cloud_ref_id', 'cloud_ref_id'),
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
            logger.error(f"Error creating summarised_trm_data: {e}")
            raise


class PosVsTrmSummary(Base):
    """POS vs TRM Summary model"""
    __tablename__ = "pos_vs_trm_summary"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    pos_transaction_id = Column(String(255), nullable=True, index=True)
    trm_transaction_id = Column(String(255), nullable=True, index=True)
    pos_date = Column(DateTime, nullable=True, index=True)
    trm_date = Column(DateTime, nullable=True)
    pos_store = Column(String(255), nullable=True, index=True)
    trm_store = Column(String(255), nullable=True)
    pos_mode_name = Column(String(255), nullable=True)
    acquirer = Column(String(100), nullable=True, index=True)
    payment_mode = Column(String(100), nullable=True, index=True)
    card_issuer = Column(String(100), nullable=True)
    card_type = Column(String(100), nullable=True)
    card_network = Column(String(100), nullable=True)
    card_colour = Column(String(50), nullable=True)
    pos_amount = Column(Numeric(15, 2), nullable=True)
    trm_amount = Column(Numeric(15, 2), nullable=True)
    reconciled_amount = Column(Numeric(15, 2), nullable=True)
    unreconciled_amount = Column(Numeric(15, 2), nullable=True)
    reconciliation_status = Column(String(50), nullable=True, index=True)
    pos_reason = Column(Text, nullable=True)
    trm_reason = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('ix_pos_vs_trm_summary_pos_transaction_id', 'pos_transaction_id'),
        Index('ix_pos_vs_trm_summary_trm_transaction_id', 'trm_transaction_id'),
        Index('ix_pos_vs_trm_summary_reconciliation_status', 'reconciliation_status'),
        Index('ix_pos_vs_trm_summary_pos_store', 'pos_store'),
        Index('ix_pos_vs_trm_summary_pos_date', 'pos_date'),
        Index('ix_pos_vs_trm_summary_payment_mode', 'payment_mode'),
        Index('ix_pos_vs_trm_summary_acquirer', 'acquirer'),
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
            logger.error(f"Error creating pos_vs_trm_summary: {e}")
            raise
    
    @classmethod
    async def get_all(cls, db: AsyncSession, limit: int = 100):
        """Get all records"""
        try:
            query = select(cls).limit(limit)
            result = await db.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all pos_vs_trm_summary: {e}")
            return []
