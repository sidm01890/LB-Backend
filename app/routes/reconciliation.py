"""
Reconciliation routes
"""

from fastapi import APIRouter, Depends, HTTPException, status, Request, BackgroundTasks, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text
from app.config.database import get_sso_db, get_main_db
from app.middleware.auth import get_current_user
from app.models.sso.user_details import UserDetails
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
import json
import logging
import os
from itertools import combinations

router = APIRouter()
logger = logging.getLogger(__name__)


class GenerateExcelRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)  # Allow both camelCase and snake_case
    
    start_date: str = Field(..., alias="startDate")
    end_date: str = Field(..., alias="endDate")
    stores: Optional[List[str]] = None  # Optional stores filter
    organization_id: Optional[int] = None


class GenerationStatusRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)  # Allow aliases
    
    # Support multiple field names for backward compatibility
    generation_id: Optional[int] = Field(None, alias="generationId")
    task_id: Optional[str] = Field(None, alias="taskId")
    job_id: Optional[str] = Field(None, alias="jobId")
    
    # Filtering options
    status: Optional[str] = None  # Can be single status or comma-separated
    store_code_pattern: Optional[str] = None
    start_date: Optional[str] = None  # Filter by created_at
    end_date: Optional[str] = None
    
    # Pagination
    limit: Optional[int] = Field(default=100, ge=1, le=1000)
    offset: Optional[int] = Field(default=0, ge=0)
    
    # Stale job handling
    exclude_stale_pending: bool = Field(default=True)
    stale_threshold_minutes: int = Field(default=30, ge=0)
    
    def get_generation_id(self) -> Optional[int]:
        """Extract generation_id from any of the supported fields"""
        # Try generation_id first
        if self.generation_id:
            return self.generation_id
        # Try task_id (if it's numeric)
        if self.task_id:
            try:
                return int(self.task_id)
            except (ValueError, TypeError):
                pass
        # Try job_id (if it's numeric)
        if self.job_id:
            try:
                return int(self.job_id)
            except (ValueError, TypeError):
                pass
        return None


class ThreePODashboardDataRequest(BaseModel):
    startDate: str
    endDate: str
    stores: List[str]


class InstoreDataRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)  # Allow both camelCase and snake_case
    
    start_date: str = Field(..., alias="startDate")
    end_date: str = Field(..., alias="endDate")
    stores: List[str]
    organization_id: Optional[int] = None


class GenerateCommonTrmRequest(BaseModel):
    start_date: Optional[str] = None  # Not currently used, kept for compatibility
    end_date: Optional[str] = None  # Not currently used, kept for compatibility
    organization_id: Optional[int] = None


class StoresRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)  # Allow both camelCase and snake_case
    
    startDate: Optional[str] = Field(None, alias="startDate")
    endDate: Optional[str] = Field(None, alias="endDate")
    cities: List[Union[str, Dict[str, Any]]] = Field(..., description="List of city IDs (strings) or city objects with id, city_id, and city_name")


class SummarySheetRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    
    startDate: str = Field(..., description="Start date for filtering (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)")
    endDate: str = Field(..., description="End date for filtering (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)")
    stores: List[str] = Field(..., description="List of store codes to filter")


@router.get("/populate-threepo-dashboard")
async def check_reconciliation_status(
    db: AsyncSession = Depends(get_main_db),  # Use main_db for reconciliation tables
    current_user: UserDetails = Depends(get_current_user)
):
    """Check reconciliation status - Populates 3PO dashboard data (matching Node.js implementation)"""
    logger.info("===========================================")
    logger.info("🚀 I AM HERE - Entry point of /api/reconciliation/populate-threepo-dashboard")
    logger.info("===========================================")
    logger.info(f"📅 Timestamp: {datetime.utcnow().isoformat()}")
    
    try:
        from sqlalchemy.sql import text
        from app.models.main.reconciliation import ZomatoVsPosSummary
        from decimal import Decimal
        
        # Import helper functions
        async def create_pos_summary_records(db: AsyncSession):
            """Create POS summary records from orders table"""
            logger.info("📍 [createPosSummaryRecords] Function started")
            try:
                # Get total count first
                logger.info("📍 [createPosSummaryRecords] Getting total count of POS orders...")
                count_query = text("SELECT COUNT(*) as cnt FROM orders WHERE online_order_taker = 'ZOMATO'")
                
                logger.info(f"📊 [SQL] createPosSummaryRecords - Count Query:")
                logger.info(f"   {count_query}")
                
                result = await db.execute(count_query)
                row = result.fetchone()
                total_count = row.cnt if row else 0
                logger.info(f"📍 [createPosSummaryRecords] Total POS orders found: {total_count}")
                
                if total_count == 0:
                    logger.info("📍 [createPosSummaryRecords] No POS orders found")
                    return {
                        "processed": 0,
                        "updated": 0,
                        "errors": 0
                    }
                
                # Process in batches to avoid memory issues
                BATCH_SIZE = 1000
                offset = 0
                total_processed = 0
                total_created = 0
                total_updated = 0
                
                while offset < total_count:
                    logger.info(f"📍 [createPosSummaryRecords] Fetching batch: offset={offset}, limit={BATCH_SIZE}")
                    
                    # Fetch batch of POS orders
                    batch_query = text("""
                        SELECT 
                            instance_id,
                            store_name,
                            date,
                            payment,
                            subtotal,
                            discount,
                            net_sale,
                            gst_at_5_percent,
                            gst_ecom_at_5_percent,
                            packaging_charge,
                            gross_amount
                        FROM orders
                        WHERE online_order_taker = 'ZOMATO'
                        ORDER BY instance_id ASC
                        LIMIT :limit OFFSET :offset
                    """)
                    
                    logger.info(f"📊 [SQL] createPosSummaryRecords - Batch Query (offset {offset}):")
                    logger.info(f"   {batch_query}")
                    
                    result = await db.execute(
                        batch_query,
                        {"limit": BATCH_SIZE, "offset": offset}
                    )
                    pos_orders = result.fetchall()
                    logger.info(f"📍 [createPosSummaryRecords] Fetched {len(pos_orders)} orders in this batch")
                    
                    if not pos_orders or len(pos_orders) == 0:
                        break
                    
                    # Get existing records for this batch
                    # Check both pos_order_id AND zomato_order_id (for matching)
                    order_ids = [order.instance_id for order in pos_orders if order.instance_id]
                    if order_ids:
                        placeholders = ",".join([f":id_{i}" for i in range(len(order_ids))])
                        existing_query = text(f"""
                            SELECT id, pos_order_id, zomato_order_id
                            FROM zomato_vs_pos_summary
                            WHERE pos_order_id IN ({placeholders})
                               OR zomato_order_id IN ({placeholders})
                        """)
                        params_existing = {f"id_{i}": order_id for i, order_id in enumerate(order_ids)}
                        
                        logger.info(f"📊 [SQL] createPosSummaryRecords - Check Existing Records Query (batch {offset}):")
                        logger.info(f"   {existing_query}")
                        
                        result_existing = await db.execute(existing_query, params_existing)
                        existing_records = {}
                        for row in result_existing.fetchall():
                            # Match by pos_order_id first, then by zomato_order_id
                            if row.pos_order_id:
                                existing_records[row.pos_order_id] = row.id
                            if row.zomato_order_id:
                                existing_records[row.zomato_order_id] = row.id
                        logger.info(f"📍 [createPosSummaryRecords] Found {len(existing_records)} existing records to update")
                    else:
                        existing_records = {}
                    
                    # Process orders and prepare bulk operations
                    bulk_create_records = []
                    bulk_update_records = []
                    
                    logger.info(f"📍 [createPosSummaryRecords] Processing {len(pos_orders)} orders in batch {offset}...")
                    for idx, order in enumerate(pos_orders):
                        try:
                            if (idx + 1) % 100 == 0:
                                logger.info(f"📍 [createPosSummaryRecords] Processing order {idx + 1}/{len(pos_orders)} in batch {offset}")
                            
                            # Calculate slab rate - use net_sale, not payment (payment is a method string like "ONLINE")
                            # net_sale is the actual amount for POS calculations
                            # POS amounts are stored as negative in orders table, so use ABS for calculations
                            pos_net_amount_raw = float(order.net_sale or 0)
                            # CRITICAL: Convert negative to positive - POS stores negative, we need positive
                            pos_net_amount = abs(pos_net_amount_raw)  # Convert to positive for calculations
                            # Debug: Log if we're getting negative values
                            if pos_net_amount_raw < 0 and pos_net_amount <= 0:
                                logger.warning(f"⚠️ [createPosSummaryRecords] ABS conversion issue for order {order.instance_id}: raw={pos_net_amount_raw}, abs={pos_net_amount}")
                            
                            if pos_net_amount < 400:
                                slab_rate = 0.165
                            elif pos_net_amount < 450:
                                slab_rate = 0.1525
                            elif pos_net_amount < 500:
                                slab_rate = 0.145
                            elif pos_net_amount < 550:
                                slab_rate = 0.1375
                            elif pos_net_amount < 600:
                                slab_rate = 0.1325
                            else:
                                slab_rate = 0.1275
                            
                            # Calculate POS values (using positive net_sale as base)
                            pos_tax_paid_by_customer = pos_net_amount * 0.05
                            pos_commission_value = pos_net_amount * slab_rate
                            pos_pg_applied_on = pos_net_amount + pos_tax_paid_by_customer
                            pos_pg_charge = pos_pg_applied_on * 0.011
                            pos_taxes_zomato_fee = (pos_commission_value + pos_pg_charge) * 0.18
                            pos_tds_amount = pos_net_amount * 0.001
                            pos_final_amount = pos_net_amount - pos_commission_value - pos_pg_charge - pos_taxes_zomato_fee - pos_tds_amount
                            
                            # Ensure all POS amounts are positive (safeguard - should already be positive from ABS above)
                            # Force positive values - this is critical for reconciliation
                            pos_net_amount = abs(float(pos_net_amount))
                            pos_tax_paid_by_customer = abs(float(pos_tax_paid_by_customer))
                            pos_commission_value = abs(float(pos_commission_value))
                            pos_pg_applied_on = abs(float(pos_pg_applied_on))
                            pos_pg_charge = abs(float(pos_pg_charge))
                            pos_taxes_zomato_fee = abs(float(pos_taxes_zomato_fee))
                            pos_tds_amount = abs(float(pos_tds_amount))
                            pos_final_amount = abs(float(pos_final_amount))
                            
                            # Verify values are positive before storing
                            if pos_net_amount < 0 or pos_final_amount < 0:
                                logger.error(f"❌ [createPosSummaryRecords] CRITICAL: Negative values detected for order {order.instance_id}: net={pos_net_amount}, final={pos_final_amount}")
                            
                            record_data = {
                                "pos_order_id": order.instance_id,
                                "store_name": order.store_name,
                                "order_date": order.date,
                                "pos_net_amount": Decimal(str(pos_net_amount)),
                                "pos_tax_paid_by_customer": Decimal(str(pos_tax_paid_by_customer)),
                                "pos_commission_value": Decimal(str(pos_commission_value)),
                                "pos_pg_applied_on": Decimal(str(pos_pg_applied_on)),
                                "pos_pg_charge": Decimal(str(pos_pg_charge)),
                                "pos_taxes_zomato_fee": Decimal(str(pos_taxes_zomato_fee)),
                                "pos_tds_amount": Decimal(str(pos_tds_amount)),
                                "pos_final_amount": Decimal(str(pos_final_amount)),
                                "order_status_pos": "Delivered",
                                "updated_at": datetime.utcnow(),
                            }
                            
                            # Check if record exists by pos_order_id OR zomato_order_id
                            record_id = None
                            if order.instance_id in existing_records:
                                record_id = existing_records[order.instance_id]
                            # Also check if there's a Zomato record with matching zomato_order_id
                            if not record_id:
                                check_zomato_query = text("""
                                    SELECT id FROM zomato_vs_pos_summary
                                    WHERE zomato_order_id = :instance_id
                                    LIMIT 1
                                """)
                                result_zomato = await db.execute(check_zomato_query, {"instance_id": order.instance_id})
                                zomato_row = result_zomato.fetchone()
                                if zomato_row:
                                    record_id = zomato_row.id
                            
                            if record_id:
                                # Update existing record (could be POS or Zomato record)
                                record_data["id"] = record_id
                                bulk_update_records.append(record_data)
                            else:
                                # Create new record
                                record_data["id"] = f"ZVS_{order.instance_id}"
                                record_data["reconciled_status"] = "PENDING"
                                record_data["created_at"] = datetime.utcnow()
                                bulk_create_records.append(record_data)
                                
                        except Exception as error:
                            logger.error(f"Error processing order {order.instance_id if order else 'unknown'}: {error}")
                    
                    # Perform bulk create - optimized with batch commits
                    if bulk_create_records:
                        logger.info(f"📍 [createPosSummaryRecords] Bulk creating {len(bulk_create_records)} records...")
                        insert_query = text("""
                            INSERT INTO zomato_vs_pos_summary (
                                id, pos_order_id, store_name, order_date,
                                pos_net_amount, pos_tax_paid_by_customer, pos_commission_value,
                                pos_pg_applied_on, pos_pg_charge, pos_taxes_zomato_fee,
                                pos_tds_amount, pos_final_amount, order_status_pos,
                                reconciled_status, created_at, updated_at
                            ) VALUES (
                                :id, :pos_order_id, :store_name, :order_date,
                                :pos_net_amount, :pos_tax_paid_by_customer, :pos_commission_value,
                                :pos_pg_applied_on, :pos_pg_charge, :pos_taxes_zomato_fee,
                                :pos_tds_amount, :pos_final_amount, :order_status_pos,
                                :reconciled_status, :created_at, :updated_at
                            )
                            ON DUPLICATE KEY UPDATE
                                pos_order_id = VALUES(pos_order_id),
                                zomato_order_id = COALESCE(VALUES(zomato_order_id), zomato_order_id),
                                store_name = VALUES(store_name),
                                order_date = VALUES(order_date),
                                pos_net_amount = VALUES(pos_net_amount),
                                pos_tax_paid_by_customer = VALUES(pos_tax_paid_by_customer),
                                pos_commission_value = VALUES(pos_commission_value),
                                pos_pg_applied_on = VALUES(pos_pg_applied_on),
                                pos_pg_charge = VALUES(pos_pg_charge),
                                pos_taxes_zomato_fee = VALUES(pos_taxes_zomato_fee),
                                pos_tds_amount = VALUES(pos_tds_amount),
                                pos_final_amount = VALUES(pos_final_amount),
                                order_status_pos = VALUES(order_status_pos),
                                updated_at = VALUES(updated_at)
                        """)
                        # Process in smaller sub-batches and commit once per batch (more efficient than individual commits)
                        SUB_BATCH_SIZE = 100
                        for i in range(0, len(bulk_create_records), SUB_BATCH_SIZE):
                            sub_batch = bulk_create_records[i:i + SUB_BATCH_SIZE]
                            for record in sub_batch:
                                try:
                                    await db.execute(insert_query, record)
                                except Exception as e:
                                    logger.error(f"Error creating record {record.get('id')}: {e}")
                            await db.commit()
                        total_created += len(bulk_create_records)
                        logger.info(f"📍 [createPosSummaryRecords] Created {len(bulk_create_records)} records in batch {offset}")
                    
                    # Perform bulk update - optimized with batch commits
                    if bulk_update_records:
                        logger.info(f"📍 [createPosSummaryRecords] Bulk updating {len(bulk_update_records)} records...")
                        update_query = text("""
                            UPDATE zomato_vs_pos_summary
                            SET 
                                pos_order_id = :pos_order_id,
                                zomato_order_id = COALESCE(zomato_order_id, :pos_order_id),
                                store_name = :store_name,
                                order_date = :order_date,
                                pos_net_amount = :pos_net_amount,
                                pos_tax_paid_by_customer = :pos_tax_paid_by_customer,
                                pos_commission_value = :pos_commission_value,
                                pos_pg_applied_on = :pos_pg_applied_on,
                                pos_pg_charge = :pos_pg_charge,
                                pos_taxes_zomato_fee = :pos_taxes_zomato_fee,
                                pos_tds_amount = :pos_tds_amount,
                                pos_final_amount = :pos_final_amount,
                                order_status_pos = :order_status_pos,
                                updated_at = :updated_at
                            WHERE id = :id
                        """)
                        # Process in smaller sub-batches and commit once per batch (more efficient than individual commits)
                        SUB_BATCH_SIZE = 100
                        for i in range(0, len(bulk_update_records), SUB_BATCH_SIZE):
                            sub_batch = bulk_update_records[i:i + SUB_BATCH_SIZE]
                            for record in sub_batch:
                                try:
                                    await db.execute(update_query, record)
                                except Exception as e:
                                    logger.error(f"Error updating record {record.get('id')}: {e}")
                            await db.commit()
                        total_updated += len(bulk_update_records)
                        logger.info(f"📍 [createPosSummaryRecords] Updated {len(bulk_update_records)} records in batch {offset}")
                    
                    total_processed += len(pos_orders)
                    offset += BATCH_SIZE
                    logger.info(f"📍 [createPosSummaryRecords] POS Summary batch: {offset}/{total_count} orders ({round((offset/total_count)*100)}%)")
                
                logger.info(f"\n✅ [createPosSummaryRecords] Function completed successfully")
                logger.info(f"📍 [createPosSummaryRecords] Total processed: {total_processed} orders")
                logger.info(f"📍 [createPosSummaryRecords] Total created: {total_created} new records")
                logger.info(f"📍 [createPosSummaryRecords] Total updated: {total_updated} existing records")
                
                # Return counts for aggregation in main function
                return {
                    "processed": total_processed,
                    "updated": total_created + total_updated,
                    "errors": 0
                }
                
            except Exception as error:
                logger.error(f"❌ [createPosSummaryRecords] Error occurred: {error}")
                logger.error(f"   Error message: {error.message if hasattr(error, 'message') else str(error)}")
                logger.error(f"   Error stack: {error.__traceback__}")
                return {
                    "processed": 0,
                    "updated": 0,
                    "errors": 1
                }
        
        async def calculate_slab_rate(net_amount: float) -> float:
            """Calculate slab rate based on net amount"""
            if net_amount < 400:
                return 0.165
            elif net_amount < 450:
                return 0.1525
            elif net_amount < 500:
                return 0.145
            elif net_amount < 550:
                return 0.1375
            elif net_amount < 600:
                return 0.1325
            else:
                return 0.1275
        
        async def create_zomato_summary_records(db: AsyncSession):
            """Create Zomato summary records from zomato table (sale/addition)"""
            logger.info("📍 [createZomatoSummaryRecords] Function started")
            try:
                # Get total count first
                logger.info("📍 [createZomatoSummaryRecords] Getting total count of Zomato orders (sale/addition)...")
                count_query = text("SELECT COUNT(*) as cnt FROM zomato WHERE action IN ('sale', 'addition')")
                
                logger.info(f"📊 [SQL] createZomatoSummaryRecords - Count Query:")
                logger.info(f"   {count_query}")
                
                result = await db.execute(count_query)
                row = result.fetchone()
                total_count = row.cnt if row else 0
                logger.info(f"📍 [createZomatoSummaryRecords] Total Zomato orders found: {total_count}")
                
                if total_count == 0:
                    logger.info("📍 [createZomatoSummaryRecords] No Zomato orders found")
                    return {
                        "processed": 0,
                        "updated": 0,
                        "errors": 0
                    }
                
                # Process in batches to avoid memory issues
                BATCH_SIZE = 1000
                offset = 0
                total_processed = 0
                total_created = 0
                total_updated = 0
                
                while offset < total_count:
                    logger.info(f"📍 [createZomatoSummaryRecords] Fetching batch: offset={offset}, limit={BATCH_SIZE}")
                    
                    # Fetch batch of Zomato orders
                    batch_query = text("""
                        SELECT 
                            order_id, store_code, order_date, action,
                            bill_subtotal, mvd, merchant_pack_charge,
                            net_amount, tax_paid_by_customer, commission_value,
                            pg_applied_on, pgcharge, taxes_zomato_fee, tds_amount, final_amount,
                            credit_note_amount, pro_discount_passthrough, customer_discount,
                            rejection_penalty_charge, user_credits_charge, promo_recovery_adj,
                            icecream_handling, icecream_deductions, order_support_cost,
                            merchant_delivery_charge
                        FROM zomato
                        WHERE action IN ('sale', 'addition')
                        ORDER BY order_id ASC
                        LIMIT :limit OFFSET :offset
                    """)
                    
                    logger.info(f"📊 [SQL] createZomatoSummaryRecords - Batch Query (offset {offset}):")
                    logger.info(f"   {batch_query}")
                    
                    result = await db.execute(
                        batch_query,
                        {"limit": BATCH_SIZE, "offset": offset}
                    )
                    zomato_orders = result.fetchall()
                    logger.info(f"📍 [createZomatoSummaryRecords] Fetched {len(zomato_orders)} orders in this batch")
                    
                    if not zomato_orders or len(zomato_orders) == 0:
                        break
                    
                    # Get existing records for this batch
                    # Check both zomato_order_id AND pos_order_id (for matching)
                    order_ids = [order.order_id for order in zomato_orders if order.order_id]
                    if order_ids:
                        placeholders = ",".join([f":id_{i}" for i in range(len(order_ids))])
                        existing_query = text(f"""
                            SELECT id, zomato_order_id, pos_order_id
                            FROM zomato_vs_pos_summary
                            WHERE zomato_order_id IN ({placeholders})
                               OR pos_order_id IN ({placeholders})
                        """)
                        params_existing = {f"id_{i}": order_id for i, order_id in enumerate(order_ids)}
                        
                        logger.info(f"📊 [SQL] createZomatoSummaryRecords - Check Existing Records Query (batch {offset}):")
                        logger.info(f"   {existing_query}")
                        
                        result_existing = await db.execute(existing_query, params_existing)
                        existing_records = {}
                        for row in result_existing.fetchall():
                            # Match by zomato_order_id first, then by pos_order_id
                            if row.zomato_order_id:
                                existing_records[row.zomato_order_id] = row.id
                            if row.pos_order_id:
                                existing_records[row.pos_order_id] = row.id
                        logger.info(f"📍 [createZomatoSummaryRecords] Found {len(existing_records)} existing records to update")
                    else:
                        existing_records = {}
                    
                    # Process orders and prepare bulk operations
                    bulk_create_records = []
                    bulk_update_records = []
                    
                    logger.info(f"📍 [createZomatoSummaryRecords] Processing {len(zomato_orders)} orders in batch {offset}...")
                    for idx, order in enumerate(zomato_orders):
                        try:
                            if (idx + 1) % 100 == 0:
                                logger.info(f"📍 [createZomatoSummaryRecords] Processing order {idx + 1}/{len(zomato_orders)} in batch {offset}")
                            
                            # Get values from zomato table
                            bill_subtotal = float(order.bill_subtotal or 0)
                            mvd = float(order.mvd or 0)
                            merchant_pack_charge = float(order.merchant_pack_charge or 0)
                            net_amount = float(order.net_amount or 0)
                            
                            # Calculate slab rate
                            slab_rate = await calculate_slab_rate(net_amount)
                            
                            # Calculate Zomato values using formulas
                            calculated_zomato_net_amount = bill_subtotal - mvd + merchant_pack_charge
                            calculated_zomato_tax_paid_by_customer = calculated_zomato_net_amount * 0.05
                            calculated_zomato_commission_value = calculated_zomato_net_amount * slab_rate
                            calculated_zomato_pg_applied_on = calculated_zomato_net_amount + calculated_zomato_tax_paid_by_customer
                            calculated_zomato_pg_charge = calculated_zomato_pg_applied_on * 0.011
                            calculated_zomato_taxes_zomato_fee = (calculated_zomato_commission_value + calculated_zomato_pg_charge) * 0.18
                            # tds_amount = (bill_subtotal + merchant_pack_charge – mvd) * 0.001 (per user's formula)
                            calculated_zomato_tds_amount = (bill_subtotal + merchant_pack_charge - mvd) * 0.001
                            calculated_zomato_final_amount = calculated_zomato_net_amount - calculated_zomato_commission_value - calculated_zomato_pg_charge - calculated_zomato_taxes_zomato_fee - calculated_zomato_tds_amount
                            
                            # Get actual values from zomato table (or use calculated if not available)
                            zomato_net_amount = float(order.net_amount or calculated_zomato_net_amount)
                            zomato_tax_paid_by_customer = float(order.tax_paid_by_customer or calculated_zomato_tax_paid_by_customer)
                            zomato_commission_value = float(order.commission_value or calculated_zomato_commission_value)
                            zomato_pg_applied_on = float(order.pg_applied_on or calculated_zomato_pg_applied_on)
                            zomato_pg_charge = float(order.pgcharge or calculated_zomato_pg_charge)
                            zomato_taxes_zomato_fee = float(order.taxes_zomato_fee or calculated_zomato_taxes_zomato_fee)
                            zomato_tds_amount = float(order.tds_amount or calculated_zomato_tds_amount)
                            zomato_final_amount = float(order.final_amount or calculated_zomato_final_amount)
                            
                            record_data = {
                                "zomato_order_id": order.order_id,
                                "store_name": order.store_code,
                                "order_date": order.order_date,
                                "zomato_net_amount": Decimal(str(zomato_net_amount)),
                                "zomato_tax_paid_by_customer": Decimal(str(zomato_tax_paid_by_customer)),
                                "zomato_commission_value": Decimal(str(zomato_commission_value)),
                                "zomato_pg_applied_on": Decimal(str(zomato_pg_applied_on)),
                                "zomato_pg_charge": Decimal(str(zomato_pg_charge)),
                                "zomato_taxes_zomato_fee": Decimal(str(zomato_taxes_zomato_fee)),
                                "zomato_tds_amount": Decimal(str(zomato_tds_amount)),
                                "zomato_final_amount": Decimal(str(zomato_final_amount)),
                                "calculated_zomato_net_amount": Decimal(str(calculated_zomato_net_amount)),
                                "calculated_zomato_tax_paid_by_customer": Decimal(str(calculated_zomato_tax_paid_by_customer)),
                                "calculated_zomato_commission_value": Decimal(str(calculated_zomato_commission_value)),
                                "calculated_zomato_pg_applied_on": Decimal(str(calculated_zomato_pg_applied_on)),
                                "calculated_zomato_pg_charge": Decimal(str(calculated_zomato_pg_charge)),
                                "calculated_zomato_taxes_zomato_fee": Decimal(str(calculated_zomato_taxes_zomato_fee)),
                                "calculated_zomato_tds_amount": Decimal(str(calculated_zomato_tds_amount)),
                                "calculated_zomato_final_amount": Decimal(str(calculated_zomato_final_amount)),
                                "fixed_credit_note_amount": Decimal(str(order.credit_note_amount or 0)),
                                "fixed_pro_discount_passthrough": Decimal(str(order.pro_discount_passthrough or 0)),
                                "fixed_customer_discount": Decimal(str(order.customer_discount or 0)),
                                "fixed_rejection_penalty_charge": Decimal(str(order.rejection_penalty_charge or 0)),
                                "fixed_user_credits_charge": Decimal(str(order.user_credits_charge or 0)),
                                "fixed_promo_recovery_adj": Decimal(str(order.promo_recovery_adj or 0)),
                                "fixed_icecream_handling": Decimal(str(order.icecream_handling or 0)),
                                "fixed_icecream_deductions": Decimal(str(order.icecream_deductions or 0)),
                                "fixed_order_support_cost": Decimal(str(order.order_support_cost or 0)),
                                "fixed_merchant_delivery_charge": Decimal(str(order.merchant_delivery_charge or 0)),
                                "action": order.action,  # Store action separately
                                "order_status_zomato": "Delivered",  # Set proper status
                                "updated_at": datetime.utcnow(),
                            }
                            
                            # Check if record exists by zomato_order_id OR pos_order_id
                            record_id = None
                            if order.order_id in existing_records:
                                record_id = existing_records[order.order_id]
                            # Also check if there's a POS record with matching pos_order_id
                            if not record_id:
                                check_pos_query = text("""
                                    SELECT id FROM zomato_vs_pos_summary
                                    WHERE pos_order_id = :order_id
                                    LIMIT 1
                                """)
                                result_pos = await db.execute(check_pos_query, {"order_id": order.order_id})
                                pos_row = result_pos.fetchone()
                                if pos_row:
                                    record_id = pos_row.id
                            
                            if record_id:
                                # Update existing record (could be POS or Zomato record)
                                record_data["id"] = record_id
                                bulk_update_records.append(record_data)
                            else:
                                # Create new record
                                record_data["id"] = f"ZVS_{order.order_id}"
                                record_data["reconciled_status"] = "PENDING"
                                record_data["created_at"] = datetime.utcnow()
                                bulk_create_records.append(record_data)
                                
                        except Exception as error:
                            logger.error(f"Error processing order {order.order_id if order else 'unknown'}: {error}")
                    
                    # Perform bulk create - optimized with batch commits
                    if bulk_create_records:
                        logger.info(f"📍 [createZomatoSummaryRecords] Bulk creating {len(bulk_create_records)} records...")
                        insert_query = text("""
                            INSERT INTO zomato_vs_pos_summary (
                                id, zomato_order_id, store_name, order_date,
                                zomato_net_amount, zomato_tax_paid_by_customer, zomato_commission_value,
                                zomato_pg_applied_on, zomato_pg_charge, zomato_taxes_zomato_fee,
                                zomato_tds_amount, zomato_final_amount,
                                calculated_zomato_net_amount, calculated_zomato_tax_paid_by_customer,
                                calculated_zomato_commission_value, calculated_zomato_pg_applied_on,
                                calculated_zomato_pg_charge, calculated_zomato_taxes_zomato_fee,
                                calculated_zomato_tds_amount, calculated_zomato_final_amount,
                                fixed_credit_note_amount, fixed_pro_discount_passthrough,
                                fixed_customer_discount, fixed_rejection_penalty_charge,
                                fixed_user_credits_charge, fixed_promo_recovery_adj,
                                fixed_icecream_handling, fixed_icecream_deductions,
                                fixed_order_support_cost, fixed_merchant_delivery_charge,
                                action, order_status_zomato, reconciled_status, created_at, updated_at
                            ) VALUES (
                                :id, :zomato_order_id, :store_name, :order_date,
                                :zomato_net_amount, :zomato_tax_paid_by_customer, :zomato_commission_value,
                                :zomato_pg_applied_on, :zomato_pg_charge, :zomato_taxes_zomato_fee,
                                :zomato_tds_amount, :zomato_final_amount,
                                :calculated_zomato_net_amount, :calculated_zomato_tax_paid_by_customer,
                                :calculated_zomato_commission_value, :calculated_zomato_pg_applied_on,
                                :calculated_zomato_pg_charge, :calculated_zomato_taxes_zomato_fee,
                                :calculated_zomato_tds_amount, :calculated_zomato_final_amount,
                                :fixed_credit_note_amount, :fixed_pro_discount_passthrough,
                                :fixed_customer_discount, :fixed_rejection_penalty_charge,
                                :fixed_user_credits_charge, :fixed_promo_recovery_adj,
                                :fixed_icecream_handling, :fixed_icecream_deductions,
                                :fixed_order_support_cost, :fixed_merchant_delivery_charge,
                                :action, :order_status_zomato, :reconciled_status, :created_at, :updated_at
                            )
                            ON DUPLICATE KEY UPDATE
                                zomato_order_id = VALUES(zomato_order_id),
                                store_name = VALUES(store_name),
                                order_date = VALUES(order_date),
                                zomato_net_amount = VALUES(zomato_net_amount),
                                zomato_tax_paid_by_customer = VALUES(zomato_tax_paid_by_customer),
                                zomato_commission_value = VALUES(zomato_commission_value),
                                zomato_pg_applied_on = VALUES(zomato_pg_applied_on),
                                zomato_pg_charge = VALUES(zomato_pg_charge),
                                zomato_taxes_zomato_fee = VALUES(zomato_taxes_zomato_fee),
                                zomato_tds_amount = VALUES(zomato_tds_amount),
                                zomato_final_amount = VALUES(zomato_final_amount),
                                calculated_zomato_net_amount = VALUES(calculated_zomato_net_amount),
                                calculated_zomato_tax_paid_by_customer = VALUES(calculated_zomato_tax_paid_by_customer),
                                calculated_zomato_commission_value = VALUES(calculated_zomato_commission_value),
                                calculated_zomato_pg_applied_on = VALUES(calculated_zomato_pg_applied_on),
                                calculated_zomato_pg_charge = VALUES(calculated_zomato_pg_charge),
                                calculated_zomato_taxes_zomato_fee = VALUES(calculated_zomato_taxes_zomato_fee),
                                calculated_zomato_tds_amount = VALUES(calculated_zomato_tds_amount),
                                calculated_zomato_final_amount = VALUES(calculated_zomato_final_amount),
                                fixed_credit_note_amount = VALUES(fixed_credit_note_amount),
                                fixed_pro_discount_passthrough = VALUES(fixed_pro_discount_passthrough),
                                fixed_customer_discount = VALUES(fixed_customer_discount),
                                fixed_rejection_penalty_charge = VALUES(fixed_rejection_penalty_charge),
                                fixed_user_credits_charge = VALUES(fixed_user_credits_charge),
                                fixed_promo_recovery_adj = VALUES(fixed_promo_recovery_adj),
                                fixed_icecream_handling = VALUES(fixed_icecream_handling),
                                fixed_icecream_deductions = VALUES(fixed_icecream_deductions),
                                fixed_order_support_cost = VALUES(fixed_order_support_cost),
                                fixed_merchant_delivery_charge = VALUES(fixed_merchant_delivery_charge),
                                action = VALUES(action),
                                order_status_zomato = VALUES(order_status_zomato),
                                updated_at = VALUES(updated_at)
                        """)
                        # Process in smaller sub-batches and commit once per batch
                        SUB_BATCH_SIZE = 100
                        for i in range(0, len(bulk_create_records), SUB_BATCH_SIZE):
                            sub_batch = bulk_create_records[i:i + SUB_BATCH_SIZE]
                            for record in sub_batch:
                                try:
                                    await db.execute(insert_query, record)
                                except Exception as e:
                                    logger.error(f"Error creating record {record.get('id')}: {e}")
                            await db.commit()
                        total_created += len(bulk_create_records)
                        logger.info(f"📍 [createZomatoSummaryRecords] Created {len(bulk_create_records)} records in batch {offset}")
                    
                    # Perform bulk update - optimized with batch commits
                    if bulk_update_records:
                        logger.info(f"📍 [createZomatoSummaryRecords] Bulk updating {len(bulk_update_records)} records...")
                        update_query = text("""
                            UPDATE zomato_vs_pos_summary
                            SET 
                                zomato_order_id = :zomato_order_id,
                                pos_order_id = COALESCE(pos_order_id, :zomato_order_id),
                                store_name = :store_name,
                                order_date = :order_date,
                                zomato_net_amount = :zomato_net_amount,
                                zomato_tax_paid_by_customer = :zomato_tax_paid_by_customer,
                                zomato_commission_value = :zomato_commission_value,
                                zomato_pg_applied_on = :zomato_pg_applied_on,
                                zomato_pg_charge = :zomato_pg_charge,
                                zomato_taxes_zomato_fee = :zomato_taxes_zomato_fee,
                                zomato_tds_amount = :zomato_tds_amount,
                                zomato_final_amount = :zomato_final_amount,
                                calculated_zomato_net_amount = :calculated_zomato_net_amount,
                                calculated_zomato_tax_paid_by_customer = :calculated_zomato_tax_paid_by_customer,
                                calculated_zomato_commission_value = :calculated_zomato_commission_value,
                                calculated_zomato_pg_applied_on = :calculated_zomato_pg_applied_on,
                                calculated_zomato_pg_charge = :calculated_zomato_pg_charge,
                                calculated_zomato_taxes_zomato_fee = :calculated_zomato_taxes_zomato_fee,
                                calculated_zomato_tds_amount = :calculated_zomato_tds_amount,
                                calculated_zomato_final_amount = :calculated_zomato_final_amount,
                                fixed_credit_note_amount = :fixed_credit_note_amount,
                                fixed_pro_discount_passthrough = :fixed_pro_discount_passthrough,
                                fixed_customer_discount = :fixed_customer_discount,
                                fixed_rejection_penalty_charge = :fixed_rejection_penalty_charge,
                                fixed_user_credits_charge = :fixed_user_credits_charge,
                                fixed_promo_recovery_adj = :fixed_promo_recovery_adj,
                                fixed_icecream_handling = :fixed_icecream_handling,
                                fixed_icecream_deductions = :fixed_icecream_deductions,
                                fixed_order_support_cost = :fixed_order_support_cost,
                                fixed_merchant_delivery_charge = :fixed_merchant_delivery_charge,
                                action = :action,
                                order_status_zomato = :order_status_zomato,
                                updated_at = :updated_at
                            WHERE id = :id
                        """)
                        # Process in smaller sub-batches and commit once per batch
                        SUB_BATCH_SIZE = 100
                        for i in range(0, len(bulk_update_records), SUB_BATCH_SIZE):
                            sub_batch = bulk_update_records[i:i + SUB_BATCH_SIZE]
                            for record in sub_batch:
                                try:
                                    await db.execute(update_query, record)
                                except Exception as e:
                                    logger.error(f"Error updating record {record.get('id')}: {e}")
                            await db.commit()
                        total_updated += len(bulk_update_records)
                        logger.info(f"📍 [createZomatoSummaryRecords] Updated {len(bulk_update_records)} records in batch {offset}")
                    
                    total_processed += len(zomato_orders)
                    offset += BATCH_SIZE
                    logger.info(f"📍 [createZomatoSummaryRecords] Zomato Summary batch: {offset}/{total_count} orders ({round((offset/total_count)*100)}%)")
                
                logger.info(f"\n✅ [createZomatoSummaryRecords] Function completed successfully")
                logger.info(f"📍 [createZomatoSummaryRecords] Total processed: {total_processed} Zomato orders")
                logger.info(f"📍 [createZomatoSummaryRecords] Total created: {total_created} new records")
                logger.info(f"📍 [createZomatoSummaryRecords] Total updated: {total_updated} existing records")
                
                # Return counts for aggregation in main function
                return {
                    "processed": total_processed,
                    "updated": total_created + total_updated,
                    "errors": 0
                }
                
            except Exception as error:
                logger.error(f"❌ [createZomatoSummaryRecords] Error occurred: {error}")
                logger.error(f"   Error message: {error.message if hasattr(error, 'message') else str(error)}")
                logger.error(f"   Error stack: {error.__traceback__}")
                return {
                    "processed": 0,
                    "updated": 0,
                    "errors": 1
                }
        
        async def create_zomato_summary_records_for_refund_only(db: AsyncSession):
            """Create Zomato summary records for refunds only from zomato table"""
            logger.info("📍 [createZomatoSummaryRecordsForRefundOnly] Function started")
            try:
                # Get total count first
                logger.info("📍 [createZomatoSummaryRecordsForRefundOnly] Getting total count of Zomato refund orders...")
                count_query = text("SELECT COUNT(*) as cnt FROM zomato WHERE action = 'refund'")
                
                logger.info(f"📊 [SQL] createZomatoSummaryRecordsForRefundOnly - Count Query:")
                logger.info(f"   {count_query}")
                
                result = await db.execute(count_query)
                row = result.fetchone()
                total_count = row.cnt if row else 0
                logger.info(f"📍 [createZomatoSummaryRecordsForRefundOnly] Total refund orders found: {total_count}")
                
                if total_count == 0:
                    logger.info("📍 [createZomatoSummaryRecordsForRefundOnly] No Zomato refund orders found")
                    return {
                        "processed": 0,
                        "updated": 0,
                        "errors": 0
                    }
                
                # Process in batches to avoid memory issues
                BATCH_SIZE = 1000
                offset = 0
                total_processed = 0
                total_created = 0
                total_updated = 0
                
                while offset < total_count:
                    logger.info(f"📍 [createZomatoSummaryRecordsForRefundOnly] Fetching batch: offset={offset}, limit={BATCH_SIZE}")
                    
                    # Fetch batch of Zomato refund orders
                    batch_query = text("""
                        SELECT 
                            order_id, store_code, order_date, action,
                            bill_subtotal, mvd, merchant_pack_charge,
                            net_amount, tax_paid_by_customer, commission_value,
                            pg_applied_on, pgcharge, taxes_zomato_fee, tds_amount, final_amount,
                            credit_note_amount, pro_discount_passthrough, customer_discount,
                            rejection_penalty_charge, user_credits_charge, promo_recovery_adj,
                            icecream_handling, icecream_deductions, order_support_cost,
                            merchant_delivery_charge
                        FROM zomato
                        WHERE action = 'refund'
                        ORDER BY order_id ASC
                        LIMIT :limit OFFSET :offset
                    """)
                    
                    logger.info(f"📊 [SQL] createZomatoSummaryRecordsForRefundOnly - Batch Query (offset {offset}):")
                    logger.info(f"   {batch_query}")
                    
                    result = await db.execute(
                        batch_query,
                        {"limit": BATCH_SIZE, "offset": offset}
                    )
                    zomato_orders = result.fetchall()
                    logger.info(f"📍 [createZomatoSummaryRecordsForRefundOnly] Fetched {len(zomato_orders)} refund orders in this batch")
                    
                    if not zomato_orders or len(zomato_orders) == 0:
                        break
                    
                    # Get existing records for this batch (refund records only)
                    order_ids = [order.order_id for order in zomato_orders if order.order_id]
                    if order_ids:
                        placeholders = ",".join([f":id_{i}" for i in range(len(order_ids))])
                        existing_query = text(f"""
                            SELECT id, zomato_order_id
                            FROM zomato_vs_pos_summary
                            WHERE zomato_order_id IN ({placeholders})
                            AND order_status_zomato = 'refund'
                        """)
                        params_existing = {f"id_{i}": order_id for i, order_id in enumerate(order_ids)}
                        
                        logger.info(f"📊 [SQL] createZomatoSummaryRecordsForRefundOnly - Check Existing Records Query (batch {offset}):")
                        logger.info(f"   {existing_query}")
                        
                        result_existing = await db.execute(existing_query, params_existing)
                        existing_records = {row.zomato_order_id: row.id for row in result_existing.fetchall()}
                        logger.info(f"📍 [createZomatoSummaryRecordsForRefundOnly] Found {len(existing_records)} existing refund records to update")
                    else:
                        existing_records = {}
                    
                    # Process orders and prepare bulk operations
                    bulk_create_records = []
                    bulk_update_records = []
                    
                    logger.info(f"📍 [createZomatoSummaryRecordsForRefundOnly] Processing {len(zomato_orders)} refund orders in batch {offset}...")
                    for idx, order in enumerate(zomato_orders):
                        try:
                            if (idx + 1) % 100 == 0:
                                logger.info(f"📍 [createZomatoSummaryRecordsForRefundOnly] Processing refund order {idx + 1}/{len(zomato_orders)} in batch {offset}")
                            
                            # Get values from zomato table
                            bill_subtotal = float(order.bill_subtotal or 0)
                            mvd = float(order.mvd or 0)
                            merchant_pack_charge = float(order.merchant_pack_charge or 0)
                            net_amount = float(order.net_amount or 0)
                            
                            # Calculate slab rate
                            slab_rate = await calculate_slab_rate(net_amount)
                            
                            # Calculate Zomato values using formulas
                            calculated_zomato_net_amount = bill_subtotal - mvd + merchant_pack_charge
                            calculated_zomato_tax_paid_by_customer = calculated_zomato_net_amount * 0.05
                            calculated_zomato_commission_value = calculated_zomato_net_amount * slab_rate
                            calculated_zomato_pg_applied_on = calculated_zomato_net_amount + calculated_zomato_tax_paid_by_customer
                            calculated_zomato_pg_charge = calculated_zomato_pg_applied_on * 0.011
                            calculated_zomato_taxes_zomato_fee = (calculated_zomato_commission_value + calculated_zomato_pg_charge) * 0.18
                            # tds_amount = (bill_subtotal + merchant_pack_charge – mvd) * 0.001 (per user's formula)
                            calculated_zomato_tds_amount = (bill_subtotal + merchant_pack_charge - mvd) * 0.001
                            calculated_zomato_final_amount = calculated_zomato_net_amount - calculated_zomato_commission_value - calculated_zomato_pg_charge - calculated_zomato_taxes_zomato_fee - calculated_zomato_tds_amount
                            
                            # Get actual values from zomato table (or use calculated if not available)
                            zomato_net_amount = float(order.net_amount or calculated_zomato_net_amount)
                            zomato_tax_paid_by_customer = float(order.tax_paid_by_customer or calculated_zomato_tax_paid_by_customer)
                            zomato_commission_value = float(order.commission_value or calculated_zomato_commission_value)
                            zomato_pg_applied_on = float(order.pg_applied_on or calculated_zomato_pg_applied_on)
                            zomato_pg_charge = float(order.pgcharge or calculated_zomato_pg_charge)
                            zomato_taxes_zomato_fee = float(order.taxes_zomato_fee or calculated_zomato_taxes_zomato_fee)
                            zomato_tds_amount = float(order.tds_amount or calculated_zomato_tds_amount)
                            zomato_final_amount = float(order.final_amount or calculated_zomato_final_amount)
                            
                            record_data = {
                                "zomato_order_id": order.order_id,
                                "store_name": order.store_code,
                                "order_date": order.order_date,
                                "zomato_net_amount": Decimal(str(zomato_net_amount)),
                                "zomato_tax_paid_by_customer": Decimal(str(zomato_tax_paid_by_customer)),
                                "zomato_commission_value": Decimal(str(zomato_commission_value)),
                                "zomato_pg_applied_on": Decimal(str(zomato_pg_applied_on)),
                                "zomato_pg_charge": Decimal(str(zomato_pg_charge)),
                                "zomato_taxes_zomato_fee": Decimal(str(zomato_taxes_zomato_fee)),
                                "zomato_tds_amount": Decimal(str(zomato_tds_amount)),
                                "zomato_final_amount": Decimal(str(zomato_final_amount)),
                                "calculated_zomato_net_amount": Decimal(str(calculated_zomato_net_amount)),
                                "calculated_zomato_tax_paid_by_customer": Decimal(str(calculated_zomato_tax_paid_by_customer)),
                                "calculated_zomato_commission_value": Decimal(str(calculated_zomato_commission_value)),
                                "calculated_zomato_pg_applied_on": Decimal(str(calculated_zomato_pg_applied_on)),
                                "calculated_zomato_pg_charge": Decimal(str(calculated_zomato_pg_charge)),
                                "calculated_zomato_taxes_zomato_fee": Decimal(str(calculated_zomato_taxes_zomato_fee)),
                                "calculated_zomato_tds_amount": Decimal(str(calculated_zomato_tds_amount)),
                                "calculated_zomato_final_amount": Decimal(str(calculated_zomato_final_amount)),
                                "fixed_credit_note_amount": Decimal(str(order.credit_note_amount or 0)),
                                "fixed_pro_discount_passthrough": Decimal(str(order.pro_discount_passthrough or 0)),
                                "fixed_customer_discount": Decimal(str(order.customer_discount or 0)),
                                "fixed_rejection_penalty_charge": Decimal(str(order.rejection_penalty_charge or 0)),
                                "fixed_user_credits_charge": Decimal(str(order.user_credits_charge or 0)),
                                "fixed_promo_recovery_adj": Decimal(str(order.promo_recovery_adj or 0)),
                                "fixed_icecream_handling": Decimal(str(order.icecream_handling or 0)),
                                "fixed_icecream_deductions": Decimal(str(order.icecream_deductions or 0)),
                                "fixed_order_support_cost": Decimal(str(order.order_support_cost or 0)),
                                "fixed_merchant_delivery_charge": Decimal(str(order.merchant_delivery_charge or 0)),
                                "action": order.action,  # Store action separately
                                "order_status_zomato": "Refund",  # Set proper status
                                "updated_at": datetime.utcnow(),
                            }
                            
                            if order.order_id in existing_records:
                                # Update existing record
                                record_data["id"] = existing_records[order.order_id]
                                bulk_update_records.append(record_data)
                            else:
                                # Create new record
                                record_data["id"] = f"ZVS_refund_{order.order_id}"
                                record_data["reconciled_status"] = "PENDING"
                                record_data["created_at"] = datetime.utcnow()
                                bulk_create_records.append(record_data)
                                
                        except Exception as error:
                            logger.error(f"Error processing refund order {order.order_id if order else 'unknown'}: {error}")
                    
                    # Perform bulk create - optimized with batch commits
                    if bulk_create_records:
                        logger.info(f"📍 [createZomatoSummaryRecordsForRefundOnly] Bulk creating {len(bulk_create_records)} refund records...")
                        insert_query = text("""
                            INSERT INTO zomato_vs_pos_summary (
                                id, zomato_order_id, store_name, order_date,
                                zomato_net_amount, zomato_tax_paid_by_customer, zomato_commission_value,
                                zomato_pg_applied_on, zomato_pg_charge, zomato_taxes_zomato_fee,
                                zomato_tds_amount, zomato_final_amount,
                                calculated_zomato_net_amount, calculated_zomato_tax_paid_by_customer,
                                calculated_zomato_commission_value, calculated_zomato_pg_applied_on,
                                calculated_zomato_pg_charge, calculated_zomato_taxes_zomato_fee,
                                calculated_zomato_tds_amount, calculated_zomato_final_amount,
                                fixed_credit_note_amount, fixed_pro_discount_passthrough,
                                fixed_customer_discount, fixed_rejection_penalty_charge,
                                fixed_user_credits_charge, fixed_promo_recovery_adj,
                                fixed_icecream_handling, fixed_icecream_deductions,
                                fixed_order_support_cost, fixed_merchant_delivery_charge,
                                action, order_status_zomato, reconciled_status, created_at, updated_at
                            ) VALUES (
                                :id, :zomato_order_id, :store_name, :order_date,
                                :zomato_net_amount, :zomato_tax_paid_by_customer, :zomato_commission_value,
                                :zomato_pg_applied_on, :zomato_pg_charge, :zomato_taxes_zomato_fee,
                                :zomato_tds_amount, :zomato_final_amount,
                                :calculated_zomato_net_amount, :calculated_zomato_tax_paid_by_customer,
                                :calculated_zomato_commission_value, :calculated_zomato_pg_applied_on,
                                :calculated_zomato_pg_charge, :calculated_zomato_taxes_zomato_fee,
                                :calculated_zomato_tds_amount, :calculated_zomato_final_amount,
                                :fixed_credit_note_amount, :fixed_pro_discount_passthrough,
                                :fixed_customer_discount, :fixed_rejection_penalty_charge,
                                :fixed_user_credits_charge, :fixed_promo_recovery_adj,
                                :fixed_icecream_handling, :fixed_icecream_deductions,
                                :fixed_order_support_cost, :fixed_merchant_delivery_charge,
                                :action, :order_status_zomato, :reconciled_status, :created_at, :updated_at
                            )
                            ON DUPLICATE KEY UPDATE
                                zomato_order_id = VALUES(zomato_order_id),
                                store_name = VALUES(store_name),
                                order_date = VALUES(order_date),
                                zomato_net_amount = VALUES(zomato_net_amount),
                                zomato_tax_paid_by_customer = VALUES(zomato_tax_paid_by_customer),
                                zomato_commission_value = VALUES(zomato_commission_value),
                                zomato_pg_applied_on = VALUES(zomato_pg_applied_on),
                                zomato_pg_charge = VALUES(zomato_pg_charge),
                                zomato_taxes_zomato_fee = VALUES(zomato_taxes_zomato_fee),
                                zomato_tds_amount = VALUES(zomato_tds_amount),
                                zomato_final_amount = VALUES(zomato_final_amount),
                                calculated_zomato_net_amount = VALUES(calculated_zomato_net_amount),
                                calculated_zomato_tax_paid_by_customer = VALUES(calculated_zomato_tax_paid_by_customer),
                                calculated_zomato_commission_value = VALUES(calculated_zomato_commission_value),
                                calculated_zomato_pg_applied_on = VALUES(calculated_zomato_pg_applied_on),
                                calculated_zomato_pg_charge = VALUES(calculated_zomato_pg_charge),
                                calculated_zomato_taxes_zomato_fee = VALUES(calculated_zomato_taxes_zomato_fee),
                                calculated_zomato_tds_amount = VALUES(calculated_zomato_tds_amount),
                                calculated_zomato_final_amount = VALUES(calculated_zomato_final_amount),
                                fixed_credit_note_amount = VALUES(fixed_credit_note_amount),
                                fixed_pro_discount_passthrough = VALUES(fixed_pro_discount_passthrough),
                                fixed_customer_discount = VALUES(fixed_customer_discount),
                                fixed_rejection_penalty_charge = VALUES(fixed_rejection_penalty_charge),
                                fixed_user_credits_charge = VALUES(fixed_user_credits_charge),
                                fixed_promo_recovery_adj = VALUES(fixed_promo_recovery_adj),
                                fixed_icecream_handling = VALUES(fixed_icecream_handling),
                                fixed_icecream_deductions = VALUES(fixed_icecream_deductions),
                                fixed_order_support_cost = VALUES(fixed_order_support_cost),
                                fixed_merchant_delivery_charge = VALUES(fixed_merchant_delivery_charge),
                                action = VALUES(action),
                                order_status_zomato = VALUES(order_status_zomato),
                                updated_at = VALUES(updated_at)
                        """)
                        # Process in smaller sub-batches and commit once per batch
                        SUB_BATCH_SIZE = 100
                        for i in range(0, len(bulk_create_records), SUB_BATCH_SIZE):
                            sub_batch = bulk_create_records[i:i + SUB_BATCH_SIZE]
                            for record in sub_batch:
                                try:
                                    await db.execute(insert_query, record)
                                except Exception as e:
                                    logger.error(f"Error creating refund record {record.get('id')}: {e}")
                            await db.commit()
                        total_created += len(bulk_create_records)
                        logger.info(f"📍 [createZomatoSummaryRecordsForRefundOnly] Created {len(bulk_create_records)} refund records in batch {offset}")
                    
                    # Perform bulk update - optimized with batch commits
                    if bulk_update_records:
                        logger.info(f"📍 [createZomatoSummaryRecordsForRefundOnly] Bulk updating {len(bulk_update_records)} refund records...")
                        update_query = text("""
                            UPDATE zomato_vs_pos_summary
                            SET 
                                zomato_order_id = :zomato_order_id,
                                store_name = :store_name,
                                order_date = :order_date,
                                zomato_net_amount = :zomato_net_amount,
                                zomato_tax_paid_by_customer = :zomato_tax_paid_by_customer,
                                zomato_commission_value = :zomato_commission_value,
                                zomato_pg_applied_on = :zomato_pg_applied_on,
                                zomato_pg_charge = :zomato_pg_charge,
                                zomato_taxes_zomato_fee = :zomato_taxes_zomato_fee,
                                zomato_tds_amount = :zomato_tds_amount,
                                zomato_final_amount = :zomato_final_amount,
                                calculated_zomato_net_amount = :calculated_zomato_net_amount,
                                calculated_zomato_tax_paid_by_customer = :calculated_zomato_tax_paid_by_customer,
                                calculated_zomato_commission_value = :calculated_zomato_commission_value,
                                calculated_zomato_pg_applied_on = :calculated_zomato_pg_applied_on,
                                calculated_zomato_pg_charge = :calculated_zomato_pg_charge,
                                calculated_zomato_taxes_zomato_fee = :calculated_zomato_taxes_zomato_fee,
                                calculated_zomato_tds_amount = :calculated_zomato_tds_amount,
                                calculated_zomato_final_amount = :calculated_zomato_final_amount,
                                fixed_credit_note_amount = :fixed_credit_note_amount,
                                fixed_pro_discount_passthrough = :fixed_pro_discount_passthrough,
                                fixed_customer_discount = :fixed_customer_discount,
                                fixed_rejection_penalty_charge = :fixed_rejection_penalty_charge,
                                fixed_user_credits_charge = :fixed_user_credits_charge,
                                fixed_promo_recovery_adj = :fixed_promo_recovery_adj,
                                fixed_icecream_handling = :fixed_icecream_handling,
                                fixed_icecream_deductions = :fixed_icecream_deductions,
                                fixed_order_support_cost = :fixed_order_support_cost,
                                fixed_merchant_delivery_charge = :fixed_merchant_delivery_charge,
                                order_status_zomato = :order_status_zomato,
                                updated_at = :updated_at
                            WHERE id = :id
                        """)
                        # Process in smaller sub-batches and commit once per batch
                        SUB_BATCH_SIZE = 100
                        for i in range(0, len(bulk_update_records), SUB_BATCH_SIZE):
                            sub_batch = bulk_update_records[i:i + SUB_BATCH_SIZE]
                            for record in sub_batch:
                                try:
                                    await db.execute(update_query, record)
                                except Exception as e:
                                    logger.error(f"Error updating refund record {record.get('id')}: {e}")
                            await db.commit()
                        total_updated += len(bulk_update_records)
                        logger.info(f"📍 [createZomatoSummaryRecordsForRefundOnly] Updated {len(bulk_update_records)} refund records in batch {offset}")
                    
                    total_processed += len(zomato_orders)
                    offset += BATCH_SIZE
                    logger.info(f"📍 [createZomatoSummaryRecordsForRefundOnly] Zomato Refund Summary batch: {offset}/{total_count} orders ({round((offset/total_count)*100)}%)")
                
                logger.info(f"\n✅ [createZomatoSummaryRecordsForRefundOnly] Function completed successfully")
                logger.info(f"📍 [createZomatoSummaryRecordsForRefundOnly] Total processed: {total_processed} refund orders")
                logger.info(f"📍 [createZomatoSummaryRecordsForRefundOnly] Total created: {total_created} new records")
                logger.info(f"📍 [createZomatoSummaryRecordsForRefundOnly] Total updated: {total_updated} existing records")
                
                # Return counts for aggregation in main function
                return {
                    "processed": total_processed,
                    "updated": total_created + total_updated,
                    "errors": 0
                }
                
            except Exception as error:
                logger.error(f"❌ [createZomatoSummaryRecordsForRefundOnly] Error occurred: {error}")
                logger.error(f"   Error message: {error.message if hasattr(error, 'message') else str(error)}")
                logger.error(f"   Error stack: {error.__traceback__}")
                return {
                    "processed": 0,
                    "updated": 0,
                    "errors": 1
                }
        
        # Initialize counters for response matching Node.js format
        total_processed = 0
        total_updated = 0
        total_errors = 0
        
        # Step 1: Create POS Summary Records
        logger.info("\n🔵 STEP 1: Starting createPosSummaryRecords()")
        logger.info("─────────────────────────────────────────")
        pos_result = await create_pos_summary_records(db)
        if pos_result:
            total_processed += pos_result.get("processed", 0)
            total_updated += pos_result.get("updated", 0)
            total_errors += pos_result.get("errors", 0)
        logger.info("✅ STEP 1 COMPLETE: createPosSummaryRecords() finished\n")
        
        # Step 2: Create Zomato Summary Records
        logger.info("\n🔵 STEP 2: Starting createZomatoSummaryRecords()")
        logger.info("─────────────────────────────────────────")
        zomato_result = await create_zomato_summary_records(db)
        if zomato_result:
            total_processed += zomato_result.get("processed", 0)
            total_updated += zomato_result.get("updated", 0)
            total_errors += zomato_result.get("errors", 0)
        logger.info("✅ STEP 2 COMPLETE: createZomatoSummaryRecords() finished\n")
        
        # Step 3: Create Zomato Summary Records for Refund Only
        logger.info("\n🔵 STEP 3: Starting createZomatoSummaryRecordsForRefundOnly()")
        logger.info("─────────────────────────────────────────")
        zomato_refund_result = await create_zomato_summary_records_for_refund_only(db)
        if zomato_refund_result:
            total_processed += zomato_refund_result.get("processed", 0)
            total_updated += zomato_refund_result.get("updated", 0)
            total_errors += zomato_refund_result.get("errors", 0)
        logger.info("✅ STEP 3 COMPLETE: createZomatoSummaryRecordsForRefundOnly() finished\n")
        
        # Step 4: Calculate Delta Values
        logger.info("\n🔵 STEP 4: Starting calculateDeltaValues()")
        logger.info("─────────────────────────────────────────")
        
        # Get total count first
        logger.info("📍 [calculateDeltaValues] Getting total count of summary records...")
        count_query_delta = text("SELECT COUNT(*) as cnt FROM zomato_vs_pos_summary")
        result_delta = await db.execute(count_query_delta)
        row_delta = result_delta.fetchone()
        total_count_delta = row_delta.cnt if row_delta else 0
        logger.info(f"📍 [calculateDeltaValues] Total summary records found: {total_count_delta}")
        logger.info(f"📊 [SQL] calculateDeltaValues - Count Query:")
        logger.info(f"   {count_query_delta}")
        
        if total_count_delta > 0:
            BATCH_SIZE_DELTA = 1000
            offset_delta = 0
            total_processed_delta = 0
            total_updated_delta = 0
            errors_delta = []
            
            while offset_delta < total_count_delta:
                logger.info(f"📍 [calculateDeltaValues] Fetching batch: offset={offset_delta}, limit={BATCH_SIZE_DELTA}")
                
                batch_query_delta = text("""
                    SELECT id, pos_net_amount, zomato_net_amount,
                           pos_tax_paid_by_customer, zomato_tax_paid_by_customer,
                           pos_commission_value, zomato_commission_value,
                           pos_pg_charge, zomato_pg_charge,
                           pos_pg_applied_on, zomato_pg_applied_on,
                           pos_final_amount, zomato_final_amount
                    FROM zomato_vs_pos_summary
                    ORDER BY id ASC
                    LIMIT :limit OFFSET :offset
                """)
                
                logger.info(f"📊 [SQL] calculateDeltaValues - Batch Query (offset {offset_delta}):")
                logger.info(f"   {batch_query_delta}")
                
                result_batch = await db.execute(
                    batch_query_delta,
                    {"limit": BATCH_SIZE_DELTA, "offset": offset_delta}
                )
                summary_records = result_batch.fetchall()
                logger.info(f"📍 [calculateDeltaValues] Fetched {len(summary_records)} records in this batch")
                
                if not summary_records or len(summary_records) == 0:
                    break
                
                logger.info(f"📍 [calculateDeltaValues] Processing {len(summary_records)} records in batch {offset_delta}...")
                
                # Optimize: batch commits for better performance
                update_delta_query = text("""
                    UPDATE zomato_vs_pos_summary
                    SET 
                        pos_vs_zomato_net_amount_delta = :pvz_net,
                        zomato_vs_pos_net_amount_delta = :zvp_net,
                        pos_vs_zomato_tax_paid_by_customer_delta = :pvz_tax,
                        zomato_vs_pos_tax_paid_by_customer_delta = :zvp_tax,
                        pos_vs_zomato_commission_value_delta = :pvz_comm,
                        zomato_vs_pos_commission_value_delta = :zvp_comm,
                        pos_vs_zomato_pg_charge_delta = :pvz_pg,
                        zomato_vs_pos_pg_charge_delta = :zvp_pg,
                        updated_at = :updated_at
                    WHERE id = :id
                """)
                
                SUB_BATCH_SIZE = 100
                for idx, record in enumerate(summary_records):
                    try:
                        if (idx + 1) % 100 == 0:
                            logger.info(f"📍 [calculateDeltaValues] Processing record {idx + 1}/{len(summary_records)} in batch {offset_delta}")
                        
                        # Calculate deltas
                        pos_net = float(record.pos_net_amount or 0)
                        zomato_net = float(record.zomato_net_amount or 0)
                        
                        pos_tax = float(record.pos_tax_paid_by_customer or 0)
                        zomato_tax = float(record.zomato_tax_paid_by_customer or 0)
                        
                        pos_comm = float(record.pos_commission_value or 0)
                        zomato_comm = float(record.zomato_commission_value or 0)
                        
                        pos_pg = float(record.pos_pg_charge or 0)
                        zomato_pg = float(record.zomato_pg_charge or 0)
                        
                        await db.execute(update_delta_query, {
                            "pvz_net": Decimal(str(pos_net - zomato_net)),
                            "zvp_net": Decimal(str(zomato_net - pos_net)),
                            "pvz_tax": Decimal(str(pos_tax - zomato_tax)),
                            "zvp_tax": Decimal(str(zomato_tax - pos_tax)),
                            "pvz_comm": Decimal(str(pos_comm - zomato_comm)),
                            "zvp_comm": Decimal(str(zomato_comm - pos_comm)),
                            "pvz_pg": Decimal(str(pos_pg - zomato_pg)),
                            "zvp_pg": Decimal(str(zomato_pg - pos_pg)),
                            "updated_at": datetime.utcnow(),
                            "id": record.id
                        })
                        
                        total_processed_delta += 1
                        
                        # Commit every SUB_BATCH_SIZE records for better performance
                        if (idx + 1) % SUB_BATCH_SIZE == 0:
                            await db.commit()
                        
                    except Exception as error:
                        error_msg = str(error)
                        logger.error(f"Error processing record {record.id if record else 'unknown'}: {error_msg}")
                        errors_delta.append({
                            "record_id": record.id if record else "unknown",
                            "error": error_msg
                        })
                
                # Final commit for any remaining records
                await db.commit()
                total_updated_delta += len(summary_records)
                offset_delta += BATCH_SIZE_DELTA
                logger.info(f"📍 [calculateDeltaValues] Delta calculation batch: {offset_delta}/{total_count_delta} records ({round((offset_delta/total_count_delta)*100)}%)")
            
            logger.info(f"\n✅ [calculateDeltaValues] Function completed successfully")
            logger.info(f"📍 [calculateDeltaValues] Total processed: {total_processed_delta} records")
            logger.info(f"📍 [calculateDeltaValues] Successfully updated: {total_updated_delta} records")
            if errors_delta:
                logger.warning(f"⚠️ [calculateDeltaValues] Failed to update {len(errors_delta)} records")
            
            total_processed += total_processed_delta
            total_updated += total_updated_delta
            total_errors += len(errors_delta)
        else:
            logger.info("📍 [calculateDeltaValues] No records found for delta calculation")
        
        logger.info("✅ STEP 4 COMPLETE: calculateDeltaValues() finished\n")
        
        # Step 5: Calculate Zomato Receivables vs Receipts
        logger.info("\n🔵 STEP 5: Starting calculateZomatoReceivablesVsReceipts()")
        logger.info("─────────────────────────────────────────")
        receivables_result = await calculate_zomato_receivables_vs_receipts(db)
        if receivables_result:
            logger.info(f"✅ [calculateZomatoReceivablesVsReceipts] Processed {receivables_result.get('processed', 0)} receivables records")
        logger.info("✅ STEP 5 COMPLETE: calculateZomatoReceivablesVsReceipts() finished\n")
        
        # Final reconciliation status processing (similar to Node.js final batch processing)
        logger.info("\n📍 [checkReconciliationStatus] Getting total count of summary records for reconciliation...")
        count_query_final = text("SELECT COUNT(*) as cnt FROM zomato_vs_pos_summary")
        result_final = await db.execute(count_query_final)
        row_final = result_final.fetchone()
        total_count_final = row_final.cnt if row_final else 0
        logger.info(f"📍 [checkReconciliationStatus] Total summary records: {total_count_final}")
        logger.info(f"📊 [SQL] checkReconciliationStatus - Count Query:")
        logger.info(f"   {count_query_final}")
        
        if total_count_final == 0:
            logger.warning("⚠️ [checkReconciliationStatus] No summary records found")
            return {
                "success": False,
                "message": "No summary records found",
                "processed": 0,
                "updated": 0,
                "errors": 0
            }
        
        # Process final reconciliation status updates in batches (matching Node.js behavior)
        # This is the final step where reconciliation status is calculated
        logger.info("\n📍 [checkReconciliationStatus] Starting final reconciliation status processing...")
        BATCH_SIZE_FINAL = 1000
        offset_final = 0
        total_processed_final = 0
        total_updated_final = 0
        errors_final = []
        
        while offset_final < total_count_final:
            logger.info(f"📍 [checkReconciliationStatus] Fetching batch: offset={offset_final}, limit={BATCH_SIZE_FINAL}")
            
            batch_query_final = text("""
                SELECT 
                    id, 
                    pos_order_id, 
                    zomato_order_id,
                    pos_net_amount,
                    zomato_net_amount,
                    pos_final_amount,
                    zomato_final_amount,
                    reconciled_status
                FROM zomato_vs_pos_summary
                ORDER BY id ASC
                LIMIT :limit OFFSET :offset
            """)
            
            logger.info(f"📊 [SQL] checkReconciliationStatus - Batch Query (offset {offset_final}):")
            logger.info(f"   {batch_query_final}")
            
            result_batch_final = await db.execute(
                batch_query_final,
                {"limit": BATCH_SIZE_FINAL, "offset": offset_final}
            )
            final_records = result_batch_final.fetchall()
            logger.info(f"📍 [checkReconciliationStatus] Fetched {len(final_records)} records in this batch")
            
            if not final_records or len(final_records) == 0:
                break
            
            logger.info(f"📍 [checkReconciliationStatus] Processing {len(final_records)} records in batch {offset_final}...")
            
            # Prepare bulk update query
            update_status_query = text("""
                UPDATE zomato_vs_pos_summary
                SET 
                    reconciled_status = :reconciled_status,
                    reconciled_amount = :reconciled_amount,
                    unreconciled_amount = :unreconciled_amount,
                    updated_at = :updated_at
                WHERE id = :id
            """)
            
            bulk_status_updates = []
            
            for idx, record in enumerate(final_records):
                try:
                    if (idx + 1) % 100 == 0:
                        logger.info(f"📍 [checkReconciliationStatus] Processing record {idx + 1}/{len(final_records)} in batch {offset_final}")
                    
                    pos_order_id = record.pos_order_id
                    zomato_order_id = record.zomato_order_id
                    pos_net_amount = float(record.pos_net_amount or 0)
                    zomato_net_amount = float(record.zomato_net_amount or 0)
                    pos_final_amount = float(record.pos_final_amount or 0)
                    zomato_final_amount = float(record.zomato_final_amount or 0)
                    
                    # Calculate reconciliation status
                    reconciled_status = "PENDING"
                    reconciled_amount = None
                    unreconciled_amount = None
                    
                    # Check 1: If both order IDs exist, compare amounts
                    if pos_order_id and zomato_order_id:
                        # Use final_amount for comparison (more accurate)
                        # Allow small tolerance for floating point differences (0.01)
                        amount_diff = abs(pos_final_amount - zomato_final_amount)
                        if amount_diff <= 0.01:
                            # Amounts match - RECONCILED
                            reconciled_status = "RECONCILED"
                            reconciled_amount = pos_final_amount
                            unreconciled_amount = None
                        else:
                            # Amounts don't match - UNRECONCILED
                            reconciled_status = "UNRECONCILED"
                            reconciled_amount = None
                            unreconciled_amount = amount_diff
                    # Check 2: If only POS order ID exists
                    elif pos_order_id and not zomato_order_id:
                        reconciled_status = "UNRECONCILED"
                        reconciled_amount = None
                        unreconciled_amount = pos_final_amount if pos_final_amount > 0 else pos_net_amount
                    # Check 3: If only Zomato order ID exists
                    elif zomato_order_id and not pos_order_id:
                        reconciled_status = "UNRECONCILED"
                        reconciled_amount = None
                        unreconciled_amount = zomato_final_amount if zomato_final_amount > 0 else zomato_net_amount
                    # Check 4: Neither exists (shouldn't happen, but handle it)
                    else:
                        reconciled_status = "UNRECONCILED"
                        reconciled_amount = None
                        unreconciled_amount = 0
                    
                    bulk_status_updates.append({
                        "id": record.id,
                        "reconciled_status": reconciled_status,
                        "reconciled_amount": reconciled_amount,
                        "unreconciled_amount": unreconciled_amount,
                        "updated_at": datetime.utcnow()
                    })
                    
                    total_processed_final += 1
                    
                except Exception as error:
                    error_msg = str(error)
                    logger.error(f"Error processing record {record.id if record else 'unknown'}: {error_msg}")
                    errors_final.append({
                        "record_id": record.id if record else "unknown",
                        "error": error_msg
                    })
            
            # Bulk update reconciliation status
            if bulk_status_updates:
                SUB_BATCH_SIZE = 100
                for i in range(0, len(bulk_status_updates), SUB_BATCH_SIZE):
                    sub_batch = bulk_status_updates[i:i + SUB_BATCH_SIZE]
                    for update_record in sub_batch:
                        await db.execute(update_status_query, update_record)
                    await db.commit()
                total_updated_final += len(bulk_status_updates)
                logger.info(f"📍 [checkReconciliationStatus] Updated {len(bulk_status_updates)} reconciliation statuses in batch {offset_final}")
            
            offset_final += BATCH_SIZE_FINAL
            logger.info(f"📍 [checkReconciliationStatus] Final reconciliation batch: {offset_final}/{total_count_final} records ({round((offset_final/total_count_final)*100)}%)")
        
        total_processed += total_processed_final
        total_updated += total_updated_final
        total_errors += len(errors_final)
        
        logger.info("\n✅ [checkReconciliationStatus] Reconciliation processing complete!")
        logger.info(f"📍 [checkReconciliationStatus] Total processed: {total_processed} records")
        logger.info(f"📍 [checkReconciliationStatus] Successfully updated: {total_updated} records")
        if total_errors > 0:
            logger.warning(f"⚠️ [checkReconciliationStatus] Failed to update {total_errors} records")
        
        logger.info("===========================================")
        logger.info("🎉 API COMPLETE - Returning success response")
        logger.info("===========================================\n")
        
        return {
            "success": True,
            "message": "Reconciliation completed successfully",
            "processed": total_processed,
            "updated": total_updated,
            "errors": total_errors
        }
        
    except Exception as e:
        logger.error("\n❌ [checkReconciliationStatus] ERROR OCCURRED:")
        logger.error(f"   Error message: {str(e)}")
        logger.error(f"   Error type: {type(e).__name__}")
        import traceback
        logger.error(f"   Error stack: {traceback.format_exc()}")
        logger.error("===========================================\n")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error checking reconciliation status: {str(e)}"
        )


@router.post("/generate-excel")
async def generate_reconciliation_excel(
    request: Request,
    background_tasks: BackgroundTasks,
    current_user: UserDetails = Depends(get_current_user)
):
    """Generate reconciliation Excel - returns immediately with generationId (MongoDB-based)"""
    try:
        from app.models.main.excel_generation import ExcelGeneration, ExcelGenerationStatus
        
        # Get raw request body for debugging
        body_bytes = await request.body()
        body_str = body_bytes.decode('utf-8') if body_bytes else ''
        logger.info(f"[GENERATE_EXCEL] Raw request body: {body_str if body_str else 'EMPTY'}")
        
        # Parse JSON body
        body_json = {}
        if body_str:
            try:
                body_json = json.loads(body_str)
            except json.JSONDecodeError as e:
                logger.error(f"[GENERATE_EXCEL] JSON decode error: {e}")
                body_json = {}
        
        logger.info(f"[GENERATE_EXCEL] Parsed JSON: {body_json}")
        
        # Validate and parse request data
        if not body_json or body_json == {}:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Request body is empty. Please provide startDate, endDate, and stores in the request body."
            )
        
        # Extract and validate fields
        start_date = body_json.get("startDate") or body_json.get("start_date")
        end_date = body_json.get("endDate") or body_json.get("end_date")
        stores = body_json.get("stores")
        
        if not start_date or not end_date or not stores:
            missing_fields = []
            if not start_date:
                missing_fields.append("startDate")
            if not end_date:
                missing_fields.append("endDate")
            if not stores:
                missing_fields.append("stores")
            
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Missing required fields: {', '.join(missing_fields)}. Received body: {body_json}"
            )
        
        logger.info(f"[GENERATE_EXCEL] Extracted values: startDate={start_date}, endDate={end_date}, stores count={len(stores) if stores else 0}")
        
        # Parse dates - handle multiple formats
        date_formats = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y"]
        start_date_dt = None
        end_date_dt = None
        
        for fmt in date_formats:
            try:
                start_date_dt = datetime.strptime(start_date, fmt)
                break
            except ValueError:
                continue
        
        if start_date_dt is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid startDate format: {start_date}. Expected formats: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS"
            )
        
        for fmt in date_formats:
            try:
                end_date_dt = datetime.strptime(end_date, fmt)
                break
            except ValueError:
                continue
        
        if end_date_dt is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid endDate format: {end_date}. Expected formats: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS"
            )
        
        # Ensure stores is a list
        if not isinstance(stores, list):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"stores must be a list. Received: {type(stores).__name__}"
            )
        
        # Create reports directory if it doesn't exist
        reports_dir = "reports"
        os.makedirs(reports_dir, exist_ok=True)
        
        # Create initial record in MongoDB
        store_code_label = f"SummaryReport_{len(stores)} store(s)"
        generation_record = await ExcelGeneration.create(
            None,  # db parameter not needed for MongoDB
            store_code=store_code_label,
            start_date=start_date_dt,
            end_date=end_date_dt,
            status=ExcelGenerationStatus.PENDING,
            progress=0,
            message="Initializing Excel generation..."
        )
        
        # Add background task
        background_tasks.add_task(
            process_excel_generation,
            generation_record.id,  # This will be a string (ObjectId) now
            {
                "start_date": start_date,
                "end_date": end_date,
                "store_codes": stores,
                "reports_dir": reports_dir
            }
        )
        
        # Return immediately with generation ID
        return {
            "success": True,
            "message": "Excel generation started",
            "generationId": generation_record.id,
            "status": "PENDING"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Excel generation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server error starting Excel generation"
        )


# Import the background worker functions at the end to avoid circular imports
from app.workers.tasks import process_excel_generation, process_receivable_receipt_excel_generation, process_summary_sheet_generation


@router.post("/generate-receivable-receipt-excel")
async def generate_receivable_receipt_excel(
    request_data: GenerateExcelRequest,
    background_tasks: BackgroundTasks,
    current_user: UserDetails = Depends(get_current_user)
):
    """Generate receivable receipt Excel - returns immediately with generationId (MongoDB-based)"""
    try:
        from app.models.main.excel_generation import ExcelGeneration, ExcelGenerationStatus
        
        # Validate required parameters
        if not request_data.start_date or not request_data.end_date or not request_data.stores:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required parameters: startDate, endDate, or stores"
            )
        
        # Parse dates - handle multiple formats
        date_formats = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y"]
        start_date_dt = None
        end_date_dt = None
        
        for fmt in date_formats:
            try:
                start_date_dt = datetime.strptime(request_data.start_date, fmt)
                break
            except ValueError:
                continue
        
        if start_date_dt is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid startDate format: {request_data.start_date}"
            )
        
        for fmt in date_formats:
            try:
                end_date_dt = datetime.strptime(request_data.end_date, fmt)
                break
            except ValueError:
                continue
        
        if end_date_dt is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid endDate format: {request_data.end_date}"
            )
        
        # Create reports directory if it doesn't exist
        reports_dir = "reports"
        os.makedirs(reports_dir, exist_ok=True)
        
        # Create initial record in MongoDB
        store_code_label = f"ReceivableVsReceipt_{len(request_data.stores)} store(s)"
        generation_record = await ExcelGeneration.create(
            None,  # db parameter not needed for MongoDB
            store_code=store_code_label,
            start_date=start_date_dt,
            end_date=end_date_dt,
            status=ExcelGenerationStatus.PENDING,
            progress=0,
            message="Initializing Excel generation..."
        )
        
        # Add background task
        background_tasks.add_task(
            process_receivable_receipt_excel_generation,
            generation_record.id,  # This will be a string (ObjectId) now
            {
                "start_date": request_data.start_date,
                "end_date": request_data.end_date,
                "store_codes": request_data.stores,
                "reports_dir": reports_dir
            }
        )
        
        # Return immediately with generation ID
        return {
            "success": True,
            "message": "Excel generation started",
            "generationId": generation_record.id,
            "status": "pending"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Generate receivable receipt Excel error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server error starting Excel generation"
        )


@router.post("/generation-status")
async def check_generation_status(
    request_data: GenerationStatusRequest,
    current_user: UserDetails = Depends(get_current_user)
):
    """Check Excel generation status - supports filtering, pagination, and stale job cleanup (MongoDB-based)"""
    try:
        from app.models.main.excel_generation import ExcelGeneration, ExcelGenerationStatus
        
        # Handle stale pending jobs if enabled
        if request_data.exclude_stale_pending and request_data.stale_threshold_minutes > 0:
            await ExcelGeneration.mark_stale_pending_as_failed(
                None,  # db parameter not needed for MongoDB
                request_data.stale_threshold_minutes
            )
        
        # Check if a specific generation_id was requested
        generation_id = request_data.get_generation_id()
        
        if generation_id:
            # Convert to string if it's an integer (for backward compatibility)
            if isinstance(generation_id, int):
                generation_id = str(generation_id)
            
            # Return specific generation by ID
            generation = await ExcelGeneration.get_by_id(None, generation_id)  # db parameter not needed
            
            if not generation:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Generation ID {generation_id} not found"
                )
            
            result = generation.to_dict()
            
            # Add download URL if completed (case-insensitive status check)
            download_url = None
            status_upper = str(generation.status).upper()
            if status_upper == ExcelGenerationStatus.COMPLETED.value and generation.filename:
                # Use filename in URL like Node.js does
                download_url = f"/api/reconciliation/download/{generation.filename}"
            
            return {
                "success": True,
                "data": {
                    **result,
                    "downloadUrl": download_url  # Use camelCase to match Node.js
                }
            }
        else:
            # Return all generations with optional filtering
            # Parse date filters
            start_date_dt = None
            end_date_dt = None
            
            if request_data.start_date:
                date_formats = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d %H:%M:%S"]
                for fmt in date_formats:
                    try:
                        start_date_dt = datetime.strptime(request_data.start_date, fmt)
                        break
                    except ValueError:
                        continue
                if start_date_dt is None:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid start_date format: {request_data.start_date}"
                    )
            
            if request_data.end_date:
                date_formats = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d %H:%M:%S"]
                for fmt in date_formats:
                    try:
                        end_date_dt = datetime.strptime(request_data.end_date, fmt)
                        break
                    except ValueError:
                        continue
                if end_date_dt is None:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid end_date format: {request_data.end_date}"
                    )
            
            # Get filtered and paginated results
            generations = await ExcelGeneration.get_all(
                None,  # db parameter not needed for MongoDB
                limit=request_data.limit,
                offset=request_data.offset,
                status=request_data.status,
                store_code_pattern=request_data.store_code_pattern,
                start_date=start_date_dt,
                end_date=end_date_dt
            )
            
            # Format results
            formatted_generations = []
            for generation in generations:
                result = generation.to_dict()
                
                # Add download URL if completed (case-insensitive status check)
                download_url = None
                status_upper = str(generation.status).upper()
                if status_upper == ExcelGenerationStatus.COMPLETED.value and generation.filename:
                    # Use filename in URL like Node.js does
                    download_url = f"/api/reconciliation/download/{generation.filename}"
                
                formatted_generations.append({
                    **result,
                    "downloadUrl": download_url  # Use camelCase to match Node.js
                })
            
            # Match Node.js response format - no pagination object
            return {
                "success": True,
                "data": formatted_generations
            }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Check generation status error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error checking generation status: {str(e)}"
        )


@router.post("/threePODashboardData")
async def get_three_po_dashboard_data(
    request_data: ThreePODashboardDataRequest,
    db: AsyncSession = Depends(get_main_db),  # Using main_db for reconciliation tables
    current_user: UserDetails = Depends(get_current_user)
):
    """Get 3PO dashboard data from MongoDB report collections"""
    try:
        from datetime import datetime
        from app.services.mongodb_service import mongodb_service
        from app.config.mongodb import get_mongodb_collection
        
        logger.info("===========================================")
        logger.info("🚀 /threePODashboardData API IS HIT")
        logger.info("===========================================")
        
        startDate = request_data.startDate
        endDate = request_data.endDate
        stores = request_data.stores
        
        if not startDate or not endDate or not stores:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required parameters: startDate, endDate, or stores"
            )
        
        # Check MongoDB connection
        if not mongodb_service.is_connected():
            logger.error("❌ MongoDB not connected")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="MongoDB is not connected"
            )
        
        # Convert date strings to datetime objects
        start_datetime = datetime.strptime(startDate, "%Y-%m-%d %H:%M:%S")
        end_datetime = datetime.strptime(endDate, "%Y-%m-%d %H:%M:%S")
        
        # Step 1: Get all report names from formulas collection
        logger.info("📊 Step 1: Getting all report names from formulas collection")
        formulas_collection = get_mongodb_collection("formulas")
        
        if not mongodb_service.collection_exists("formulas"):
            logger.warning("⚠️ Formulas collection does not exist")
            report_names = []
        else:
            # Get all report names
            formulas_docs = list(formulas_collection.find({}, {"report_name": 1}))
            report_names = [doc.get("report_name") for doc in formulas_docs if doc.get("report_name")]
            logger.info(f"📋 Found {len(report_names)} report name(s): {report_names}")
        
        # Step 2: Aggregate data from all report collections
        logger.info("📊 Step 2: Aggregating data from report collections")
        total_pos_payment = 0.0
        total_net_amount = 0.0
        
        for report_name in report_names:
            if not report_name:
                continue
            
            report_name_lower = report_name.lower().strip()
            
            # Check if collection exists
            if not mongodb_service.collection_exists(report_name_lower):
                logger.warning(f"⚠️ Collection '{report_name_lower}' does not exist, skipping")
                continue
            
            try:
                # Get the report collection
                report_collection = mongodb_service.db[report_name_lower]
                
                # MongoDB aggregation pipeline
                # Filter by order_date field and sum pos_payment and net_amount
                pipeline = [
                    {
                        "$match": {
                            "order_date": {
                                "$gte": start_datetime,
                                "$lte": end_datetime
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "pos_payment_sum": {
                                "$sum": {
                                    "$ifNull": ["$pos_payment", 0]  # Treat missing/null as 0
                                }
                            },
                            "net_amount_sum": {
                                "$sum": {
                                    "$ifNull": ["$net_amount", 0]  # Treat missing/null as 0
                                }
                            }
                        }
                    }
                ]
                
                logger.info(f"📊 Aggregating data from collection '{report_name_lower}'")
                logger.info(f"   Date range: {start_datetime} to {end_datetime}")
                
                # Execute aggregation
                result = list(report_collection.aggregate(pipeline))
                
                if result and len(result) > 0:
                    report_pos_payment = float(result[0].get("pos_payment_sum", 0) or 0)
                    report_net_amount = float(result[0].get("net_amount_sum", 0) or 0)
                    
                    total_pos_payment += report_pos_payment
                    total_net_amount += report_net_amount
                    
                    logger.info(f"   ✅ Collection '{report_name_lower}': pos_payment={report_pos_payment}, net_amount={report_net_amount}")
                else:
                    # Empty collection or no matching documents
                    logger.info(f"   ℹ️ Collection '{report_name_lower}': No data found (returning 0)")
                    
            except Exception as e:
                logger.error(f"❌ Error processing collection '{report_name_lower}': {e}")
                # Continue with next collection
                continue
        
        logger.info(f"📊 Total aggregated: pos_payment={total_pos_payment}, net_amount={total_net_amount}")
        
        # Step 3: Get tender-wise data from devyani_posvszom collection
        logger.info("📊 Step 3: Getting tender-wise data from devyani_posvszom collection")
        tender_wise_data = []
        
        if mongodb_service.collection_exists("devyani_posvszom"):
            try:
                posvszom_collection = get_mongodb_collection("devyani_posvszom")
                
                # Aggregation pipeline to group by tender_name and sum all fields
                pipeline = [
                    {
                        "$match": {
                            "order_date": {
                                "$gte": start_datetime,
                                "$lte": end_datetime
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": "$tender_name",  # Group by tender_name (ZOMATO, SWIGGY, etc.)
                            
                            # POS Fields
                            "posSales": {
                                "$sum": {"$ifNull": ["$pos_payment", 0]}
                            },
                            "posReceivables": {
                                "$sum": {"$ifNull": ["$pos_final_amount", 0]}  # Using pos_final_amount for POS receivables
                            },
                            "posCommission": {
                                "$sum": {"$ifNull": ["$pos_commission", 0]}
                            },
                            "posCharges": {
                                "$sum": {"$ifNull": ["$pos_charges", 0]}
                            },
                            "posDiscounts": {
                                "$sum": {"$ifNull": ["$pos_discounts", 0]}
                            },
                            "posFreebies": {
                                "$sum": {"$ifNull": ["$pos_freebies", 0]}
                            },
                            
                            # 3PO/Aggregator Fields
                            "threePOSales": {
                                "$sum": {"$ifNull": ["$net_amount", 0]}
                            },
                            "threePOReceivables": {
                                "$sum": {"$ifNull": ["$final_amount", 0]}  # Using final_amount for 3PO receivables
                            },
                            "threePOCommission": {
                                "$sum": {"$ifNull": ["$three_po_commission", 0]}
                            },
                            "threePOCharges": {
                                "$sum": {"$ifNull": ["$three_po_charges", 0]}
                            },
                            "threePODiscounts": {
                                "$sum": {"$ifNull": ["$three_po_discounts", 0]}
                            },
                            "threePOFreebies": {
                                "$sum": {"$ifNull": ["$three_po_freebies", 0]}
                            },
                            
                            # Comparison Fields
                            "posVsThreePO": {
                                "$sum": {"$ifNull": ["$pos_vs_three_po", 0]}
                            },
                            "receivablesVsReceipts": {
                                "$sum": {"$ifNull": ["$receivables_vs_receipts", 0]}
                            },
                            
                            # Other Fields
                            "reconciled": {
                                "$sum": {"$ifNull": ["$reconciled", 0]}
                            },
                            "promo": {
                                "$sum": {"$ifNull": ["$promo", 0]}
                            },
                            "totalReceivables": {
                                "$sum": {"$ifNull": ["$total_receivables", 0]}
                            },
                            "totalReceipts": {
                                "$sum": {"$ifNull": ["$total_receipts", 0]}
                            },
                            "booked": {
                                "$sum": {"$ifNull": ["$booked", 0]}
                            },
                            "deltaPromo": {
                                "$sum": {"$ifNull": ["$delta_promo", 0]}
                            }
                        }
                    },
                    {
                        "$project": {
                            "tenderName": "$_id",
                            "posSales": 1,
                            "posReceivables": 1,
                            "posCommission": 1,
                            "posCharges": 1,
                            "posDiscounts": 1,
                            "posFreebies": 1,
                            "threePOSales": 1,
                            "threePOReceivables": 1,
                            "threePOCommission": 1,
                            "threePOCharges": 1,
                            "threePODiscounts": 1,
                            "threePOFreebies": 1,
                            "posVsThreePO": 1,
                            "receivablesVsReceipts": 1,
                            "reconciled": 1,
                            "promo": 1,
                            "totalReceivables": 1,
                            "totalReceipts": 1,
                            "booked": 1,
                            "deltaPromo": 1,
                            # Calculate all charges
                            "allThreePOCharges": {
                                "$add": [
                                    "$threePOCharges",
                                    "$promo",
                                    "$threePODiscounts",
                                    "$threePOFreebies",
                                    "$threePOCommission"
                                ]
                            },
                            "allPOSCharges": {
                                "$add": [
                                    "$posCharges",
                                    "$promo",
                                    "$posDiscounts",
                                    "$posFreebies",
                                    "$posCommission"
                                ]
                            }
                        }
                    }
                ]
                
                logger.info(f"📊 Executing aggregation on devyani_posvszom collection")
                logger.info(f"   Date range: {start_datetime} to {end_datetime}")
                
                # Execute aggregation
                tender_wise_results = list(posvszom_collection.aggregate(pipeline))
                
                logger.info(f"   ✅ Found {len(tender_wise_results)} tender(s) in devyani_posvszom")
                
                # Transform results to match frontend structure
                for item in tender_wise_results:
                    # Default to "ZOMATO" if tenderName is null or empty
                    tender_name = item.get("tenderName") or item.get("_id") or "ZOMATO"
                    if not tender_name or tender_name == "null":
                        tender_name = "ZOMATO"
                    
                    tender_item = {
                        "tenderName": tender_name,
                        "posSales": float(item.get("posSales", 0) or 0),
                        "posReceivables": float(item.get("posReceivables", 0) or 0),
                        "posCommission": float(item.get("posCommission", 0) or 0),
                        "posCharges": float(item.get("posCharges", 0) or 0),
                        "posDiscounts": int(item.get("posDiscounts", 0) or 0),
                        "threePOSales": float(item.get("threePOSales", 0) or 0),
                        "threePOReceivables": float(item.get("threePOReceivables", 0) or 0),
                        "threePOCommission": float(item.get("threePOCommission", 0) or 0),
                        "threePOCharges": float(item.get("threePOCharges", 0) or 0),
                        "threePODiscounts": int(item.get("threePODiscounts", 0) or 0),
                        "reconciled": int(item.get("reconciled", 0) or 0),
                        "receivablesVsReceipts": float(item.get("receivablesVsReceipts", 0) or 0),
                        "posFreebies": int(item.get("posFreebies", 0) or 0),
                        "threePOFreebies": int(item.get("threePOFreebies", 0) or 0),
                        "posVsThreePO": float(item.get("posVsThreePO", 0) or 0),
                        "booked": int(item.get("booked", 0) or 0),
                        "promo": int(item.get("promo", 0) or 0),
                        "deltaPromo": int(item.get("deltaPromo", 0) or 0),
                        "allThreePOCharges": float(item.get("allThreePOCharges", 0) or 0),
                        "allPOSCharges": float(item.get("allPOSCharges", 0) or 0),
                        "totalReceivables": float(item.get("totalReceivables", 0) or 0),
                        "totalReceipts": int(item.get("totalReceipts", 0) or 0)
                    }
                    tender_wise_data.append(tender_item)
                    logger.info(f"   📋 Tender: {tender_item['tenderName']}, POS Sales: {tender_item['posSales']}, 3PO Sales: {tender_item['threePOSales']}")
                
            except Exception as e:
                logger.error(f"❌ Error querying devyani_posvszom collection: {e}", exc_info=True)
                # Continue with empty array if collection query fails
                tender_wise_data = []
        else:
            logger.warning("⚠️ Collection 'devyani_posvszom' does not exist, using empty arrays")
        
        # Helper to convert to int if zero, otherwise keep as float
        def format_number(value):
            if value is None or value == 0:
                return 0
            val = float(value)
            return int(val) if val == 0 else val
        
        # Calculate top-level totals from tender-wise data
        total_pos_sales_from_tenders = sum(item.get("posSales", 0) for item in tender_wise_data)
        total_three_po_sales_from_tenders = sum(item.get("threePOSales", 0) for item in tender_wise_data)
        
        # Use tender-wise totals if available, otherwise fall back to report collection totals
        final_pos_sales = total_pos_sales_from_tenders if tender_wise_data else total_pos_payment
        final_three_po_sales = total_three_po_sales_from_tenders if tender_wise_data else total_net_amount
        
        # Prepare final response
        # Map aggregated MongoDB data to response structure
        response = {
            "posSales": final_pos_sales,
            "posReceivables": sum(item.get("posReceivables", 0) for item in tender_wise_data),
            "posCommission": sum(item.get("posCommission", 0) for item in tender_wise_data),
            "posCharges": sum(item.get("posCharges", 0) for item in tender_wise_data),
            "posDiscounts": sum(item.get("posDiscounts", 0) for item in tender_wise_data),
            "threePOSales": final_three_po_sales,
            "threePOReceivables": sum(item.get("threePOReceivables", 0) for item in tender_wise_data),
            "threePOCommission": sum(item.get("threePOCommission", 0) for item in tender_wise_data),
            "threePOCharges": sum(item.get("threePOCharges", 0) for item in tender_wise_data),
            "threePODiscounts": sum(item.get("threePODiscounts", 0) for item in tender_wise_data),
            "reconciled": sum(item.get("reconciled", 0) for item in tender_wise_data),
            "receivablesVsReceipts": sum(item.get("receivablesVsReceipts", 0) for item in tender_wise_data),
            "posFreebies": sum(item.get("posFreebies", 0) for item in tender_wise_data),
            "threePOFreebies": sum(item.get("threePOFreebies", 0) for item in tender_wise_data),
            "posVsThreePO": sum(item.get("posVsThreePO", 0) for item in tender_wise_data),
            "booked": sum(item.get("booked", 0) for item in tender_wise_data),
            "promo": sum(item.get("promo", 0) for item in tender_wise_data),
            "deltaPromo": sum(item.get("deltaPromo", 0) for item in tender_wise_data),
            "allThreePOCharges": sum(item.get("allThreePOCharges", 0) for item in tender_wise_data),
            "allPOSCharges": sum(item.get("allPOSCharges", 0) for item in tender_wise_data),
            "threePOData": tender_wise_data,  # ✅ NOW POPULATED
            "tenderWisePOSData": tender_wise_data,  # ✅ NOW POPULATED (same data, frontend uses based on salesType)
            "instoreTotal": final_pos_sales
        }
        
        logger.info("✅ API Request Completed Successfully")
        
        return {
            "success": True,
            "data": response
        }
        
    except Exception as e:
        logger.error(f"Get 3PO dashboard data error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching 3PO dashboard data: {str(e)}"
        )


@router.post("/threePODashboardDataNew")
async def get_three_po_dashboard_data_new(
    request_data: ThreePODashboardDataRequest,
    db: AsyncSession = Depends(get_main_db),  # Using main_db for reconciliation tables
    current_user: UserDetails = Depends(get_current_user)
):
    """Get 3PO dashboard data from MongoDB report collections - New version"""
    try:
        from datetime import datetime
        from app.services.mongodb_service import mongodb_service
        from app.config.mongodb import get_mongodb_collection
        
        logger.info("===========================================")
        logger.info("🚀 /threePODashboardDataNew API IS HIT")
        logger.info("===========================================")
        
        startDate = request_data.startDate
        endDate = request_data.endDate
        stores = request_data.stores
        
        if not startDate or not endDate or not stores:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required parameters: startDate, endDate, or stores"
            )
        
        # Check MongoDB connection
        if not mongodb_service.is_connected():
            logger.error("❌ MongoDB not connected")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="MongoDB is not connected"
            )
        
        # Convert date strings to datetime objects
        start_datetime = datetime.strptime(startDate, "%Y-%m-%d %H:%M:%S")
        end_datetime = datetime.strptime(endDate, "%Y-%m-%d %H:%M:%S")
        
        # Step 1: Get mapping configurations from dashboard_api_mapping_keys collection
        logger.info("📊 Step 1: Getting mapping configurations from dashboard_api_mapping_keys collection")
        mapping_keys_collection = get_mongodb_collection("dashboard_api_mapping_keys")
        
        if not mongodb_service.collection_exists("dashboard_api_mapping_keys"):
            logger.warning("⚠️ dashboard_api_mapping_keys collection does not exist")
            mapping_documents = []
        else:
            # Get all documents where is_3PO is true
            mapping_documents = list(mapping_keys_collection.find({"is_3PO": True}))
            logger.info(f"📋 Found {len(mapping_documents)} mapping document(s) with is_3PO=true")
        
        # Initialize response structure with default values
        response_fields = {
            "posSales": 0.0,
            "posReceivables": 0.0,
            "posCommission": 0.0,
            "posCharges": 0.0,
            "posDiscounts": 0,
            "posFreebies": 0,
            "threePOSales": 0.0,
            "threePOReceivables": 0.0,
            "threePOCommission": 0.0,
            "threePOCharges": 0.0,
            "threePODiscounts": 0,
            "threePOFreebies": 0,
            "reconciled": 0,
            "receivablesVsReceipts": 0.0,
            "posVsThreePO": 0.0,
            "booked": 0,
            "promo": 0,
            "deltaPromo": 0,
            "allThreePOCharges": 0.0,
            "allPOSCharges": 0.0,
            "totalReceivables": 0.0,
            "totalReceipts": 0
        }
        
        # Dictionary to store tender-wise data (grouped by tender_name)
        tender_wise_data_dict = {}
        
        # Step 2: Process each mapping document
        logger.info("📊 Step 2: Processing mapping documents and aggregating data")
        for mapping_doc in mapping_documents:
            collection_name = mapping_doc.get("name")
            mapping_keys = mapping_doc.get("mapping_keys", [])
            
            if not collection_name:
                logger.warning(f"⚠️ Mapping document missing 'name' field, skipping")
                continue
            
            collection_name_lower = collection_name.lower().strip()
            
            # Check if collection exists
            if not mongodb_service.collection_exists(collection_name_lower):
                logger.warning(f"⚠️ Collection '{collection_name_lower}' does not exist, skipping")
                continue
            
            if not mapping_keys or len(mapping_keys) == 0:
                logger.warning(f"⚠️ No mapping_keys found for collection '{collection_name}', skipping")
                continue
            
            logger.info(f"📊 Processing collection '{collection_name}' with {len(mapping_keys)} mapping(s)")
            
            try:
                # Get the report collection
                report_collection = mongodb_service.db[collection_name_lower]
                
                # Build dynamic aggregation pipeline based on mapping_keys
                # First, build the $group stage dynamically
                group_stage = {"_id": "$tender_name"}  # Group by tender_name for tender-wise data
                
                # Also create a separate aggregation for totals (no grouping)
                total_group_stage = {"_id": None}
                
                # Track which fields need to be aggregated
                fields_to_aggregate = {}
                
                for mapping in mapping_keys:
                    if not isinstance(mapping, dict):
                        continue
                    
                    three_po_key = mapping.get("3po_key")
                    collection_key = mapping.get("collection_key")
                    
                    if not three_po_key or not collection_key:
                        logger.warning(f"⚠️ Invalid mapping: {mapping}, skipping")
                        continue
                    
                    # Store mapping for later use
                    fields_to_aggregate[three_po_key] = collection_key
                    
                    # Build field reference for MongoDB (need to construct the $field reference properly)
                    # In MongoDB aggregation, field references are strings like "$field_name"
                    field_ref = f"${collection_key}"
                    
                    # Add to group stage for tender-wise aggregation
                    group_stage[three_po_key] = {
                        "$sum": {"$ifNull": [field_ref, 0]}
                    }
                    
                    # Add to total group stage for top-level totals
                    total_group_stage[three_po_key] = {
                        "$sum": {"$ifNull": [field_ref, 0]}
                    }
                
                if not fields_to_aggregate:
                    logger.warning(f"⚠️ No valid mappings found for collection '{collection_name}', skipping")
                    continue
                
                # Build aggregation pipeline for tender-wise data
                tender_pipeline = [
                    {
                        "$match": {
                            "order_date": {
                                "$gte": start_datetime,
                                "$lte": end_datetime
                            }
                        }
                    },
                    {
                        "$group": group_stage
                    },
                    {
                        "$project": {
                            "tenderName": "$_id",
                            **{key: 1 for key in fields_to_aggregate.keys()}
                        }
                    }
                ]
                
                # Build aggregation pipeline for totals
                total_pipeline = [
                    {
                        "$match": {
                            "order_date": {
                                "$gte": start_datetime,
                                "$lte": end_datetime
                            }
                        }
                    },
                    {
                        "$group": total_group_stage
                    }
                ]
                
                logger.info(f"   📊 Executing aggregation on '{collection_name_lower}'")
                logger.info(f"   Date range: {start_datetime} to {end_datetime}")
                logger.info(f"   Fields to aggregate: {list(fields_to_aggregate.keys())}")
                
                # Execute tender-wise aggregation
                tender_results = list(report_collection.aggregate(tender_pipeline))
                logger.info(f"   ✅ Found {len(tender_results)} tender(s) in '{collection_name_lower}'")
                
                # Process tender-wise results
                for tender_item in tender_results:
                    tender_name = tender_item.get("tenderName") or tender_item.get("_id") or "ZOMATO"
                    if not tender_name or tender_name == "null":
                        tender_name = "ZOMATO"
                    
                    # Initialize tender data if not exists
                    if tender_name not in tender_wise_data_dict:
                        tender_wise_data_dict[tender_name] = {key: 0 for key in response_fields.keys()}
                        tender_wise_data_dict[tender_name]["tenderName"] = tender_name
                    
                    # Update tender data with aggregated values
                    for three_po_key, value in tender_item.items():
                        if three_po_key != "tenderName" and three_po_key != "_id":
                            if three_po_key in tender_wise_data_dict[tender_name]:
                                # Convert to appropriate type
                                if three_po_key in ["posDiscounts", "posFreebies", "threePODiscounts", "threePOFreebies", 
                                                   "reconciled", "booked", "promo", "deltaPromo", "totalReceipts"]:
                                    tender_wise_data_dict[tender_name][three_po_key] += int(value or 0)
                                else:
                                    tender_wise_data_dict[tender_name][three_po_key] += float(value or 0)
                
                # Execute total aggregation
                total_results = list(report_collection.aggregate(total_pipeline))
                if total_results and len(total_results) > 0:
                    total_data = total_results[0]
                    logger.info(f"   ✅ Total aggregation completed for '{collection_name_lower}'")
                    
                    # Update top-level response fields
                    for three_po_key, value in total_data.items():
                        if three_po_key != "_id" and three_po_key in response_fields:
                            # Convert to appropriate type
                            if three_po_key in ["posDiscounts", "posFreebies", "threePODiscounts", "threePOFreebies", 
                                               "reconciled", "booked", "promo", "deltaPromo", "totalReceipts"]:
                                response_fields[three_po_key] += int(value or 0)
                            else:
                                response_fields[three_po_key] += float(value or 0)
                
            except Exception as e:
                logger.error(f"❌ Error processing collection '{collection_name_lower}': {e}", exc_info=True)
                continue
        
        # Step 3: Convert tender_wise_data_dict to list and calculate computed fields
        logger.info("📊 Step 3: Processing tender-wise data and calculating computed fields")
        tender_wise_data = []
        
        for tender_name, tender_data in tender_wise_data_dict.items():
            # Calculate computed fields
            # Check if allThreePOCharges was already aggregated from MongoDB (via mapping)
            # If the key exists in tender_data (meaning it was aggregated), use it; otherwise calculate it
            if "allThreePOCharges" in tender_data:
                all_three_po_charges = float(tender_data.get("allThreePOCharges", 0) or 0)
                logger.info(f"   ✅ Using allThreePOCharges from MongoDB for {tender_name}: {all_three_po_charges}")
            else:
                # Fallback: calculate from component fields
                all_three_po_charges = (
                    tender_data.get("threePOCharges", 0) +
                    tender_data.get("promo", 0) +
                    tender_data.get("threePODiscounts", 0) +
                    tender_data.get("threePOFreebies", 0) +
                    tender_data.get("threePOCommission", 0)
                )
                logger.info(f"   📊 Calculated allThreePOCharges for {tender_name}: {all_three_po_charges}")
            
            # Check if allPOSCharges was already aggregated from MongoDB (via mapping)
            if "allPOSCharges" in tender_data:
                all_pos_charges = float(tender_data.get("allPOSCharges", 0) or 0)
                logger.info(f"   ✅ Using allPOSCharges from MongoDB for {tender_name}: {all_pos_charges}")
            else:
                # Fallback: calculate from component fields
                all_pos_charges = (
                    tender_data.get("posCharges", 0) +
                    tender_data.get("promo", 0) +
                    tender_data.get("posDiscounts", 0) +
                    tender_data.get("posFreebies", 0) +
                    tender_data.get("posCommission", 0)
                )
                logger.info(f"   📊 Calculated allPOSCharges for {tender_name}: {all_pos_charges}")
            
            tender_item = {
                "tenderName": tender_name,
                "posSales": float(tender_data.get("posSales", 0) or 0),
                "posReceivables": float(tender_data.get("posReceivables", 0) or 0),
                "posCommission": float(tender_data.get("posCommission", 0) or 0),
                "posCharges": float(tender_data.get("posCharges", 0) or 0),
                "posDiscounts": int(tender_data.get("posDiscounts", 0) or 0),
                "posFreebies": int(tender_data.get("posFreebies", 0) or 0),
                "threePOSales": float(tender_data.get("threePOSales", 0) or 0),
                "threePOReceivables": float(tender_data.get("threePOReceivables", 0) or 0),
                "threePOCommission": float(tender_data.get("threePOCommission", 0) or 0),
                "threePOCharges": float(tender_data.get("threePOCharges", 0) or 0),
                "threePODiscounts": int(tender_data.get("threePODiscounts", 0) or 0),
                "threePOFreebies": int(tender_data.get("threePOFreebies", 0) or 0),
                "reconciled": int(tender_data.get("reconciled", 0) or 0),
                "receivablesVsReceipts": float(tender_data.get("receivablesVsReceipts", 0) or 0),
                "posVsThreePO": float(tender_data.get("posVsThreePO", 0) or 0),
                "booked": int(tender_data.get("booked", 0) or 0),
                "promo": int(tender_data.get("promo", 0) or 0),
                "deltaPromo": int(tender_data.get("deltaPromo", 0) or 0),
                "allThreePOCharges": float(all_three_po_charges),
                "allPOSCharges": float(all_pos_charges),
                "totalReceivables": float(tender_data.get("totalReceivables", 0) or 0),
                "totalReceipts": int(tender_data.get("totalReceipts", 0) or 0)
            }
            tender_wise_data.append(tender_item)
            logger.info(f"   📋 Tender: {tender_name}, POS Sales: {tender_item['posSales']}, 3PO Sales: {tender_item['threePOSales']}")
        
        # Calculate computed fields for top-level response
        # Check if allThreePOCharges was already aggregated from MongoDB (via mapping)
        # If the key exists in response_fields (meaning it was aggregated), use it; otherwise calculate it
        if "allThreePOCharges" in response_fields:
            all_three_po_charges_total = float(response_fields.get("allThreePOCharges", 0) or 0)
            logger.info(f"   ✅ Using allThreePOCharges from MongoDB (total): {all_three_po_charges_total}")
        else:
            # Fallback: calculate from component fields
            all_three_po_charges_total = (
                response_fields.get("threePOCharges", 0) +
                response_fields.get("promo", 0) +
                response_fields.get("threePODiscounts", 0) +
                response_fields.get("threePOFreebies", 0) +
                response_fields.get("threePOCommission", 0)
            )
            logger.info(f"   📊 Calculated allThreePOCharges (total): {all_three_po_charges_total}")
        
        # Check if allPOSCharges was already aggregated from MongoDB (via mapping)
        if "allPOSCharges" in response_fields:
            all_pos_charges_total = float(response_fields.get("allPOSCharges", 0) or 0)
            logger.info(f"   ✅ Using allPOSCharges from MongoDB (total): {all_pos_charges_total}")
        else:
            # Fallback: calculate from component fields
            all_pos_charges_total = (
                response_fields.get("posCharges", 0) +
                response_fields.get("promo", 0) +
                response_fields.get("posDiscounts", 0) +
                response_fields.get("posFreebies", 0) +
                response_fields.get("posCommission", 0)
            )
            logger.info(f"   📊 Calculated allPOSCharges (total): {all_pos_charges_total}")
        
        # Calculate top-level totals from tender-wise data if available
        total_pos_sales_from_tenders = sum(item.get("posSales", 0) for item in tender_wise_data)
        total_three_po_sales_from_tenders = sum(item.get("threePOSales", 0) for item in tender_wise_data)
        
        # Use tender-wise totals if available, otherwise use aggregated totals
        final_pos_sales = total_pos_sales_from_tenders if tender_wise_data else response_fields.get("posSales", 0)
        final_three_po_sales = total_three_po_sales_from_tenders if tender_wise_data else response_fields.get("threePOSales", 0)
        
        # Prepare final response
        response = {
            "posSales": final_pos_sales,
            "posReceivables": response_fields.get("posReceivables", 0.0),
            "posCommission": response_fields.get("posCommission", 0.0),
            "posCharges": response_fields.get("posCharges", 0.0),
            "posDiscounts": response_fields.get("posDiscounts", 0),
            "posFreebies": response_fields.get("posFreebies", 0),
            "threePOSales": final_three_po_sales,
            "threePOReceivables": response_fields.get("threePOReceivables", 0.0),
            "threePOCommission": response_fields.get("threePOCommission", 0.0),
            "threePOCharges": response_fields.get("threePOCharges", 0.0),
            "threePODiscounts": response_fields.get("threePODiscounts", 0),
            "threePOFreebies": response_fields.get("threePOFreebies", 0),
            "reconciled": response_fields.get("reconciled", 0),
            "receivablesVsReceipts": response_fields.get("receivablesVsReceipts", 0.0),
            "posVsThreePO": response_fields.get("posVsThreePO", 0.0),
            "booked": response_fields.get("booked", 0),
            "promo": response_fields.get("promo", 0),
            "deltaPromo": response_fields.get("deltaPromo", 0),
            "allThreePOCharges": float(all_three_po_charges_total),
            "allPOSCharges": float(all_pos_charges_total),
            "totalReceivables": response_fields.get("totalReceivables", 0.0),
            "totalReceipts": response_fields.get("totalReceipts", 0),
            "threePOData": tender_wise_data,
            "tenderWisePOSData": tender_wise_data,
            "instoreTotal": final_pos_sales
        }
        
        logger.info("✅ API Request Completed Successfully")
        
        return {
            "success": True,
            "data": response
        }
        
    except Exception as e:
        logger.error(f"Get 3PO dashboard data new error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching 3PO dashboard data: {str(e)}"
        )


@router.post("/instore-data")
async def get_instore_dashboard_data(
    request_data: InstoreDataRequest,
    db: AsyncSession = Depends(get_main_db),  # Using main_db for reconciliation tables
    current_user: UserDetails = Depends(get_current_user)
):
    """Get instore dashboard data - matches Node.js getInstoreDashboardData structure"""
    try:
        from sqlalchemy.sql import text
        from datetime import datetime, date
        
        logger.info("===========================================")
        logger.info("🚀 /instore-data API IS HIT")
        logger.info("===========================================")
        
        startDate = request_data.start_date
        endDate = request_data.end_date
        stores = request_data.stores
        
        if not startDate or not endDate or not stores:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required parameters: startDate, endDate, or stores"
            )
        
        # Convert date strings to datetime objects
        start_datetime = datetime.strptime(startDate, "%Y-%m-%d %H:%M:%S")
        end_datetime = datetime.strptime(endDate, "%Y-%m-%d %H:%M:%S")
        
        # Helper function to create default bank data
        def create_default_bank_data(bank_name):
            return {
                "sales": 0,
                "salesCount": 0,
                "receipts": 0,
                "receiptsCount": 0,
                "reconciled": 0,
                "reconciledCount": 0,
                "difference": 0,
                "differenceCount": 0,
                "charges": 0,
                "booked": 0,
                "posVsTrm": 0,
                "trmVsMpr": 0,
                "mprVsBank": 0,
                "salesVsPickup": 0,
                "pickupVsReceipts": 0,
                "bankName": bank_name,
                "missingTidValue": "0/0",
                "unreconciled": 0,
            }
        
        # Helper function to create default tender data structure
        def create_default_tender_data(tender_name, banks):
            return {
                "sales": 0,
                "salesCount": 0,
                "receipts": 0,
                "receiptsCount": 0,
                "reconciled": 0,
                "reconciledCount": 0,
                "difference": 0,
                "differenceCount": 0,
                "charges": 0,
                "booked": 0,
                "posVsTrm": 0,
                "trmVsMpr": 0,
                "mprVsBank": 0,
                "salesVsPickup": 0,
                "pickupVsReceipts": 0,
                "tenderName": tender_name,
                "bankWiseDataList": [create_default_bank_data(bank) for bank in banks],
                "trmSalesData": {
                    "sales": 0,
                    "salesCount": 0,
                    "receipts": 0,
                    "receiptsCount": 0,
                    "reconciled": 0,
                    "reconciledCount": 0,
                    "difference": 0,
                    "differenceCount": 0,
                    "charges": 0,
                    "booked": 0,
                    "posVsTrm": 0,
                    "trmVsMpr": 0,
                    "mprVsBank": 0,
                    "salesVsPickup": 0,
                    "pickupVsReceipts": 0,
                    "unreconciled": 0,
                },
                "unreconciled": 0,
            }
        
        # Build dynamic IN clause for stores
        def build_store_params(stores_list, prefix="store"):
            placeholders = ",".join([f":{prefix}_{i}" for i in range(len(stores_list))])
            params = {f"{prefix}_{i}": store for i, store in enumerate(stores_list)}
            return placeholders, params
        
        # Helper function to format SQL for readable logging (security: only for display)
        def format_sql_for_logging(query_str, params_dict):
            """Format SQL query with actual parameter values for logging only.
            This is for readability - actual queries remain parameterized for security."""
            formatted_query = query_str
            # Sort keys by length (longest first) to avoid partial replacements
            # e.g., replace :store_10 before :store_1
            sorted_keys = sorted(params_dict.keys(), key=lambda k: (-len(k), k))
            for key in sorted_keys:
                value = params_dict[key]
                # Handle different value types
                if isinstance(value, str):
                    formatted_value = f"'{value}'"
                elif isinstance(value, datetime):
                    formatted_value = f"'{value}'"
                elif isinstance(value, date):
                    formatted_value = f"'{value}'"
                elif value is None:
                    formatted_value = "NULL"
                else:
                    formatted_value = str(value)
                # Replace all occurrences of the parameter
                formatted_query = formatted_query.replace(f":{key}", formatted_value)
            return formatted_query.strip()
        
        # Helper to convert to int if zero, otherwise keep as float
        def format_number(value):
            if value is None or value == 0:
                return 0
            val = float(value)
            return int(val) if val == 0 else val
        
        # Query 1: Aggregator Total (for online orders)
        logger.info("📊 QUERY 1: Aggregator Total")
        aggregators = ["Zomato", "Swiggy", "MagicPin"]
        aggregators_placeholder = ",".join([f":agg_{i}" for i in range(len(aggregators))])
        aggregators_params = {f"agg_{i}": agg for i, agg in enumerate(aggregators)}
        
        stores_placeholder_1, stores_params_1 = build_store_params(stores, "store_1")
        aggregator_query_str = f"""
            SELECT SUM(CAST(COALESCE(payment, 0) AS DECIMAL(15,2))) AS aggregatorSales
            FROM orders
            WHERE date BETWEEN :start_date AND :end_date
            AND store_name IN ({stores_placeholder_1})
            AND online_order_taker IN ({aggregators_placeholder})
        """
        aggregator_query = text(aggregator_query_str)
        
        params_1 = {
            "start_date": startDate,
            "end_date": endDate,
            **stores_params_1,
            **aggregators_params
        }
        
        logger.info("📝 Aggregator Query SQL (for logging only):\n%s", format_sql_for_logging(aggregator_query_str, params_1))
        
        aggregator_result = await db.execute(aggregator_query, params_1)
        aggregator_row = aggregator_result.first()
        aggregator_total = float(aggregator_row.aggregatorSales or 0) if aggregator_row else 0
        
        logger.info(f"📊 Aggregator Total: {aggregator_total}")
        
        # Bank name mappings for CARD and UPI (matching Node.js line 3188-3189)
        cardBanks = ["AMEX", "YES", "ICICI", "ICICI_LYRA", "HDFC", "SBI87"]
        upiBanks = ["PHONEPE", "YES_BANK_QR"]
        
        logger.info(f"📊 Using predefined bank lists - CARD: {cardBanks}, UPI: {upiBanks}")
        
        # Build acquirer to bank name mapping for processing reconciliation data
        # This helps normalize acquirer names from database to standard bank names
        acquirer_to_bank_map = {}
        
        # Helper function to normalize acquirer name to bank name
        def normalize_acquirer_to_bank(acquirer_raw):
            acquirer_upper = (acquirer_raw or "").strip().upper()
            if not acquirer_upper:
                return None
            
            # Check if already mapped
            if acquirer_upper in acquirer_to_bank_map:
                return acquirer_to_bank_map[acquirer_upper]
            
            # Normalize acquirer names (handling variations)
            bank_name = acquirer_upper
            if "ICICI" in acquirer_upper and ("LYRA" in acquirer_upper or "ICICI LYRA" in acquirer_upper):
                bank_name = "ICICI_LYRA"
            elif "ICICI LYRA" in acquirer_upper:
                bank_name = "ICICI_LYRA"
            elif "SBI" in acquirer_upper:
                bank_name = "SBI87"
            elif "YES BANK QR" in acquirer_upper or "YES_BANK_QR" in acquirer_upper:
                bank_name = "YES_BANK_QR"
            elif "PHONEPE" in acquirer_upper:
                bank_name = "PHONEPE"
            elif "AMEX" in acquirer_upper:
                bank_name = "AMEX"
            elif "YES" in acquirer_upper:
                bank_name = "YES"
            elif "HDFC" in acquirer_upper:
                bank_name = "HDFC"
            
            # Cache the mapping
            acquirer_to_bank_map[acquirer_upper] = bank_name
            return bank_name
        
        # Initialize response structure with predefined banks (matching Node.js line 3254-3256)
        tenderWiseData = {
            "CARD": create_default_tender_data("CARD", cardBanks),
            "UPI": create_default_tender_data("UPI", upiBanks),
        }
        
        # Query 2: POS Sales Data for CARD/UPI from orders table (using online_order_taker column)
        # FIXED: Changed from trm table to orders table to match Node.js implementation
        logger.info("📊 QUERY 2: POS Sales Data for CARD/UPI from orders table")
        stores_placeholder_2, stores_params_2 = build_store_params(stores, "store_2")
        pos_sales_rows = []
        
        try:
            # Query orders table (matching Node.js implementation)
            # Note: orders table has DATE type, so we use direct date comparison
            pos_sales_query_str = f"""
                SELECT 
                    UPPER(TRIM(online_order_taker)) AS tender,
                    SUM(CAST(COALESCE(payment, 0) AS DECIMAL(15,2))) AS sales,
                    COUNT(CASE WHEN payment IS NOT NULL AND payment != 0 THEN 1 END) AS salesCount
                FROM orders
                WHERE date BETWEEN :start_date AND :end_date
                AND store_name IN ({stores_placeholder_2})
                AND (
                    UPPER(TRIM(online_order_taker)) = 'CARD'
                    OR UPPER(TRIM(online_order_taker)) = 'UPI'
                    OR UPPER(online_order_taker) = 'CARD'
                    OR UPPER(online_order_taker) = 'UPI'
                )
                GROUP BY UPPER(TRIM(online_order_taker))
            """
            pos_sales_query = text(pos_sales_query_str)
                
            params_2 = {
                "start_date": startDate,
                "end_date": endDate,
                **stores_params_2
            }
                
            logger.info(f"📝 Query Parameters: start_date={startDate}, end_date={endDate}, stores_count={len(stores)}")
            logger.info("📝 POS Sales Query SQL (for logging only):\n%s", format_sql_for_logging(pos_sales_query_str, params_2))
                
            # Execute query
            pos_sales_result = await db.execute(pos_sales_query, params_2)
            pos_sales_rows = pos_sales_result.fetchall()
            logger.info(f"📊 Found {len(pos_sales_rows)} tender(s) from orders table")
            
            # Diagnostic: Check if ANY data exists in orders table for this date range
            try:
                diagnostic_query = text(f"""
                    SELECT COUNT(*) as total_orders_records
                    FROM orders
                    WHERE date BETWEEN :start_date AND :end_date
                    AND store_name IN ({stores_placeholder_2})
                    AND (
                        UPPER(TRIM(online_order_taker)) = 'CARD'
                        OR UPPER(TRIM(online_order_taker)) = 'UPI'
                    )
                """)
                diagnostic_result = await db.execute(diagnostic_query, params_2)
                diagnostic_row = diagnostic_result.first()
                logger.info(f"🔍 Diagnostic: Found {diagnostic_row.total_orders_records if diagnostic_row else 0} total CARD/UPI orders in date range")
            except Exception as diag_error:
                logger.warning(f"⚠️  Diagnostic query failed: {diag_error}")
            
            # Check distinct online_order_taker values in orders table
            try:
                tender_check_query = text(f"""
                    SELECT DISTINCT UPPER(TRIM(online_order_taker)) AS tender, COUNT(*) as count
                    FROM orders
                    WHERE date BETWEEN :start_date AND :end_date
                    AND store_name IN ({stores_placeholder_2})
                    GROUP BY UPPER(TRIM(online_order_taker))
                    ORDER BY count DESC
                    LIMIT 10
                """)
                tender_check_result = await db.execute(tender_check_query, params_2)
                tender_check_rows = tender_check_result.fetchall()
                if tender_check_rows:
                    logger.info(f"🔍 Diagnostic: Distinct online_order_taker values found in orders table:")
                    for row in tender_check_rows:
                        logger.info(f"   - {row.tender}: {row.count} records")
            except Exception as tender_check_error:
                logger.warning(f"⚠️  Tender check query failed: {tender_check_error}")
            
            # Check if reconciliation table has any data at all (not just for this date range)
            reconciliation_check_query = text("""
                SELECT COUNT(*) as total_records
                FROM pos_vs_trm_summary
                WHERE payment_mode IN ('CARD', 'UPI')
                LIMIT 1
            """)
            reconciliation_check_result = await db.execute(reconciliation_check_query)
            reconciliation_check_row = reconciliation_check_result.first()
            total_reconciliation_records = reconciliation_check_row.total_records if reconciliation_check_row else 0
            logger.info(f"🔍 Diagnostic: Total CARD/UPI records in pos_vs_trm_summary table: {total_reconciliation_records}")
            
            if total_reconciliation_records == 0:
                logger.warning("⚠️  WARNING: pos_vs_trm_summary table is empty!")
                logger.warning("   This table needs to be populated by running the reconciliation process.")
                logger.warning("   In Node.js, this is done via: POST /api/node/reconciliation/generate-common-trm")
                logger.warning("   Or ensure ENVIRONMENT=production to connect to AWS RDS database with populated data.")
                logger.info("   ✅ Fallback: Using orders table data for sales (matching Node.js behavior)")
            
            logger.info(f"📊 Found {len(pos_sales_rows)} tender(s) from orders table")
            if len(pos_sales_rows) == 0:
                logger.warning("⚠️  No POS sales data found! This could mean:")
                logger.warning("   1. No data exists for date range: %s to %s", startDate, endDate)
                logger.warning("   2. No CARD/UPI records in orders table for the selected stores")
                logger.warning("   3. Store names don't match database values")
                logger.warning("   4. online_order_taker column values don't match 'CARD' or 'UPI'")
            else:
                for row in pos_sales_rows:
                    logger.info(f"   📋 Found: {row.tender} - sales={row.sales}, count={row.salesCount}")
        except Exception as pos_sales_error:
            logger.error(f"❌ Error fetching POS sales data from orders table: {pos_sales_error}", exc_info=True)
            pos_sales_rows = []
        
        # Process POS sales data from orders table and populate tender-level sales
        # FIXED: Changed to use 'tender' column (from online_order_taker) instead of 'payment_mode'
        for row in pos_sales_rows:
            tender = (row.tender or "").strip().upper()
            sales = float(row.sales or 0)
            sales_count = int(row.salesCount or 0)
            
            logger.info(f"📊 Processing POS sales: tender={tender}, sales={sales}, salesCount={sales_count}")
            
            if tender == "CARD" and "CARD" in tenderWiseData:
                tenderWiseData["CARD"]["sales"] += sales
                tenderWiseData["CARD"]["salesCount"] += sales_count
                # posVsTrm = POS sales amount (from orders table)
                tenderWiseData["CARD"]["posVsTrm"] = sales
            elif tender == "UPI" and "UPI" in tenderWiseData:
                tenderWiseData["UPI"]["sales"] += sales
                tenderWiseData["UPI"]["salesCount"] += sales_count
                # posVsTrm = POS sales amount (from orders table)
                tenderWiseData["UPI"]["posVsTrm"] = sales
        
        # Query 3: Reconciliation Data from pos_vs_trm_summary (optional)
        logger.info("📊 QUERY 3: Reconciliation Data from pos_vs_trm_summary")
        reconciliation_data = []
        
        try:
            stores_placeholder_3, stores_params_3 = build_store_params(stores, "store_3")
            reconciliation_query_str = f"""
                SELECT 
                    payment_mode,
                    acquirer,
                    SUM(COALESCE(pos_amount, 0)) AS sales,
                    COUNT(CASE WHEN pos_amount IS NOT NULL THEN 1 END) AS salesCount,
                    SUM(COALESCE(reconciled_amount, 0)) AS reconciled,
                    COUNT(CASE WHEN reconciled_amount IS NOT NULL AND reconciled_amount > 0 THEN 1 END) AS reconciledCount,
                    SUM(COALESCE(unreconciled_amount, 0)) AS unreconciled,
                    SUM(
                        CASE 
                            WHEN COALESCE(pos_amount, 0) != COALESCE(trm_amount, 0) 
                            THEN ABS(COALESCE(pos_amount, 0) - COALESCE(trm_amount, 0)) 
                            ELSE 0 
                        END
                    ) AS difference,
                    COUNT(
                        CASE 
                            WHEN COALESCE(pos_amount, 0) != COALESCE(trm_amount, 0) 
                            THEN 1 
                        END
                    ) AS differenceCount
                FROM pos_vs_trm_summary
                WHERE pos_date BETWEEN :start_date AND :end_date
                AND pos_store IN ({stores_placeholder_3})
                AND payment_mode IN ('CARD', 'UPI')
                GROUP BY payment_mode, acquirer
            """
            reconciliation_query = text(reconciliation_query_str)
            
            params_3 = {
                "start_date": startDate,
                "end_date": endDate,
                **stores_params_3
            }
            
            logger.info("📝 Reconciliation Query SQL (for logging only):\n%s", format_sql_for_logging(reconciliation_query_str, params_3))
            
            reconciliation_result = await db.execute(reconciliation_query, params_3)
            reconciliation_data = reconciliation_result.fetchall()
            logger.info(f"📊 Found {len(reconciliation_data)} bank/acquirer combination(s) from reconciliation table")
            if len(reconciliation_data) > 0:
                for idx, row in enumerate(reconciliation_data[:5]):  # Log first 5 rows
                    logger.info(f"  Row {idx}: payment_mode={row.payment_mode}, acquirer={row.acquirer}, sales={row.sales}")
        except Exception as rec_error:
            logger.warning(f"⚠️  Could not fetch reconciliation data: {rec_error}")
            reconciliation_data = []
        
        # Query 4: TRM Data from trm table
        logger.info("📊 QUERY 4: TRM Data")
        trm_data = []
        
        # acquirer_to_bank_map is already populated from Query 1.5 above
        
        try:
            stores_placeholder_4, stores_params_4 = build_store_params(stores, "store_4")
            # Try standard date format (YYYY-MM-DD HH:MM:SS)
            trm_query_str = f"""
                SELECT 
                    payment_mode,
                    acquirer,
                    SUM(COALESCE(amount, 0)) AS trmAmount,
                    COUNT(CASE WHEN amount IS NOT NULL THEN 1 END) AS trmCount
                FROM trm
                WHERE STR_TO_DATE(date, '%Y-%m-%d %H:%i:%s') BETWEEN STR_TO_DATE(:start_date, '%Y-%m-%d %H:%i:%s') AND STR_TO_DATE(:end_date, '%Y-%m-%d %H:%i:%s')
                AND store_name IN ({stores_placeholder_4})
                AND payment_mode IN ('CARD', 'UPI')
                GROUP BY payment_mode, acquirer
            """
            trm_query = text(trm_query_str)
            
            params_4 = {
                "start_date": startDate,
                "end_date": endDate,
                **stores_params_4
            }
            
            logger.info("📝 TRM Query SQL (for logging only):\n%s", format_sql_for_logging(trm_query_str, params_4))
            
            trm_result = await db.execute(trm_query, params_4)
            trm_data = trm_result.fetchall()
            logger.info(f"📊 Found {len(trm_data)} TRM records with standard date format")
            
            # If no data, try alternative date format (DD/MM/YYYY HH:MM:SS)
            if len(trm_data) == 0:
                try:
                    logger.info("📊 Trying alternative date format for TRM query")
                    trm_query_str_alt = f"""
                        SELECT 
                            payment_mode,
                            acquirer,
                            SUM(COALESCE(amount, 0)) AS trmAmount,
                            COUNT(CASE WHEN amount IS NOT NULL THEN 1 END) AS trmCount
                        FROM trm
                        WHERE STR_TO_DATE(date, '%d/%m/%Y %H:%i:%s') BETWEEN STR_TO_DATE(:start_date, '%Y-%m-%d %H:%i:%s') AND STR_TO_DATE(:end_date, '%Y-%m-%d %H:%i:%s')
                        AND store_name IN ({stores_placeholder_4})
                        AND payment_mode IN ('CARD', 'UPI')
                        GROUP BY payment_mode, acquirer
                    """
                    trm_query_alt = text(trm_query_str_alt)
                    trm_result_alt = await db.execute(trm_query_alt, params_4)
                    trm_data = trm_result_alt.fetchall()
                    logger.info(f"📊 Found {len(trm_data)} TRM records with alternative date format")
                except Exception as alt_error:
                    logger.warning(f"⚠️  Alternative date format query failed: {alt_error}")
                    trm_data = []
            
            # If still no data, try using DATE() function for date-only comparison
            if len(trm_data) == 0:
                try:
                    logger.info("📊 Trying date-only comparison for TRM query")
                    trm_query_str_date_only = f"""
                        SELECT 
                            payment_mode,
                            acquirer,
                            SUM(COALESCE(amount, 0)) AS trmAmount,
                            COUNT(CASE WHEN amount IS NOT NULL THEN 1 END) AS trmCount
                        FROM trm
                        WHERE DATE(STR_TO_DATE(date, '%Y-%m-%d %H:%i:%s')) BETWEEN DATE(:start_date) AND DATE(:end_date)
                        AND store_name IN ({stores_placeholder_4})
                        AND payment_mode IN ('CARD', 'UPI')
                        GROUP BY payment_mode, acquirer
                    """
                    trm_query_date_only = text(trm_query_str_date_only)
                    trm_result_date_only = await db.execute(trm_query_date_only, params_4)
                    trm_data = trm_result_date_only.fetchall()
                    logger.info(f"📊 Found {len(trm_data)} TRM records with date-only comparison")
                except Exception as date_only_error:
                    logger.warning(f"⚠️  Date-only comparison query failed: {date_only_error}")
            
            # Log warning if no TRM data found with date filter (but don't fall back to no date filter)
            if len(trm_data) == 0:
                logger.warning("⚠️  No TRM data found for the specified date range. This is expected if:")
                logger.warning("   - No TRM records exist for date range: %s to %s", startDate, endDate)
                logger.warning("   - Date format in trm.date column doesn't match expected formats")
                logger.warning("   - TRM data might be stored in a different table or format")
        except Exception as trm_error:
            logger.error(f"❌ Error fetching TRM data: {trm_error}", exc_info=True)
            trm_data = []
        
        # Process TRM data and populate trmVsMpr (tender-level only, matching Node.js)
        # Note: In Node.js, TRM is only used for trmVsMpr, NOT for sales
        for row in trm_data:
            payment_mode = (row.payment_mode or "").strip().upper()
            trm_amount = float(row.trmAmount or 0)

            # Add to tender-level trmVsMpr only (matching Node.js line 3529-3533)
            if payment_mode == "CARD" and "CARD" in tenderWiseData:
                tenderWiseData["CARD"]["trmVsMpr"] += trm_amount
            elif payment_mode == "UPI" and "UPI" in tenderWiseData:
                tenderWiseData["UPI"]["trmVsMpr"] += trm_amount
        
        # Process reconciliation data and map acquirer to bank names
        for row in reconciliation_data:
            payment_mode = (row.payment_mode or "").strip().upper()
            acquirer_raw = (row.acquirer or "").strip()
            
            # Normalize acquirer to bank name using helper function (matching Node.js line 3540)
            bank_name = normalize_acquirer_to_bank(acquirer_raw) or acquirer_raw.upper()
            
            logger.info(f"📊 Processing reconciliation row: payment_mode={payment_mode}, acquirer={acquirer_raw}, bank_name={bank_name}, sales={row.sales}")
            
            # Process reconciliation data (matching Node.js lines 3542-3566)
            # Only process if bank is in the predefined list (Node.js checks cardBanks.includes/bankName)
            if payment_mode == "CARD" and bank_name in cardBanks:
                bank_index = cardBanks.index(bank_name)
                if bank_index >= 0 and bank_index < len(tenderWiseData["CARD"]["bankWiseDataList"]):
                    bank_data = tenderWiseData["CARD"]["bankWiseDataList"][bank_index]
                    bank_data["sales"] += float(row.sales or 0)
                    bank_data["salesCount"] += int(row.salesCount or 0)
                    bank_data["reconciled"] += float(row.reconciled or 0)
                    bank_data["reconciledCount"] += int(row.reconciledCount or 0)
                    bank_data["unreconciled"] += float(row.unreconciled or 0)
                    bank_data["difference"] += float(row.difference or 0)
                    bank_data["differenceCount"] += int(row.differenceCount or 0)
                    logger.info(f"  ✅ Added to CARD.{bank_name}: sales={bank_data['sales']}")
            
            elif payment_mode == "UPI" and bank_name in upiBanks:
                bank_index = upiBanks.index(bank_name)
                if bank_index >= 0 and bank_index < len(tenderWiseData["UPI"]["bankWiseDataList"]):
                    bank_data = tenderWiseData["UPI"]["bankWiseDataList"][bank_index]
                    bank_data["sales"] += float(row.sales or 0)
                    bank_data["salesCount"] += int(row.salesCount or 0)
                    bank_data["reconciled"] += float(row.reconciled or 0)
                    bank_data["reconciledCount"] += int(row.reconciledCount or 0)
                    bank_data["unreconciled"] += float(row.unreconciled or 0)
                    bank_data["difference"] += float(row.difference or 0)
                    bank_data["differenceCount"] += int(row.differenceCount or 0)
                    logger.info(f"  ✅ Added to UPI.{bank_name}: sales={bank_data['sales']}")
        
        # Aggregate data for each tender
        for tender_key in tenderWiseData:
            tender_data = tenderWiseData[tender_key]
            sales_from_orders = tender_data["sales"]
            sales_count_from_orders = tender_data["salesCount"]
            
            # Reset sales counters (will be recalculated from bank data if reconciliation table exists)
            tender_data["sales"] = 0
            tender_data["salesCount"] = 0
            
            # Sum up all bank data for this tender
            for bank_data in tender_data["bankWiseDataList"]:
                tender_data["sales"] += bank_data["sales"]
                tender_data["salesCount"] += bank_data["salesCount"]
                tender_data["reconciled"] += bank_data["reconciled"]
                tender_data["reconciledCount"] += bank_data["reconciledCount"]
                tender_data["difference"] += bank_data["difference"]
                tender_data["differenceCount"] += bank_data["differenceCount"]
                tender_data["unreconciled"] += bank_data["unreconciled"]
        
            # If reconciliation table had no data, use orders table data instead (matching Node.js line 3592-3596)
            if tender_data["sales"] == 0 and sales_from_orders > 0:
                tender_data["sales"] = sales_from_orders
                tender_data["salesCount"] = sales_count_from_orders
            
            # Copy to TRM sales data including posVsTrm and trmVsMpr (matching Node.js line 3598-3607)
            tender_data["trmSalesData"]["sales"] = tender_data["sales"]
            tender_data["trmSalesData"]["salesCount"] = tender_data["salesCount"]
            tender_data["trmSalesData"]["reconciled"] = tender_data["reconciled"]
            tender_data["trmSalesData"]["reconciledCount"] = tender_data["reconciledCount"]
            tender_data["trmSalesData"]["difference"] = tender_data["difference"]
            tender_data["trmSalesData"]["differenceCount"] = tender_data["differenceCount"]
            tender_data["trmSalesData"]["unreconciled"] = tender_data["unreconciled"]
            tender_data["trmSalesData"]["posVsTrm"] = tender_data["posVsTrm"]
            tender_data["trmSalesData"]["trmVsMpr"] = tender_data["trmVsMpr"]
        
        # Calculate overall totals
        overall_totals = {
            "sales": 0,
            "salesCount": 0,
            "receipts": 0,
            "receiptsCount": 0,
            "reconciled": 0,
            "reconciledCount": 0,
            "difference": 0,
            "differenceCount": 0,
            "charges": 0,
            "booked": 0,
            "posVsTrm": 0,
            "trmVsMpr": 0,
            "mprVsBank": 0,
            "salesVsPickup": 0,
            "pickupVsReceipts": 0,
            "unreconciled": 0,
        }
        
        for tender_data in tenderWiseData.values():
            overall_totals["sales"] += tender_data["sales"]
            overall_totals["salesCount"] += tender_data["salesCount"]
            overall_totals["reconciled"] += tender_data["reconciled"]
            overall_totals["reconciledCount"] += tender_data["reconciledCount"]
            overall_totals["difference"] += tender_data["difference"]
            overall_totals["differenceCount"] += tender_data["differenceCount"]
            overall_totals["unreconciled"] += tender_data["unreconciled"]
            overall_totals["posVsTrm"] += tender_data.get("posVsTrm", 0) or 0
            overall_totals["trmVsMpr"] += tender_data.get("trmVsMpr", 0) or 0
        
        # Calculate trmSalesData totals (aggregate from all tenders)
        # Note: Only sales and salesCount are summed. Other fields remain null/0 as per Node.js behavior
        trm_sales_data_totals = {
            "sales": 0,
            "salesCount": 0,
            "receipts": 0,
            "receiptsCount": 0,
            "reconciled": None,  # null in Node.js response
            "reconciledCount": None,  # null in Node.js response
            "difference": 0,
            "differenceCount": 0,
            "charges": None,  # null in Node.js response
            "booked": 0,
            "posVsTrm": 0,
            "trmVsMpr": 0,
            "mprVsBank": 0,
            "salesVsPickup": 0,
            "pickupVsReceipts": 0,
            "unreconciled": 0,
        }
        
        for tender_data in tenderWiseData.values():
            trm_sales_data_totals["sales"] += tender_data["trmSalesData"]["sales"] or 0
            trm_sales_data_totals["salesCount"] += tender_data["trmSalesData"]["salesCount"] or 0
            trm_sales_data_totals["posVsTrm"] += tender_data["trmSalesData"].get("posVsTrm", 0) or 0
            trm_sales_data_totals["trmVsMpr"] += tender_data["trmSalesData"].get("trmVsMpr", 0) or 0
        
        # Build final response
        response = {
            **overall_totals,
            "tenderWiseDataList": [tenderWiseData["CARD"], tenderWiseData["UPI"]],
            "trmSalesData": trm_sales_data_totals,
            "aggregatorTotal": aggregator_total
        }
        
        # Check if we're getting zero values and provide helpful guidance
        from app.config.settings import settings
        if overall_totals["sales"] == 0 and overall_totals["salesCount"] == 0:
            logger.warning("⚠️  API returned zero sales/salesCount. Possible reasons:")
            if settings.environment != "production":
                logger.warning(f"   - Current environment: {settings.environment}")
                logger.warning("   - To connect to AWS RDS (production database), set ENVIRONMENT=production")
                logger.warning("   - Or ensure local database has CARD/UPI orders in 'orders' table")
                logger.warning("   - Or populate 'pos_vs_trm_summary' by running reconciliation process")
            else:
                logger.warning("   - Connected to production database but no data found")
                logger.warning("   - Check if date range and stores are correct")
                logger.warning("   - Ensure pos_vs_trm_summary table is populated")
        
        logger.info("✅ API Request Completed Successfully")
        
        return {
            "success": True,
            "data": response
        }
        
    except Exception as e:
        logger.error(f"Get instore dashboard data error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching instore dashboard data: {str(e)}"
        )


# NOTE: The /generate-common-trm endpoint has been moved below to implement the full pipeline
# This matches the Node.js implementation which runs the complete reconciliation pipeline


@router.get("/download/{filename}")
async def download_file(
    filename: str,
    db: AsyncSession = Depends(get_sso_db)
):
    """Download generated file - matches Node.js implementation (no auth required)"""
    try:
        import os
        from pathlib import Path
        from fastapi.responses import FileResponse
        
        # Get the absolute path to the reports directory
        # This should be relative to the python directory (where reports/ is located)
        # Get the directory of this file (python/app/routes/reconciliation.py)
        # Go up 2 levels to get to python/ directory, then join with reports/
        current_file = Path(__file__)
        python_dir = current_file.parent.parent.parent  # From app/routes/ to python/
        reports_dir = python_dir / "reports"
        
        # Create reports directory if it doesn't exist
        reports_dir.mkdir(exist_ok=True)
        
        # Build the file path
        file_path = reports_dir / filename
        
        # Normalize the path to prevent directory traversal attacks
        file_path = file_path.resolve()
        reports_dir_resolved = reports_dir.resolve()
        
        # Ensure the file is within the reports directory (security check)
        # Use resolve() to prevent directory traversal attacks (e.g., ../.. attacks)
        try:
            file_path.relative_to(reports_dir_resolved)
        except ValueError:
            # File is outside reports directory
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid file path"
            )
        
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"File not found: {filename}"
            )
        
        if not file_path.is_file():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Path is not a file"
            )
        
        # Return the file for download
        logger.info(f"Serving file: {file_path}")
        return FileResponse(
            path=str(file_path),
            filename=filename,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Download file error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error downloading file: {str(e)}"
        )


@router.get("/cities")
async def get_all_cities(
    db: AsyncSession = Depends(get_main_db)
):
    """Get all cities (Node-compatible):
    - No auth required
    - Source of truth: devyani_city table
    - Fields: id, city_id, city_name
    """
    try:
        # Query devyani_city like Node implementation
        logger.info("[CITIES] Controller hit: GET /api/reconciliation/cities")
        from sqlalchemy import text
        sql_query = (
            """
                SELECT id, city_id, city_name
                FROM devyani_cities
                ORDER BY city_name ASC
                """
        )
        logger.info("[CITIES] SQL: %s", sql_query.replace("\n", " ").strip())
        result = await db.execute(
            text(sql_query)
        )
        rows = result.mappings().all() or []

        # Normalize to plain JSON-serializable dicts
        data = [
            {
                "id": int(row["id"]) if row.get("id") is not None else None,
                "city_id": str(row["city_id"]) if row.get("city_id") is not None else None,
                "city_name": row.get("city_name"),
            }
            for row in rows
        ]

        payload = {"success": True, "data": data}
        logger.info("[CITIES] Response: %s", json.dumps(payload))
        return payload
        
    except Exception as e:
        logger.error(f"Get cities error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching cities"
        )


@router.post("/stores")
async def get_stores_by_cities(
    request_data: StoresRequest,
    db: AsyncSession = Depends(get_main_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Get stores by cities (Node-compatible):
    - Matches Node.js /api/node/reconciliation/stores endpoint
    - Source of truth: devyani_stores table
    - Fields: id, code (from store_code), sap_code, store_name, city_id, posDataSync
    """
    try:
        logger.info("[STORES] Controller hit: POST /api/reconciliation/stores")
        
        startDate = request_data.startDate
        endDate = request_data.endDate
        cities = request_data.cities
        
        logger.info("[STORES] Request params: %s", json.dumps({
            "startDate": startDate,
            "endDate": endDate,
            "citiesCount": len(cities) if cities else 0,
        }))
        
        # Validate cities
        if not cities or len(cities) < 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="At least one city ID is required"
            )
        
        # Extract city IDs from cities array (supports both object and string format)
        city_ids_map = []
        for city in cities:
            if isinstance(city, dict):
                city_id = city.get("city_id") or city.get("id")
                if city_id:
                    city_ids_map.append(str(city_id))
            elif isinstance(city, str):
                city_ids_map.append(city)
        
        logger.info("[STORES] cityIdsMaps: %s", city_ids_map)
        logger.info("[STORES] Total city IDs: %d", len(city_ids_map))
        
        if not city_ids_map:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No valid city IDs found in request"
            )
        
        # Build WHERE clause with IN condition using parameterized query for safety
        from sqlalchemy import text
        
        # Create parameterized placeholders
        placeholders_str = ",".join([f":city_id_{i}" for i in range(len(city_ids_map))])
        
        # Build SQL query (date filters are commented out in Node.js, so not included here)
        sql_query = f"""
            SELECT 
                id,
                store_code AS code,
                sap_code,
                store_name,
                city_id
            FROM devyani_stores
            WHERE city_id IN ({placeholders_str})
            ORDER BY store_name ASC
        """
        
        # Prepare parameters dict
        params = {f"city_id_{i}": city_id for i, city_id in enumerate(city_ids_map)}
        
        logger.info("[STORES] WHERE clause: city_id IN (%s) with %d params", 
                   placeholders_str[:100] + "..." if len(placeholders_str) > 100 else placeholders_str, 
                   len(params))
        
        logger.info("[STORES] SQL: %s", sql_query.replace("\n", " ").strip()[:500] + "...")
        
        result = await db.execute(
            text(sql_query),
            params
        )
        rows = result.mappings().all() or []
        
        logger.info("[STORES] Found stores count: %d", len(rows))
        
        # Transform to match Node.js response format
        store_list = []
        for row in rows:
            store_list.append({
                "id": int(row["id"]) if row.get("id") is not None else None,
                "code": str(row["code"]) if row.get("code") is not None else None,
                "sap_code": str(row["sap_code"]) if row.get("sap_code") is not None else None,
                "store_name": row.get("store_name"),
                "city_id": str(row["city_id"]) if row.get("city_id") is not None else None,
                "posDataSync": True  # Hardcoded to true like Node.js implementation
            })
        
        response_payload = {
            "success": True,
            "data": store_list
        }
        
        logger.info("[STORES] Response payload: %s", json.dumps({
            "success": response_payload["success"],
            "dataCount": len(response_payload["data"])
        }))
        
        return response_payload
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[STORES] Error: %s", str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching stores"
        )


@router.get("/public/threepo/missingStoreMappings")
async def get_missing_store_mappings(
    db: AsyncSession = Depends(get_main_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Get missing store mappings for 3PO - matches Node.js implementation"""
    try:
        from sqlalchemy.sql import text
        
        # Query to find stores that don't have mappings in zomato_mappings table
        # This matches the Node.js query logic: stores NOT IN (SELECT store_code FROM zomato_mappings WHERE store_code IS NOT NULL)
        query = text("""
            SELECT 
                store_code,
                store_name,
                city,
                state,
                store_type,
                store_status
            FROM store
            WHERE store_code NOT IN (
                SELECT DISTINCT store_code 
                FROM zomato_mappings 
                WHERE store_code IS NOT NULL
            )
            ORDER BY store_name ASC
        """)
        
        result = await db.execute(query)
        stores = result.fetchall()
        
        # Match Node.js response format: { ZOMATO: [ ... ] }
        zomato_array = []
        for store in stores:
            zomato_array.append({
                "store_code": store.store_code,
                "store_name": store.store_name,
                "city": store.city,
                "state": store.state,
                "store_type": store.store_type,
                "store_status": store.store_status,
                "tender": "ZOMATO",
            })

        response_payload = {
            "success": True,
            "data": {"ZOMATO": zomato_array}
        }
        logger.info("missingStoreMappings response: %s", json.dumps(response_payload))
        return response_payload
        
    except Exception as e:
        logger.error(f"Get missing store mappings error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching missing store mappings: {str(e)}"
        )


@router.get("/public/dashboard/reportingTenders")
async def get_reporting_tenders():
    """Get reporting tenders grouped by category - matches Node.js implementation"""
    try:
        from datetime import datetime
        
        logger.info("[REPORTING_TENDERS] Controller hit: GET /public/dashboard/reportingTenders")

        # Static tender configuration matching the expected response format
        reporting_tenders = [
            {
                "category": "3PO",
                "tenders": [
                    {
                        "displayName": "Swiggy",
                        "technicalName": "SWIGGY"
                    },
                    {
                        "displayName": "Zomato",
                        "technicalName": "ZOMATO"
                    },
                    {
                        "displayName": "Magicpin",
                        "technicalName": "MAGICPIN"
                    }
                ]
            },
            {
                "category": "InStore",
                "tenders": [
                    {
                        "displayName": "Cards",
                        "technicalName": "CARD"
                    },
                    {
                        "displayName": "UPI",
                        "technicalName": "UPI"
                    }
                ]
            }
        ]

        response_payload = {
            "timestamp": datetime.now().isoformat(),
            "code": 200,
            "data": reporting_tenders
        }

        logger.info("[REPORTING_TENDERS] Response: %s", json.dumps(response_payload, indent=2))
        return response_payload
        
    except Exception as e:
        logger.error("[REPORTING_TENDERS] Error: %s", str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching reporting tenders"
        )


@router.get("/public/custom/reportFields")
async def get_report_fields(
    category: str = Query(..., description="Category/tender name (e.g., SWIGGY, ZOMATO)"),
    db: AsyncSession = Depends(get_main_db),
):
    """
    Get report fields for a given category/tender
    
    Queries customised_db_fields table to get field mappings:
    - excel_column_name -> name (display name)
    - db_column_name -> technicalName (camelCase version)
    
    Flow:
    1. Query customised_db_fields table filtering by tender_name = category
    2. Convert excel_column_name to display name (name field)
    3. Convert db_column_name from snake_case to camelCase (technicalName field)
    4. Return formatted response matching expected structure
    
    Table: customised_db_fields
    Columns used: tender_name, excel_column_name, db_column_name
    
    Example response for SWIGGY:
    {
        "timestamp": "2025-01-02T13:55:05.895Z",
        "code": 200,
        "data": [
            {
                "name": "Delivery Fee",
                "technicalName": "deliveryFee"
            },
            ...
        ]
    }
    """
    try:
        from sqlalchemy import text
        from datetime import datetime
        
        logger.info(f"[REPORT_FIELDS] Controller hit: GET /public/custom/reportFields?category={category}")
        
        # Helper function to convert snake_case to camelCase
        def snake_to_camel(snake_str):
            """Convert snake_case to camelCase"""
            if not snake_str:
                return ""
            # Split by underscore
            components = snake_str.split('_')
            # First component is lowercase, rest are capitalized
            return components[0].lower() + ''.join(word.capitalize() for word in components[1:])
        
        # Query customised_db_fields table filtered by tender_name
        # The category parameter maps to tender_name in the table
        query = text("""
            SELECT 
                excel_column_name,
                db_column_name
            FROM customised_db_fields 
            WHERE tender_name = :category
            AND excel_column_name IS NOT NULL 
            AND excel_column_name != ''
            AND db_column_name IS NOT NULL 
            AND db_column_name != ''
            ORDER BY excel_column_name
        """)
        
        result = await db.execute(query, {"category": category.upper()})
        rows = result.fetchall()
        
        logger.info(f"[REPORT_FIELDS] Found {len(rows)} records for category: {category}")
        
        # Transform rows to expected format
        report_fields = []
        for row in rows:
            excel_column_name = row[0]  # Display name
            db_column_name = row[1]      # Technical name (needs camelCase conversion)
            
            # Convert db_column_name from snake_case to camelCase
            technical_name = snake_to_camel(db_column_name)
            
            report_fields.append({
                "name": excel_column_name,
                "technicalName": technical_name
            })
        
        response_payload = {
            "timestamp": datetime.now().strftime("%d-%m-%YT%H:%M:%S.%f")[:-3] + "Z",
            "code": 200,
            "data": report_fields
        }
        
        logger.info(f"[REPORT_FIELDS] Response: {len(report_fields)} fields returned for category: {category}")
        return response_payload
        
    except Exception as e:
        logger.error(f"[REPORT_FIELDS] Error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching report fields: {str(e)}"
        )


# Compatibility: Node API used by frontend
@router.get("/api/v1/recologics/findOldestEffectiveDate")
async def find_oldest_effective_date(
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user),
):
    """Return an ISO date string indicating the oldest effective date.
    Original Java API wrapped this in an ApiResponse; the frontend only
    cares about the date string, so we keep the lightweight format.
    """
    try:
        from datetime import date

        return {
            "success": True,
            "data": date.today().isoformat(),
        }
    except Exception as e:
        logger.error("Find oldest effective date error: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching effective date",
        )


@router.get("/api/v1/tenderList")
async def get_tender_list(
    db: AsyncSession = Depends(get_main_db),
    current_user: UserDetails = Depends(get_current_user),
):
    """Get list of distinct data_source values - matches original Node/Java behaviour."""
    try:
        from sqlalchemy import text

        logger.info("[TENDER_LIST] Controller hit: GET /api/v1/tenderList")

        # Use table_columns_mapping as the single source of truth for tenders / data sources
        query = text(
            """
            SELECT DISTINCT data_source
            FROM table_columns_mapping
            WHERE data_source IS NOT NULL
              AND data_source != ''
            ORDER BY data_source
        """
        )

        result = await db.execute(query)
        rows = result.fetchall()

        # Extract data_source values into a list
        raw_tender_list = [row[0] for row in rows if row[0]]

        # Transform the list: replace _ with space, convert to title case but keep POS uppercase
        def format_tender_name(name: str) -> str:
            """Format tender name: replace _ with space, title case, but keep POS uppercase."""
            name = name.replace("_", " ")
            words = name.split()
            formatted_words = []
            for word in words:
                if word.upper() == "POS":
                    formatted_words.append("POS")
                else:
                    formatted_words.append(word.capitalize())
            return " ".join(formatted_words)

        tender_list = [format_tender_name(name) for name in raw_tender_list]

        logger.info("[TENDER_LIST] Response: %s", json.dumps(tender_list, indent=2))
        return tender_list

    except Exception as e:
        logger.error("[TENDER_LIST] Error: %s", str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching tender list",
        )


class TenderRequest(BaseModel):
    """Request model matching Java TenderRequest used by tenderWisetables and recologics/get."""

    tenders: List[str]


class ColumnsBean(BaseModel):
    dbColumnName: str
    excelColumnName: str


class TableAndColumnsBean(BaseModel):
    dataSourceName: str
    tableName: str
    columns: List[ColumnsBean]


class TenderAndTablesBean(BaseModel):
    tender: str
    dataSourceWiseColumns: List[TableAndColumnsBean]


@router.post("/api/v1/tenderWisetables")
async def get_tender_wise_tables(
    request_data: TenderRequest,
    db: AsyncSession = Depends(get_main_db),
    current_user: UserDetails = Depends(get_current_user),
):
    """Return tender-wise data source / table / column mappings.

    This mirrors the original Java TenderApisController.tenderWisetables endpoint,
    but uses the table_columns_mapping table instead of customised_db_fields.
    """
    try:
        from sqlalchemy import text

        logger.info(
            "[TENDER_WISE_TABLES] Controller hit: POST /api/v1/tenderWisetables "
            "for tenders=%s",
            request_data.tenders,
        )

        if not request_data.tenders:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="tenders field is required and cannot be empty",
            )

        # Map data_source names to table names
        data_source_to_table = {
            "ZOMATO": "zomato",
            "POS_ORDERS": "orders",
            "TRM": "trm",
            "MPR_HDFC_CARD": "mpr_hdfc_card",
            "MPR_HDFC_UPI": "mpr_hdfc_upi",
        }

        # Query distinct tender / data_source by joining with data_source table
        # The request tenders are formatted names (e.g., "Zomato", "POS Orders")
        # We need to match them with data_source.tender column
        params = {"tenders": tuple(request_data.tenders)}
        base_query = text(
            """
            SELECT DISTINCT ds.tender, tcm.data_source
            FROM table_columns_mapping tcm
            INNER JOIN data_source ds ON tcm.data_source = ds.name
            WHERE ds.tender IS NOT NULL
              AND ds.tender IN :tenders
        """
        )

        result = await db.execute(base_query, params)
        rows = result.fetchall()

        logger.info(
            "[TENDER_WISE_TABLES] Found %d tender/data_source rows", len(rows)
        )

        # Group by tender
        tender_table_map: Dict[str, List[TableAndColumnsBean]] = {}

        async def fetch_columns_for(data_source: str) -> List[ColumnsBean]:
            col_query = text(
                """
                SELECT excel_column_name, db_column_name
                FROM table_columns_mapping
                WHERE data_source = :data_source
            """
            )
            col_result = await db.execute(
                col_query, {"data_source": data_source}
            )
            col_rows = col_result.fetchall()
            return [
                ColumnsBean(
                    dbColumnName=row[1].strip() if row[1] else "",
                    excelColumnName=row[0].strip() if row[0] else "",
                )
                for row in col_rows
                if row[0] and row[1]
            ]

        # Build table/column structures
        for tender_name, data_source in rows:
            if tender_name not in tender_table_map:
                tender_table_map[tender_name] = []

            # Get table name from mapping
            table_name = data_source_to_table.get(data_source, data_source.lower())
            
            columns = await fetch_columns_for(data_source)
            table_bean = TableAndColumnsBean(
                dataSourceName=data_source,
                tableName=table_name,
                columns=columns,
            )
            tender_table_map[tender_name].append(table_bean)

        tender_list: List[TenderAndTablesBean] = []
        for tender_name, tables in tender_table_map.items():
            tender_list.append(
                TenderAndTablesBean(
                    tender=tender_name,
                    dataSourceWiseColumns=tables,
                )
            )

        response_payload = {
            "code": 200,
            "data": [t.model_dump() for t in tender_list],
        }

        logger.info(
            "[TENDER_WISE_TABLES] Returning %d tenders", len(response_payload["data"])
        )
        return response_payload

    except HTTPException:
        raise
    except Exception as e:
        logger.error("[TENDER_WISE_TABLES] Error: %s", str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching tender-wise tables",
        )


@router.post("/api/v1/recologics/get")
async def get_recologics_by_tender(
    request_data: TenderRequest,
    db: AsyncSession = Depends(get_main_db),
    current_user: UserDetails = Depends(get_current_user),
):
    """Return recologics records filtered by tender list.

    Mirrors the Java CustomRecoLogicController.getRecoLogicsByTenders endpoint which
    delegates to RecoLogicService.findLogicsByTender.
    """
    try:
        from sqlalchemy import text

        logger.info(
            "[RECOLOGICS_GET] Controller hit: POST /api/v1/recologics/get tenders=%s",
            request_data.tenders,
        )

        if not request_data.tenders:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="tenders field is required and cannot be empty",
            )

        # Java code joins multiple tenders into a sorted comma-separated string
        if len(request_data.tenders) == 1:
            tender_key = request_data.tenders[0]
        else:
            tender_key = ",".join(sorted(request_data.tenders))

        query = text(
            """
            SELECT id,
                   tender,
                   createdby,
                   effectivefrom,
                   effectiveto,
                   effectivetype,
                   recologic,
                   status,
                   remarks,
                   created_date,
                   updated_date
            FROM reco_logics
            WHERE tender = :tender_key
            ORDER BY created_date DESC
        """
        )

        result = await db.execute(query, {"tender_key": tender_key})
        rows = result.mappings().all()

        logger.info("[RECOLOGICS_GET] Found %d recologic rows", len(rows))

        # Return raw records wrapped in Java-style ApiResponse
        response_payload = {
            "code": 200,
            "data": rows,
        }
        return response_payload

    except HTTPException:
        raise
    except Exception as e:
        logger.error("[RECOLOGICS_GET] Error: %s", str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching recologics",
        )


class SaveRecoLogicsRequest(BaseModel):
    """Request model for saving recologics."""
    model_config = ConfigDict(populate_by_name=True)
    
    tenders: List[str]
    recoData: Union[Dict[str, Any], List[Dict[str, Any]], str]  # Can be dict, list, or JSON string
    effectiveFrom: Optional[str] = None
    effectiveTo: Optional[str] = None
    effectiveType: Optional[str] = None
    id: Optional[int] = None  # For updates
    remarks: Optional[str] = None
    status: Optional[str] = None  # Will be set to 'ACTIVE' for new records


@router.post("/api/v1/recologics/save")
async def save_recologics(
    request_data: SaveRecoLogicsRequest,
    db: AsyncSession = Depends(get_main_db),
    current_user: UserDetails = Depends(get_current_user),
):
    """Save new recologics record.
    
    Creates a new record in reco_logics table with the provided reconciliation logic.
    """
    try:
        from sqlalchemy import text
        
        logger.info(
            "[RECOLOGICS_SAVE] Controller hit: POST /api/v1/recologics/save tenders=%s",
            request_data.tenders,
        )
        
        if not request_data.tenders:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="tenders field is required and cannot be empty",
            )
        
        # Process tender list - join multiple tenders into sorted comma-separated string
        if len(request_data.tenders) == 1:
            tender_key = request_data.tenders[0]
        else:
            tender_key = ",".join(sorted(request_data.tenders))
        
        # Process recoData - convert to JSON string if needed
        if isinstance(request_data.recoData, str):
            recologic_json = request_data.recoData
        else:
            recologic_json = json.dumps(request_data.recoData)
        
        # Generate remark for new formula
        remark = request_data.remarks
        if not remark:
            # Extract formula names from recoData
            try:
                if isinstance(request_data.recoData, str):
                    reco_data = json.loads(request_data.recoData)
                else:
                    reco_data = request_data.recoData
                
                if isinstance(reco_data, list):
                    formula_names = [item.get("logicName", "Unknown") for item in reco_data if isinstance(item, dict)]
                elif isinstance(reco_data, dict) and "logicName" in reco_data:
                    formula_names = [reco_data.get("logicName", "Unknown")]
                else:
                    formula_names = []
                
                if formula_names:
                    remark = f"New formula(s) added: {', '.join(formula_names)}"
                else:
                    remark = "New formula added"
            except Exception as e:
                logger.warning(f"[RECOLOGICS_SAVE] Could not generate remark: {e}")
                remark = "New formula added"
        
        # Parse dates
        effective_from = None
        effective_to = None
        if request_data.effectiveFrom:
            try:
                # Try parsing various date formats
                for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y", "%d/%m/%Y"]:
                    try:
                        effective_from = datetime.strptime(request_data.effectiveFrom, fmt).date()
                        break
                    except ValueError:
                        continue
                if effective_from is None:
                    effective_from = datetime.fromisoformat(request_data.effectiveFrom.replace('Z', '+00:00')).date()
            except Exception as e:
                logger.warning(f"[RECOLOGICS_SAVE] Could not parse effectiveFrom: {e}")
        
        if request_data.effectiveTo:
            try:
                for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y", "%d/%m/%Y"]:
                    try:
                        effective_to = datetime.strptime(request_data.effectiveTo, fmt).date()
                        break
                    except ValueError:
                        continue
                if effective_to is None:
                    effective_to = datetime.fromisoformat(request_data.effectiveTo.replace('Z', '+00:00')).date()
            except Exception as e:
                logger.warning(f"[RECOLOGICS_SAVE] Could not parse effectiveTo: {e}")
        
        # Insert new record
        insert_query = text("""
            INSERT INTO reco_logics (
                tender,
                createdby,
                effectivefrom,
                effectiveto,
                effectivetype,
                recologic,
                status,
                remarks,
                created_date,
                updated_date
            ) VALUES (
                :tender,
                :createdby,
                :effectivefrom,
                :effectiveto,
                :effectivetype,
                :recologic,
                :status,
                :remarks,
                NOW(),
                NOW()
            )
        """)
        
        result = await db.execute(insert_query, {
            "tender": tender_key,
            "createdby": current_user.username,
            "effectivefrom": effective_from,
            "effectiveto": effective_to,
            "effectivetype": request_data.effectiveType,
            "recologic": recologic_json,
            "status": "ACTIVE",  # New formulas should be ACTIVE, not PROCESSED
            "remarks": remark,
        })
        
        await db.commit()
        
        # Get the inserted ID
        inserted_id = result.lastrowid
        
        logger.info(f"[RECOLOGICS_SAVE] Successfully saved recologic with id={inserted_id}")
        
        response_payload = {
            "code": 200,
            "message": "Recologic saved successfully",
            "data": {"id": inserted_id},
        }
        return response_payload
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error("[RECOLOGICS_SAVE] Error: %s", str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error saving recologics: {str(e)}",
        )


@router.post("/api/v1/recologics/update")
async def update_recologics(
    request_data: SaveRecoLogicsRequest,
    db: AsyncSession = Depends(get_main_db),
    current_user: UserDetails = Depends(get_current_user),
):
    """Update existing recologics record.
    
    Updates an existing record in reco_logics table identified by the id field.
    """
    try:
        from sqlalchemy import text
        
        logger.info(
            "[RECOLOGICS_UPDATE] Controller hit: POST /api/v1/recologics/update id=%s",
            request_data.id,
        )
        
        if not request_data.id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="id field is required for update",
            )
        
        # Check if record exists
        check_query = text("SELECT id FROM reco_logics WHERE id = :id")
        result = await db.execute(check_query, {"id": request_data.id})
        existing = result.fetchone()
        
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Recologic with id={request_data.id} not found",
            )
        
        # Process tender list if provided
        tender_key = None
        if request_data.tenders:
            if len(request_data.tenders) == 1:
                tender_key = request_data.tenders[0]
            else:
                tender_key = ",".join(sorted(request_data.tenders))
        
        # Process recoData - convert to JSON string if needed
        recologic_json = None
        if request_data.recoData is not None:
            if isinstance(request_data.recoData, str):
                recologic_json = request_data.recoData
            else:
                recologic_json = json.dumps(request_data.recoData)
        
        # Parse dates
        effective_from = None
        effective_to = None
        if request_data.effectiveFrom:
            try:
                for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y", "%d/%m/%Y"]:
                    try:
                        effective_from = datetime.strptime(request_data.effectiveFrom, fmt).date()
                        break
                    except ValueError:
                        continue
                if effective_from is None:
                    effective_from = datetime.fromisoformat(request_data.effectiveFrom.replace('Z', '+00:00')).date()
            except Exception as e:
                logger.warning(f"[RECOLOGICS_UPDATE] Could not parse effectiveFrom: {e}")
        
        if request_data.effectiveTo:
            try:
                for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y", "%d/%m/%Y"]:
                    try:
                        effective_to = datetime.strptime(request_data.effectiveTo, fmt).date()
                        break
                    except ValueError:
                        continue
                if effective_to is None:
                    effective_to = datetime.fromisoformat(request_data.effectiveTo.replace('Z', '+00:00')).date()
            except Exception as e:
                logger.warning(f"[RECOLOGICS_UPDATE] Could not parse effectiveTo: {e}")
        
        # Build update query dynamically based on provided fields
        update_fields = []
        update_params = {"id": request_data.id}
        
        if tender_key is not None:
            update_fields.append("tender = :tender")
            update_params["tender"] = tender_key
        
        if effective_from is not None:
            update_fields.append("effectivefrom = :effectivefrom")
            update_params["effectivefrom"] = effective_from
        
        if effective_to is not None:
            update_fields.append("effectiveto = :effectiveto")
            update_params["effectiveto"] = effective_to
        
        if request_data.effectiveType is not None:
            update_fields.append("effectivetype = :effectivetype")
            update_params["effectivetype"] = request_data.effectiveType
        
        if recologic_json is not None:
            update_fields.append("recologic = :recologic")
            update_params["recologic"] = recologic_json
        
        if request_data.status is not None:
            update_fields.append("status = :status")
            update_params["status"] = request_data.status
        
        if request_data.remarks is not None:
            update_fields.append("remarks = :remarks")
            update_params["remarks"] = request_data.remarks
        
        # Always update updated_date
        update_fields.append("updated_date = NOW()")
        
        if not update_fields:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No fields provided for update",
            )
        
        update_query = text(f"""
            UPDATE reco_logics
            SET {', '.join(update_fields)}
            WHERE id = :id
        """)
        
        await db.execute(update_query, update_params)
        await db.commit()
        
        logger.info(f"[RECOLOGICS_UPDATE] Successfully updated recologic with id={request_data.id}")
        
        response_payload = {
            "code": 200,
            "message": "Recologic updated successfully",
            "data": {"id": request_data.id},
        }
        return response_payload
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error("[RECOLOGICS_UPDATE] Error: %s", str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating recologics: {str(e)}",
        )


@router.get("/api/ve1/datalog/lastSynced")
async def get_last_synced(
    dsLog: str = Query(..., description="Data source log identifier (e.g., SWIGGY, ZOMATO)"),
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """
    Get last synced timestamp for a given data source
    
    Query parameters:
    - dsLog: Data source identifier (SWIGGY, ZOMATO, etc.)
    
    Returns:
    - lastReconciled: Last reconciliation date
    - lastSyncList: List of sync information for different data sources
    """
    try:
        logger.info(f"[LAST_SYNCED] API hit for dsLog: {dsLog}")
        
        # Map dsLog values to search patterns for upload_type and filename
        dslog_to_patterns = {
            "SWIGGY": ["swiggy", "SWIGGY"],
            "ZOMATO": ["zomato", "ZOMATO"],
            "DOTPE": ["dotpe", "DOTPE"],
        }
        
        # Get search patterns for the dsLog value
        search_patterns = dslog_to_patterns.get(dsLog.upper(), [dsLog])
        primary_pattern = search_patterns[0].upper()
        
        # Query upload_logs table to find the most recent successful upload
        # Use simpler pattern matching for MySQL compatibility
        pattern_search = f"%{primary_pattern}%"
        
        try:
            query = text("""
                SELECT 
                    upload_type,
                    filename,
                    created_at,
                    updated_at,
                    status
                FROM upload_logs
                WHERE status = :status_val
                AND (
                    upload_type LIKE :pattern
                    OR filename LIKE :pattern
                )
                ORDER BY created_at DESC
                LIMIT 10
            """)
            
            params = {
                "status_val": "completed",
                "pattern": pattern_search
            }
            
            logger.info(f"[LAST_SYNCED] Executing query with pattern: {pattern_search}")
            result = await db.execute(query, params)
            rows = result.fetchall()
        except Exception as db_error:
            logger.error(f"[LAST_SYNCED] Database query error: {str(db_error)}", exc_info=True)
            # If query fails, return default response instead of crashing
            rows = []
        
        logger.info(f"[LAST_SYNCED] Found {len(rows)} matching records")
        
        # Process results to extract sync information
        last_sync_list = []
        last_reconciled_date = None
        
        def format_date_to_dd_mm_yyyy(date_value):
            """Helper function to format date to DD-MM-YYYY"""
            if not date_value:
                return None
            try:
                if isinstance(date_value, datetime):
                    return date_value.strftime("%d-%m-%Y")
                elif isinstance(date_value, str):
                    # Try parsing various date formats
                    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"]:
                        try:
                            date_obj = datetime.strptime(date_value, fmt)
                            return date_obj.strftime("%d-%m-%Y")
                        except:
                            continue
                    # Fallback to isoformat parsing
                    try:
                        date_obj = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
                        return date_obj.strftime("%d-%m-%Y")
                    except:
                        return date_value
                return str(date_value)
            except Exception as e:
                logger.warning(f"[LAST_SYNCED] Error formatting date: {e}")
                return None
        
        if rows:
            # Get the most recent upload date
            most_recent_upload = rows[0]
            upload_date = most_recent_upload[2] if len(most_recent_upload) > 2 else None  # created_at
            
            if upload_date:
                last_reconciled_date = format_date_to_dd_mm_yyyy(upload_date)
        
        # Build lastSyncList from upload records
        # Group by upload_type to create sync entries
        seen_types = set()
        for row in rows[:5]:  # Limit to 5 most recent
            upload_type = str(row[0]) if len(row) > 0 and row[0] else ""
            filename = str(row[1]) if len(row) > 1 and row[1] else ""
            created_at = row[2] if len(row) > 2 else None
            status_val = str(row[4]) if len(row) > 4 and row[4] else ""
            
            # Skip if we've already added this type
            if upload_type.upper() in seen_types:
                continue
                
            seen_types.add(upload_type.upper())
            
            # Format date
            sync_date = format_date_to_dd_mm_yyyy(created_at)
            
            # Determine tender and type based on upload_type
            tender_name = upload_type.capitalize() if upload_type else dsLog.capitalize()
            data_source = upload_type.upper() if upload_type else dsLog.upper()
            
            # Map to appropriate type
            sync_type = "Delivery Performance"
            if "POS" in upload_type.upper() or "ORDER" in upload_type.upper():
                sync_type = "STLD"
            
            last_sync_list.append({
                "tender": tender_name,
                "type": sync_type,
                "dataSource": data_source,
                "lastSynced": sync_date or (last_reconciled_date or "N/A")
            })
        
        # If no records found, provide default response
        if not last_sync_list:
            logger.warning(f"[LAST_SYNCED] No records found for dsLog: {dsLog}")
            # Provide default based on dsLog
            default_date = "01-01-2024"  # Default fallback date
            last_sync_list.append({
                "tender": dsLog.capitalize(),
                "type": "Delivery Performance",
                "dataSource": dsLog.upper(),
                "lastSynced": default_date
            })
            last_reconciled_date = default_date
        
        # If last_reconciled_date is still None, use the first sync date
        if not last_reconciled_date and last_sync_list:
            last_reconciled_date = last_sync_list[0]["lastSynced"]
        
        # Ensure we have a last_reconciled_date
        if not last_reconciled_date:
            last_reconciled_date = "01-01-2024"
        
        response_data = {
            "lastReconciled": last_reconciled_date,
            "lastSyncList": last_sync_list
        }
        
        logger.info(f"[LAST_SYNCED] Response: {response_data}")
        
        return {
            "status": True,
            "data": response_data
        }
        
    except Exception as e:
        logger.error(f"[LAST_SYNCED] Error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching last synced data: {str(e)}"
        )


# ============================================
# POS vs TRM Summary Write Functions
# ============================================

async def calculate_zomato_receivables_vs_receipts(db: AsyncSession):
    """
    Calculate Zomato receivables vs receipts by matching UTR numbers with bank statements.
    Populates zomato_receivables_vs_receipts table.
    """
    logger.info("📍 [calculateZomatoReceivablesVsReceipts] Function started")
    try:
        from decimal import Decimal
        import re
        
        # Step 1: Aggregate Zomato records by UTR number
        logger.info("📍 [calculateZomatoReceivablesVsReceipts] Fetching Zomato records grouped by UTR...")
        zomato_aggregate_query = text("""
            SELECT 
                order_date,
                store_code,
                utr_number,
                utr_date,
                COUNT(order_id) as total_orders,
                SUM(COALESCE(final_amount, 0)) as total_final_amount
            FROM zomato
            WHERE utr_number IS NOT NULL
            AND utr_number != ''
            GROUP BY order_date, store_code, utr_number, utr_date
        """)
        
        logger.info(f"📊 [SQL] calculateZomatoReceivablesVsReceipts - Aggregate Query:")
        logger.info(f"   {zomato_aggregate_query}")
        
        result = await db.execute(zomato_aggregate_query)
        zomato_records = result.fetchall()
        
        logger.info(f"📍 [calculateZomatoReceivablesVsReceipts] Found {len(zomato_records)} Zomato record groups with UTR numbers")
        
        if not zomato_records or len(zomato_records) == 0:
            logger.info("📍 [calculateZomatoReceivablesVsReceipts] No Zomato records found with UTR numbers")
            return {
                "processed": 0,
                "created": 0,
                "updated": 0,
                "errors": 0
            }
        
        # Step 2: Extract UTR numbers and fetch bank statements
        utr_numbers = [record.utr_number for record in zomato_records if record.utr_number]
        logger.info(f"📍 [calculateZomatoReceivablesVsReceipts] Fetching bank statements for {len(utr_numbers)} UTR numbers...")
        
        bank_statement_map = {}
        if utr_numbers:
            # Build IN clause for UTR numbers
            utr_placeholders = ",".join([f":utr_{i}" for i in range(len(utr_numbers))])
            bank_statement_query = text(f"""
                SELECT 
                    utr,
                    deposit_amount,
                    bank,
                    account_no
                FROM bank_statement
                WHERE utr IN ({utr_placeholders})
            """)
            
            params = {f"utr_{i}": utr for i, utr in enumerate(utr_numbers)}
            
            logger.info(f"📊 [SQL] calculateZomatoReceivablesVsReceipts - Bank Statement Query:")
            logger.info(f"   {bank_statement_query}")
            
            bank_result = await db.execute(bank_statement_query, params)
            bank_statements = bank_result.fetchall()
            
            logger.info(f"📍 [calculateZomatoReceivablesVsReceipts] Found {len(bank_statements)} matching bank statements")
            
            # Create map for quick lookup
            for bs in bank_statements:
                if bs.utr:
                    bank_statement_map[bs.utr] = {
                        "deposit_amount": float(bs.deposit_amount or 0),
                        "bank": bs.bank,
                        "account_no": bs.account_no
                    }
        
        logger.info(f"📍 [calculateZomatoReceivablesVsReceipts] Created map with {len(bank_statement_map)} UTR entries")
        
        # Step 3: Process each Zomato record and create/update receivables records
        bulk_create_records = []
        total_processed = 0
        total_created = 0
        total_updated = 0
        
        for record in zomato_records:
            try:
                # Get values from aggregated record
                order_date = record.order_date
                store_code = record.store_code
                utr_number = record.utr_number
                utr_date = record.utr_date
                total_orders = int(record.total_orders or 0)
                total_final_amount = float(record.total_final_amount or 0)
                
                # Calculate final amount (use total_final_amount for now, can be enhanced with calculated_final_amount from summary)
                calculated_final_amount = total_final_amount
                
                # Find matching bank statement by UTR
                bank_match = bank_statement_map.get(utr_number) if utr_number else None
                
                if bank_match:
                    deposit_amount = bank_match["deposit_amount"]
                    bank = bank_match["bank"]
                    account_no = bank_match["account_no"]
                    amount_delta = total_final_amount - deposit_amount
                else:
                    deposit_amount = 0
                    bank = None
                    account_no = None
                    amount_delta = total_final_amount
                
                # Create unique ID for each record (sanitize special characters)
                record_id = f"ZR_{order_date}_{store_code}_{utr_number}".replace("-", "_").replace(" ", "_")
                record_id = re.sub(r'[^a-zA-Z0-9_]', '_', record_id)
                
                # Prepare record data
                record_data = {
                    "id": record_id,
                    "order_date": order_date,
                    "store_name": store_code,  # Using store_code as store_name
                    "utr_number": utr_number,
                    "utr_date": utr_date,
                    "total_orders": total_orders,
                    "final_amount": Decimal(str(total_final_amount)),
                    "calculated_final_amount": Decimal(str(calculated_final_amount)),
                    "deposit_amount": Decimal(str(deposit_amount)),
                    "amount_delta": Decimal(str(amount_delta)),
                    "bank": bank,
                    "account_no": account_no,
                    "updated_at": datetime.utcnow(),
                }
                
                bulk_create_records.append(record_data)
                total_processed += 1
                
            except Exception as error:
                logger.error(f"❌ [calculateZomatoReceivablesVsReceipts] Error processing record: {error}")
                logger.error(f"   Record: order_date={record.order_date if record else 'N/A'}, store_code={record.store_code if record else 'N/A'}, utr={record.utr_number if record else 'N/A'}")
                continue
        
        # Step 4: Perform bulk create/update (optimized - removed inefficient check query)
        if bulk_create_records:
            logger.info(f"📍 [calculateZomatoReceivablesVsReceipts] Bulk creating/updating {len(bulk_create_records)} receivables records...")
            
            # Use INSERT ... ON DUPLICATE KEY UPDATE for upsert (no need to check first)
            insert_query = text("""
                INSERT INTO zomato_receivables_vs_receipts (
                    id, order_date, store_name, utr_number, utr_date,
                    total_orders, final_amount, calculated_final_amount,
                    deposit_amount, amount_delta, bank, account_no,
                    created_at, updated_at
                ) VALUES (
                    :id, :order_date, :store_name, :utr_number, :utr_date,
                    :total_orders, :final_amount, :calculated_final_amount,
                    :deposit_amount, :amount_delta, :bank, :account_no,
                    :created_at, :updated_at
                )
                ON DUPLICATE KEY UPDATE
                    order_date = VALUES(order_date),
                    store_name = VALUES(store_name),
                    utr_number = VALUES(utr_number),
                    utr_date = VALUES(utr_date),
                    total_orders = VALUES(total_orders),
                    final_amount = VALUES(final_amount),
                    calculated_final_amount = VALUES(calculated_final_amount),
                    deposit_amount = VALUES(deposit_amount),
                    amount_delta = VALUES(amount_delta),
                    bank = VALUES(bank),
                    account_no = VALUES(account_no),
                    updated_at = VALUES(updated_at)
            """)
            
            # Process in batches to avoid memory issues and optimize commits
            BATCH_SIZE = 1000
            SUB_BATCH_SIZE = 100
            for i in range(0, len(bulk_create_records), BATCH_SIZE):
                batch = bulk_create_records[i:i + BATCH_SIZE]
                
                for j, record in enumerate(batch):
                    try:
                        # Set created_at (MySQL will ignore on UPDATE due to ON DUPLICATE KEY)
                        record_with_created = {
                            **record,
                            "created_at": datetime.utcnow()
                        }
                        
                        await db.execute(insert_query, record_with_created)
                        total_processed += 1
                        total_created += 1  # We'll adjust this later if needed, but ON DUPLICATE handles it
                        
                        # Commit every SUB_BATCH_SIZE records for better performance
                        if (j + 1) % SUB_BATCH_SIZE == 0:
                            await db.commit()
                            
                    except Exception as error:
                        logger.error(f"❌ [calculateZomatoReceivablesVsReceipts] Error inserting record {record.get('id')}: {error}")
                        continue
                
                # Final commit for any remaining records in batch
                await db.commit()
                logger.info(f"📍 [calculateZomatoReceivablesVsReceipts] Processed batch {i//BATCH_SIZE + 1}: {len(batch)} records")
        
        logger.info(f"📍 [calculateZomatoReceivablesVsReceipts] Processed {total_processed} receivables records")
        logger.info(f"📍 [calculateZomatoReceivablesVsReceipts] Created {total_created} new records")
        logger.info(f"📍 [calculateZomatoReceivablesVsReceipts] Updated {total_updated} existing records")
        logger.info("✅ [calculateZomatoReceivablesVsReceipts] Function completed successfully")
        
        return {
            "processed": total_processed,
            "created": total_created,
            "updated": total_updated,
            "errors": 0
        }
        
    except Exception as error:
        logger.error(f"❌ [calculateZomatoReceivablesVsReceipts] Error occurred: {error}")
        logger.error(f"   Error message: {str(error)}")
        import traceback
        logger.error(f"   Error stack: {traceback.format_exc()}")
        await db.rollback()
        return {
            "processed": 0,
            "created": 0,
            "updated": 0,
            "errors": 1
        }


async def generate_common_trm_table_internal(db: AsyncSession):
    """
    Generate common TRM table - Populates summarised_trm_data from TRM provider tables
    This is Step 1 of the reconciliation pipeline
    """
    logger.info("[generateCommonTRMTableInternal] Starting to generate common TRM table...")
    
    TRM_PROVIDERS = ["RZP"]
    TRM_PROVIDER_TABLE = {
        "RZP": "trm",
    }
    TRM_SUMMARY_TABLE_MAPPING = {
        "RZP": {
            "trm_uid": "uid",
            "store_name": "store_name",
            "acquirer": "acquirer",
            "payment_mode": "payment_mode",
            "card_issuer": "card_issuer",
            "card_type": "card_type",
            "card_network": "card_network",
            "card_colour": "card_colour",
            "transaction_id": "transaction_id",
            "transaction_type_detail": "type",  # Using 'type' column from trm table
            "amount": "amount",
            "currency": "currency",
            "transaction_date": "date",  # trm table uses 'date' column, not 'transaction_date'
            "rrn": "rrn",
            "cloud_ref_id": "cloud_ref_id",
        },
    }
    
    summary_data = []
    total_processed = 0
    
    try:
        for provider in TRM_PROVIDERS:
            table_name = TRM_PROVIDER_TABLE.get(provider)
            if not table_name:
                logger.warn(f"[generateCommonTRMTableInternal] No table mapping found for provider: {provider}")
                continue
            
            # Check if source table exists
            check_table_query = text(f"""
                SELECT COUNT(*) as cnt 
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = :table_name
            """)
            result = await db.execute(check_table_query, {"table_name": table_name})
            table_exists = result.scalar() > 0
            
            if not table_exists:
                logger.warn(f"[generateCommonTRMTableInternal] Source table '{table_name}' does not exist for provider '{provider}'. Skipping...")
                continue
            
            column_mapping = TRM_SUMMARY_TABLE_MAPPING.get(provider)
            if not column_mapping:
                logger.warn(f"[generateCommonTRMTableInternal] No column mapping found for provider: {provider}")
                continue
            
            # Read records from the provider's table
            logger.info(f"[generateCommonTRMTableInternal] Reading records from {table_name}...")
            read_query = text(f"SELECT * FROM {table_name}")
            result = await db.execute(read_query)
            records = result.fetchall()
            
            logger.info(f"[generateCommonTRMTableInternal] Found {len(records)} records in {table_name}")
            
            # Process each record and map columns
            for record in records:
                mapped_record = {
                    "trm_name": provider,
                }
                
                # Map each column according to the mapping
                record_dict = dict(record._mapping) if hasattr(record, '_mapping') else dict(record)
                
                for summary_column, source_column in column_mapping.items():
                    mapped_record[summary_column] = record_dict.get(source_column)
                
                summary_data.append(mapped_record)
        
        # Save all records to summarised_trm_data table in batches
        if summary_data:
            BATCH_SIZE = 1000
            columns = list(summary_data[0].keys())
            
            for i in range(0, len(summary_data), BATCH_SIZE):
                batch = summary_data[i:i + BATCH_SIZE]
                
                # Build INSERT ... ON DUPLICATE KEY UPDATE query
                columns_str = ', '.join(columns)
                placeholders = ', '.join([f':{col}' for col in columns])
                update_clause = ', '.join([f"{col} = VALUES({col})" for col in columns])
                
                insert_query = text(f"""
                    INSERT INTO summarised_trm_data (
                        {columns_str}
                    ) VALUES ({placeholders})
                    ON DUPLICATE KEY UPDATE {update_clause}
                """)
                
                # Execute each record individually
                for record in batch:
                    values_dict = {col: record.get(col) for col in columns}
                    await db.execute(insert_query, values_dict)
                
                await db.commit()
                total_processed += len(batch)
                logger.info(f"[generateCommonTRMTableInternal] Processed batch {i//BATCH_SIZE + 1}: {len(batch)} records")
        
        logger.info(f"[generateCommonTRMTableInternal] Completed: {total_processed} TRM records processed")
        return total_processed
        
    except Exception as e:
        logger.error(f"[generateCommonTRMTableInternal] Error: {str(e)}", exc_info=True)
        await db.rollback()
        raise


async def process_orders_data_internal(db: AsyncSession):
    """
    Process orders data - Populates pos_vs_trm_summary with POS data from orders table
    This is Step 2 of the reconciliation pipeline
    """
    logger.info("[processOrdersDataInternal] Starting to process orders data...")
    
    # Note: mode_name column doesn't exist in orders table, so we process all orders with valid transaction_number
    POS_AND_SUMMARY_TABLE_MAPPING = {
        "pos_transaction_id": "transaction_number",
        "pos_date": "date",
        "pos_store": "store_name",
        "pos_mode_name": None,  # mode_name column doesn't exist, will be set to NULL
        "pos_amount": "gross_amount",
    }
    
    try:
        # Check if orders table exists
        check_table_query = text("""
            SELECT COUNT(*) as cnt 
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = 'orders'
        """)
        result = await db.execute(check_table_query)
        table_exists = result.scalar() > 0
        
        if not table_exists:
            logger.warn("[processOrdersDataInternal] Orders table does not exist. Skipping orders processing...")
            return 0
        
        # Get all orders where transaction_number is valid (not NULL and not '-')
        # Note: No mode_name filter since that column doesn't exist in orders table
        orders_query = text("""
            SELECT 
                transaction_number,
                date,
                store_name,
                gross_amount
            FROM orders
            WHERE transaction_number IS NOT NULL
            AND transaction_number != '-'
            AND transaction_number != ''
        """)
        
        result = await db.execute(orders_query)
        orders = result.fetchall()
        
        logger.info(f"[processOrdersDataInternal] Found {len(orders)} orders to process (all orders with valid transaction_number)")
        
        if not orders:
            logger.warn("[processOrdersDataInternal] No orders found with valid transaction_number in orders table")
            
            # Diagnostic: Check what's actually in the orders table
            try:
                # Check total orders count
                total_count_query = text("SELECT COUNT(*) as cnt FROM orders")
                total_result = await db.execute(total_count_query)
                total_count = total_result.scalar()
                logger.warn(f"[processOrdersDataInternal] 📊 Total orders in table: {total_count}")
                
                # Check orders with transaction_number
                if total_count > 0:
                    # Sample transaction_number values
                    sample_query = text("""
                        SELECT 
                            transaction_number,
                            COUNT(*) as count
                        FROM orders
                        WHERE transaction_number IS NOT NULL
                        GROUP BY transaction_number
                        ORDER BY count DESC
                        LIMIT 10
                    """)
                    sample_result = await db.execute(sample_query)
                    sample_rows = sample_result.fetchall()
                    
                    if sample_rows:
                        logger.warn(f"[processOrdersDataInternal] 📊 Sample transaction_number values found:")
                        for row in sample_rows[:5]:
                            logger.warn(f"[processOrdersDataInternal]   - transaction_number='{row.transaction_number}': count={row.count}")
                    else:
                        logger.warn("[processOrdersDataInternal] ⚠️  No transaction_number values found in orders table")
                    
                    # Check for NULL/empty transaction_number
                    null_count_query = text("""
                        SELECT 
                            COUNT(*) as null_count,
                            COUNT(CASE WHEN transaction_number = '-' THEN 1 END) as dash_count,
                            COUNT(CASE WHEN transaction_number = '' THEN 1 END) as empty_count
                        FROM orders
                    """)
                    null_result = await db.execute(null_count_query)
                    null_row = null_result.fetchone()
                    if null_row:
                        logger.warn(f"[processOrdersDataInternal] 📊 transaction_number breakdown: NULL={null_row.null_count or 0}, '-'={null_row.dash_count or 0}, ''={null_row.empty_count or 0}")
                    
                    # Check column names (in case of mismatch)
                    columns_query = text("""
                        SELECT COLUMN_NAME 
                        FROM information_schema.COLUMNS 
                        WHERE TABLE_SCHEMA = DATABASE() 
                        AND TABLE_NAME = 'orders'
                        AND (COLUMN_NAME LIKE '%transaction%' OR COLUMN_NAME LIKE '%amount%')
                        ORDER BY COLUMN_NAME
                    """)
                    cols_result = await db.execute(columns_query)
                    columns = [row[0] for row in cols_result.fetchall()]
                    if columns:
                        logger.warn(f"[processOrdersDataInternal] 📊 Related columns in orders table: {', '.join(columns)}")
            except Exception as diag_error:
                logger.warn(f"[processOrdersDataInternal] Could not run diagnostic queries: {str(diag_error)}")
            
            return 0
        
        # Process orders and prepare bulk insert
        summary_data = []
        for order in orders:
            mapped_record = {}
            order_dict = dict(order._mapping) if hasattr(order, '_mapping') else dict(order)
            
            # Map each column according to the mapping
            for summary_column, source_column in POS_AND_SUMMARY_TABLE_MAPPING.items():
                if source_column is None:
                    # Set to NULL if source column doesn't exist (e.g., mode_name)
                    mapped_record[summary_column] = None
                else:
                    value = order_dict.get(source_column)
                    # CRITICAL: Convert negative POS amounts to positive
                    # POS amounts (gross_amount, net_sale) are stored as negative in orders table
                    if summary_column == "pos_amount" and value is not None:
                        value = abs(float(value)) if value != 0 else value
                    mapped_record[summary_column] = value
            
            summary_data.append(mapped_record)
        
        # Save all records to pos_vs_trm_summary table in batches
        if summary_data:
            BATCH_SIZE = 1000
            total_processed = 0
            columns = list(summary_data[0].keys())
            columns_str = ', '.join(columns)
            placeholders = ', '.join([f':{col}' for col in columns])
            update_clause = ', '.join([f"{col} = VALUES({col})" for col in columns])
            
            for i in range(0, len(summary_data), BATCH_SIZE):
                batch = summary_data[i:i + BATCH_SIZE]
                
                # Build INSERT ... ON DUPLICATE KEY UPDATE query
                insert_query = text(f"""
                    INSERT INTO pos_vs_trm_summary (
                        {columns_str}
                    ) VALUES ({placeholders})
                    ON DUPLICATE KEY UPDATE {update_clause}
                """)
                
                # Execute each record
                for record in batch:
                    values_dict = {col: record.get(col) for col in columns}
                    await db.execute(insert_query, values_dict)
                
                await db.commit()
                total_processed += len(batch)
                logger.info(f"[processOrdersDataInternal] Processed batch {i//BATCH_SIZE + 1}: {len(batch)} records")
            
            logger.info(f"[processOrdersDataInternal] Completed: {total_processed} orders processed")
            return total_processed
        
        return 0
        
    except Exception as e:
        logger.error(f"[processOrdersDataInternal] Error: {str(e)}", exc_info=True)
        await db.rollback()
        raise


async def process_trm_data_internal(db: AsyncSession):
    """
    Process TRM data - Merges TRM data directly from trm table into pos_vs_trm_summary
    This is Step 3 of the reconciliation pipeline
    Note: We read directly from trm table, skipping summarised_trm_data intermediate table
    """
    logger.info("[processTrmDataInternal] Starting to process TRM data from trm table...")
    
    TRM_AND_SUMMARY_TABLE_MAPPING = {
        "trm_transaction_id": "cloud_ref_id",
        "trm_date": "date",  # Read 'date' column directly from trm table
        "trm_store": "store_name",
        "acquirer": "acquirer",
        "payment_mode": "payment_mode",
        "card_issuer": "card_issuer",
        "card_type": "card_type",
        "card_network": "card_network",
        "card_colour": "card_colour",
        "trm_amount": "amount",
    }
    
    try:
        # Check if trm table exists
        check_table_query = text("""
            SELECT COUNT(*) as cnt 
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = 'trm'
        """)
        result = await db.execute(check_table_query)
        table_exists = result.scalar() > 0
        
        if not table_exists:
            logger.warn("[processTrmDataInternal] trm table does not exist. Skipping TRM data processing...")
            return {
                "totalProcessed": 0,
                "totalUpdated": 0,
                "totalCreated": 0,
            }
        
        # Get all TRM records where cloud_ref_id is not '0' or NULL
        # Read directly from trm table instead of summarised_trm_data
        trm_query = text("""
            SELECT 
                cloud_ref_id,
                date,
                store_name,
                acquirer,
                payment_mode,
                card_issuer,
                card_type,
                card_network,
                card_colour,
                amount
            FROM trm
            WHERE cloud_ref_id IS NOT NULL
            AND cloud_ref_id != '0'
        """)
        
        result = await db.execute(trm_query)
        trm_records = result.fetchall()
        
        logger.info(f"[processTrmDataInternal] Found {len(trm_records)} TRM records to process")
        
        if not trm_records:
            return {
                "totalProcessed": 0,
                "totalUpdated": 0,
                "totalCreated": 0,
            }
        
        total_updated = 0
        total_created = 0
        summary_data = []
        
        # Process each TRM record
        for trm_record in trm_records:
            trm_dict = dict(trm_record._mapping) if hasattr(trm_record, '_mapping') else dict(trm_record)
            mapped_record = {}
            
            # Map each column according to the mapping
            for summary_column, source_column in TRM_AND_SUMMARY_TABLE_MAPPING.items():
                if summary_column == "trm_date":
                    # Convert date format from DD/MM/YYYY HH:mm:ss AM/PM to MySQL datetime
                    date_str = trm_dict.get(source_column)
                    if date_str:
                        try:
                            # Try parsing DD/MM/YYYY HH:mm:ss AM/PM format
                            from datetime import datetime
                            # Common formats to try
                            date_formats = [
                                "%d/%m/%Y %I:%M:%S %p",
                                "%d/%m/%Y %H:%M:%S",
                                "%Y-%m-%d %H:%M:%S",
                                "%Y-%m-%d",
                            ]
                            
                            parsed_date = None
                            for fmt in date_formats:
                                try:
                                    parsed_date = datetime.strptime(str(date_str), fmt)
                                    break
                                except ValueError:
                                    continue
                            
                            if parsed_date:
                                mapped_record[summary_column] = parsed_date.strftime("%Y-%m-%d %H:%M:%S")
                            else:
                                logger.warn(f"[processTrmDataInternal] Invalid date format for record: {trm_dict.get('cloud_ref_id')}, date: {date_str}")
                                mapped_record[summary_column] = None
                        except Exception as e:
                            logger.warn(f"[processTrmDataInternal] Error parsing date for record: {trm_dict.get('cloud_ref_id')}, date: {date_str}, error: {str(e)}")
                            mapped_record[summary_column] = None
                    else:
                        mapped_record[summary_column] = None
                else:
                    mapped_record[summary_column] = trm_dict.get(source_column)
            
            cloud_ref_id = trm_dict.get("cloud_ref_id")
            
            # Check if record exists in pos_vs_trm_summary
            check_existing_query = text("""
                SELECT id 
                FROM pos_vs_trm_summary 
                WHERE pos_transaction_id = :pos_transaction_id
                LIMIT 1
            """)
            result = await db.execute(check_existing_query, {"pos_transaction_id": cloud_ref_id})
            existing_record = result.fetchone()
            
            if existing_record:
                # Update existing record
                update_query = text("""
                    UPDATE pos_vs_trm_summary
                    SET trm_transaction_id = :trm_transaction_id,
                        trm_date = :trm_date,
                        trm_store = :trm_store,
                        acquirer = :acquirer,
                        payment_mode = :payment_mode,
                        card_issuer = :card_issuer,
                        card_type = :card_type,
                        card_network = :card_network,
                        card_colour = :card_colour,
                        trm_amount = :trm_amount,
                        updated_at = NOW()
                    WHERE pos_transaction_id = :pos_transaction_id
                """)
                
                update_params = {
                    "trm_transaction_id": mapped_record.get("trm_transaction_id"),
                    "trm_date": mapped_record.get("trm_date"),
                    "trm_store": mapped_record.get("trm_store"),
                    "acquirer": mapped_record.get("acquirer"),
                    "payment_mode": mapped_record.get("payment_mode"),
                    "card_issuer": mapped_record.get("card_issuer"),
                    "card_type": mapped_record.get("card_type"),
                    "card_network": mapped_record.get("card_network"),
                    "card_colour": mapped_record.get("card_colour"),
                    "trm_amount": mapped_record.get("trm_amount"),
                    "pos_transaction_id": cloud_ref_id,
                }
                
                await db.execute(update_query, update_params)
                total_updated += 1
            else:
                # Add to batch for new record creation
                mapped_record["pos_transaction_id"] = cloud_ref_id
                summary_data.append(mapped_record)
        
        # Save new records to pos_vs_trm_summary table in batches
        if summary_data:
            BATCH_SIZE = 1000
            columns = list(summary_data[0].keys())
            columns_str = ', '.join(columns)
            placeholders = ', '.join([f':{col}' for col in columns])
            
            for i in range(0, len(summary_data), BATCH_SIZE):
                batch = summary_data[i:i + BATCH_SIZE]
                
                insert_query = text(f"""
                    INSERT INTO pos_vs_trm_summary (
                        {columns_str}
                    ) VALUES ({placeholders})
                """)
                
                # Execute each record
                for record in batch:
                    values_dict = {col: record.get(col) for col in columns}
                    await db.execute(insert_query, values_dict)
                
                await db.commit()
                total_created += len(batch)
                logger.info(f"[processTrmDataInternal] Created batch {i//BATCH_SIZE + 1}: {len(batch)} records")
        
        total_processed = total_updated + total_created
        
        logger.info(f"[processTrmDataInternal] Completed: {total_processed} TRM records processed ({total_updated} updated, {total_created} created)")
        
        return {
            "totalProcessed": total_processed,
            "totalUpdated": total_updated,
            "totalCreated": total_created,
        }
        
    except Exception as e:
        logger.error(f"[processTrmDataInternal] Error: {str(e)}", exc_info=True)
        await db.rollback()
        raise


async def calculate_reconciliation_status(db: AsyncSession):
    """
    Calculate reconciliation status for all records in pos_vs_trm_summary
    This is Step 4 of the reconciliation pipeline
    """
    logger.info("[calculateReconciliationStatus] Starting to calculate reconciliation status...")
    
    try:
        # Get all records from pos_vs_trm_summary
        records_query = text("""
            SELECT 
                id,
                pos_transaction_id,
                trm_transaction_id,
                pos_amount,
                trm_amount
            FROM pos_vs_trm_summary
        """)
        
        result = await db.execute(records_query)
        records = result.fetchall()
        
        logger.info(f"[calculateReconciliationStatus] Found {len(records)} records to process")
        
        if len(records) == 0:
            logger.warn("[calculateReconciliationStatus] No records found in pos_vs_trm_summary table.")
            return {
                "totalProcessed": 0,
                "totalReconciled": 0,
                "totalUnreconciled": 0,
            }
        
        total_processed = 0
        total_reconciled = 0
        total_unreconciled = 0
        
        # Process each record
        for record in records:
            record_dict = dict(record._mapping) if hasattr(record, '_mapping') else dict(record)
            
            record_id = record_dict.get("id")
            pos_transaction_id = record_dict.get("pos_transaction_id")
            trm_transaction_id = record_dict.get("trm_transaction_id")
            pos_amount = float(record_dict.get("pos_amount") or 0)
            trm_amount = float(record_dict.get("trm_amount") or 0)
            
            update_data = {}
            
            # Check 1: If pos_transaction_id is NULL
            if not pos_transaction_id:
                # Use absolute value to ensure unreconciled_amount is always positive
                update_data["unreconciled_amount"] = abs(trm_amount) if trm_amount != 0 else 0
                update_data["reconciliation_status"] = "UNRECONCILED"
                update_data["pos_reason"] = "ORDER NOT FOUND IN POS"
                update_data["trm_reason"] = "ORDER NOT FOUND IN POS"
                total_unreconciled += 1
            # Check 2: If trm_transaction_id is NULL
            elif not trm_transaction_id:
                # Use absolute value to ensure unreconciled_amount is always positive
                update_data["unreconciled_amount"] = abs(pos_amount) if pos_amount != 0 else 0
                update_data["reconciliation_status"] = "UNRECONCILED"
                update_data["pos_reason"] = "ORDER NOT FOUND IN TRM"
                update_data["trm_reason"] = "ORDER NOT FOUND IN TRM"
                total_unreconciled += 1
            # Check 3: If amounts don't match (matching Node.js strict comparison)
            elif pos_amount != trm_amount:
                # Use absolute value to ensure unreconciled_amount is always positive
                # Note: Node.js uses pos_amount directly, but we use abs() to handle negative values
                update_data["unreconciled_amount"] = abs(pos_amount) if pos_amount != 0 else abs(trm_amount)
                update_data["reconciliation_status"] = "UNRECONCILED"
                update_data["pos_reason"] = "ORDER AMOUNT NOT MATCHED"
                update_data["trm_reason"] = "ORDER AMOUNT NOT MATCHED"
                total_unreconciled += 1
            # Check 4: All conditions passed - reconciled
            else:
                # Only set reconciled_amount if positive (filter out refunds/adjustments)
                update_data["reconciled_amount"] = pos_amount if pos_amount > 0 else None
                update_data["reconciliation_status"] = "RECONCILED" if pos_amount > 0 else "UNRECONCILED"
                update_data["pos_reason"] = ""
                update_data["trm_reason"] = ""
                if pos_amount > 0:
                    total_reconciled += 1
                else:
                    # If amount is negative/zero, mark as unreconciled
                    update_data["unreconciled_amount"] = abs(pos_amount)
                    total_unreconciled += 1
            
            # Update the record
            update_query = text("""
                UPDATE pos_vs_trm_summary
                SET reconciled_amount = :reconciled_amount,
                    unreconciled_amount = :unreconciled_amount,
                    reconciliation_status = :reconciliation_status,
                    pos_reason = :pos_reason,
                    trm_reason = :trm_reason,
                    updated_at = NOW()
                WHERE id = :id
            """)
            
            update_params = {
                "id": record_id,
                "reconciled_amount": update_data.get("reconciled_amount"),
                "unreconciled_amount": update_data.get("unreconciled_amount"),
                "reconciliation_status": update_data.get("reconciliation_status"),
                "pos_reason": update_data.get("pos_reason"),
                "trm_reason": update_data.get("trm_reason"),
            }
            
            await db.execute(update_query, update_params)
            total_processed += 1
        
        await db.commit()
        
        logger.info(f"[calculateReconciliationStatus] Completed: {total_processed} records processed ({total_reconciled} reconciled, {total_unreconciled} unreconciled)")
        
        return {
            "totalProcessed": total_processed,
            "totalReconciled": total_reconciled,
            "totalUnreconciled": total_unreconciled,
        }
        
    except Exception as e:
        logger.error(f"[calculateReconciliationStatus] Error: {str(e)}", exc_info=True)
        await db.rollback()
        raise


@router.post("/generate-common-trm")
async def generate_common_trm_full_pipeline(
    request_data: Optional[GenerateCommonTrmRequest] = None,
    db: AsyncSession = Depends(get_main_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """
    Calculate POS vs TRM reconciliation - Full pipeline
    This endpoint matches Node.js /api/node/reconciliation/generate-common-trm
    It orchestrates the complete reconciliation pipeline:
    1. Check/create pos_vs_trm_summary table
    2. Process orders data (populate pos_vs_trm_summary with POS data from orders table)
    3. Process TRM data (merge TRM data directly from trm table into pos_vs_trm_summary)
    4. Calculate reconciliation status
    
    Note: We read directly from trm table, skipping the intermediate summarised_trm_data table
    """
    logger.info("[calculatePosVsTrm] Starting full reconciliation pipeline...")
    
    try:
        # Step 1: Check if table exists, create if not
        check_table_query = text("""
            SELECT COUNT(*) as cnt 
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = 'pos_vs_trm_summary'
        """)
        result = await db.execute(check_table_query)
        table_exists = result.scalar() > 0
        
        if not table_exists:
            logger.info("[calculatePosVsTrm] Table pos_vs_trm_summary does not exist. Creating table...")
            # Try to create table using the SQL from CREATE_pos_vs_trm_summary.sql
            create_table_query = text("""
                CREATE TABLE IF NOT EXISTS `pos_vs_trm_summary` (
                    `id` INT(11) NOT NULL AUTO_INCREMENT,
                    `pos_transaction_id` VARCHAR(255) DEFAULT NULL,
                    `trm_transaction_id` VARCHAR(255) DEFAULT NULL,
                    `pos_date` DATETIME DEFAULT NULL,
                    `trm_date` DATETIME DEFAULT NULL,
                    `pos_store` VARCHAR(255) DEFAULT NULL,
                    `trm_store` VARCHAR(255) DEFAULT NULL,
                    `pos_mode_name` VARCHAR(255) DEFAULT NULL,
                    `acquirer` VARCHAR(100) DEFAULT NULL,
                    `payment_mode` VARCHAR(100) DEFAULT NULL,
                    `card_issuer` VARCHAR(100) DEFAULT NULL,
                    `card_type` VARCHAR(100) DEFAULT NULL,
                    `card_network` VARCHAR(100) DEFAULT NULL,
                    `card_colour` VARCHAR(50) DEFAULT NULL,
                    `pos_amount` FLOAT DEFAULT NULL,
                    `trm_amount` FLOAT DEFAULT NULL,
                    `reconciled_amount` FLOAT DEFAULT NULL,
                    `unreconciled_amount` FLOAT DEFAULT NULL,
                    `reconciliation_status` VARCHAR(50) DEFAULT NULL,
                    `pos_reason` TEXT DEFAULT NULL,
                    `trm_reason` TEXT DEFAULT NULL,
                    `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
                    `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (`id`),
                    INDEX `ix_pos_vs_trm_summary_pos_transaction_id` (`pos_transaction_id`),
                    INDEX `ix_pos_vs_trm_summary_trm_transaction_id` (`trm_transaction_id`),
                    INDEX `ix_pos_vs_trm_summary_reconciliation_status` (`reconciliation_status`),
                    INDEX `ix_pos_vs_trm_summary_pos_store` (`pos_store`),
                    INDEX `ix_pos_vs_trm_summary_pos_date` (`pos_date`),
                    INDEX `ix_pos_vs_trm_summary_payment_mode` (`payment_mode`),
                    INDEX `ix_pos_vs_trm_summary_acquirer` (`acquirer`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
            """)
            await db.execute(create_table_query)
            await db.commit()
            logger.info("[calculatePosVsTrm] Table pos_vs_trm_summary created successfully")
        else:
            logger.info("[calculatePosVsTrm] Table pos_vs_trm_summary already exists")
        
        # Step 2: Process orders data (populate pos_vs_trm_summary with POS data)
        logger.info("[calculatePosVsTrm] Step 1: Processing orders data...")
        orders_processed = 0
        try:
            orders_processed = await process_orders_data_internal(db)
            logger.info(f"[calculatePosVsTrm] Step 1 completed: {orders_processed} orders processed")
        except Exception as error:
            logger.warn(f"[calculatePosVsTrm] Step 1 warning: {str(error)}. Continuing with other steps...")
        
        # Step 3: Process TRM data (merge TRM data directly from trm table into pos_vs_trm_summary)
        logger.info("[calculatePosVsTrm] Step 2: Processing TRM data from trm table...")
        trm_data_result = {
            "totalProcessed": 0,
            "totalUpdated": 0,
            "totalCreated": 0,
        }
        try:
            trm_data_result = await process_trm_data_internal(db)
            logger.info(f"[calculatePosVsTrm] Step 2 completed: {trm_data_result['totalProcessed']} TRM records processed ({trm_data_result['totalUpdated']} updated, {trm_data_result['totalCreated']} created)")
        except Exception as error:
            logger.warn(f"[calculatePosVsTrm] Step 2 warning: {str(error)}. Continuing with reconciliation...")
        
        # Step 4: Calculate reconciliation status
        logger.info("[calculatePosVsTrm] Step 3: Calculating reconciliation status...")
        reconciliation_result = {
            "totalProcessed": 0,
            "totalReconciled": 0,
            "totalUnreconciled": 0,
        }
        try:
            reconciliation_result = await calculate_reconciliation_status(db)
            logger.info(f"[calculatePosVsTrm] Step 3 completed: {reconciliation_result['totalProcessed']} records processed ({reconciliation_result['totalReconciled']} reconciled, {reconciliation_result['totalUnreconciled']} unreconciled)")
        except Exception as error:
            logger.warn(f"[calculatePosVsTrm] Step 3 warning: {str(error)}")
        
        logger.info("[calculatePosVsTrm] Full reconciliation pipeline completed successfully")
        
        return {
            "success": True,
            "message": "Reconciliation calculation completed successfully",
            "data": {
                "totalProcessed": reconciliation_result["totalProcessed"],
                "totalReconciled": reconciliation_result["totalReconciled"],
                "totalUnreconciled": reconciliation_result["totalUnreconciled"],
                "pipeline": {
                    "ordersProcessed": orders_processed,
                    "trmDataMerged": trm_data_result["totalProcessed"],
                },
            },
        }
        
    except Exception as e:
        logger.error(f"[calculatePosVsTrm] Error calculating POS vs TRM reconciliation: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error calculating POS vs TRM reconciliation: {str(e)}"
        )


# ============================================================================
# SUMMARY SHEET ENDPOINTS - Auto-triggered calculations and download
# ============================================================================

@router.post("/prepare-self-reco")
async def prepare_self_reco_table(
    db: AsyncSession = Depends(get_main_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """
    Prepare self-reco table with ALL zomato data (no filtering).
    This should be auto-triggered periodically or on data sync.
    Creates/updates self_reco_tender table with all calculated columns.
    """
    try:
        logger.info("=" * 80)
        logger.info("🚀 PREPARING SELF-RECO TABLE (ALL DATA)")
        logger.info("=" * 80)
        
        # Formulas and hardcoded columns from your requirements
        formulas_dict = {
            "net_amount": "bill_subtotal - mvd + merchant_pack_charge",
            "tax_paid_by_customer": "net_amount_new * 0.05",
            "commission_value": """CASE
                    WHEN net_amount_new < 400 THEN net_amount_new * 0.165
                    WHEN net_amount_new BETWEEN 400 AND 449.99 THEN net_amount_new * 0.1525
                    WHEN net_amount_new BETWEEN 450 AND 499.99 THEN net_amount_new * 0.145
                    WHEN net_amount_new BETWEEN 500 AND 549.99 THEN net_amount_new * 0.1375
                    WHEN net_amount_new BETWEEN 550 AND 599.99 THEN net_amount_new * 0.1325
                    ELSE net_amount_new * 0.1275
                    END""",
            "pg_applied_on": "net_amount_new + tax_paid_by_customer_new",
            "pgcharge": "pg_applied_on_new * 0.011",
            "taxes_zomato_fee": "(commission_value_new + pgcharge_new) * 0.18",
            "tds_amount": "(bill_subtotal + merchant_pack_charge - mvd) * 0.001",
            "final_amount": "net_amount_new - commission_value_new - pgcharge_new - taxes_zomato_fee_new - tds_amount_new"
        }
        
        hardcoded_zero_cols = [
            "credit_note_amount",
            "pro_discount_passthrough",
            "customer_discount",
            "rejection_penalty_charge",
            "user_credits_charge",
            "promo_recovery_adj",
            "icecream_handling",
            "icecream_deductions",
            "order_support_cost",
            "merchant_delivery_charge"
        ]
        
        self_reco_table = "self_reco_tender"
        zomato_table = "zomato"
        
        # Check if zomato table exists
        check_table = text(f"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{zomato_table}'")
        result = await db.execute(check_table)
        if result.scalar() == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Source table '{zomato_table}' does not exist"
            )
        
        # Drop and recreate table
        logger.info(f"📋 Creating/refreshing {self_reco_table} table...")
        await db.execute(text(f"DROP TABLE IF EXISTS {self_reco_table}"))
        await db.execute(text(f"CREATE TABLE {self_reco_table} AS SELECT * FROM {zomato_table}"))
        await db.commit()
        
        row_count_query = text(f"SELECT COUNT(*) FROM {self_reco_table}")
        row_count_result = await db.execute(row_count_query)
        row_count_value = row_count_result.scalar()  # Store scalar value immediately
        logger.info(f"✅ Table {self_reco_table} created with {row_count_value} rows")
        
        # Hardcode zero columns
        existing_cols_query = text(f"DESCRIBE {self_reco_table}")
        existing_cols_result = await db.execute(existing_cols_query)
        existing_cols = {row[0] for row in existing_cols_result.fetchall()}
        
        for col in hardcoded_zero_cols:
            if col in existing_cols:
                logger.info(f"   Setting {col} = 0")
                await db.execute(text(f"UPDATE {self_reco_table} SET {col} = 0"))
        
        await db.commit()
        
        # Add calculated _new columns
        logger.info("📊 Adding calculated columns (_new)...")
        column_list = list(formulas_dict.keys())
        formula_list = list(formulas_dict.values())
        
        for i, col_name in enumerate(column_list):
            new_col = f"{col_name}_new"
            logger.info(f"   Adding {new_col}")
            await db.execute(text(f"""
                ALTER TABLE {self_reco_table}
                ADD COLUMN {new_col} DOUBLE(10,2)
                GENERATED ALWAYS AS (
                    {formula_list[i]}
                ) STORED
            """))
        
        await db.commit()
        
        # Add delta columns
        logger.info("📊 Adding delta columns...")
        for col in column_list:
            new_col = f"{col}_new"
            delta_col = f"{col}_delta"
            logger.info(f"   Adding {delta_col}")
            await db.execute(text(f"""
                ALTER TABLE {self_reco_table}
                ADD COLUMN {delta_col} DOUBLE(10,2)
                GENERATED ALWAYS AS ({col} - {new_col}) STORED
            """))
        
        await db.commit()
        
        # Add reconciliation status and discrepancy
        logger.info("📊 Adding reconciliation status...")
        delta_reconc_columns = [f"{col}_delta" for col in column_list]
        
        # Reconciliation status
        condition = " AND ".join([f"ABS({col}) <= 0.5" for col in delta_reconc_columns])
        reconc_status_sql = f"""
            ALTER TABLE {self_reco_table}
            ADD COLUMN reconc_status TEXT
            GENERATED ALWAYS AS (
                IF({condition}, 'Reconciled!', 'Unreconciled')
            ) STORED
        """
        await db.execute(text(reconc_status_sql))
        
        # Discrepancy source
        discrepancy_cases = " ".join([
            f"WHEN ABS(COALESCE({col}, 0)) > 0.5 THEN '{col.replace('_delta', '')} mismatch'"
            for col in delta_reconc_columns
        ])
        discrepancy_sql = f"""
            ALTER TABLE {self_reco_table}
            ADD COLUMN discrepancy_source TEXT
            GENERATED ALWAYS AS (
                CASE
                    WHEN reconc_status = 'Reconciled!' THEN 'None'
                    {discrepancy_cases}
                    ELSE 'Unknown discrepancy'
                END
            ) STORED
        """
        await db.execute(text(discrepancy_sql))
        
        # Add indexes
        logger.info("📊 Creating indexes...")
        try:
            await db.execute(text(f"CREATE INDEX idx_order_date ON {self_reco_table}(order_date)"))
        except:
            pass
        try:
            await db.execute(text(f"CREATE INDEX idx_store_code ON {self_reco_table}(store_code)"))
        except:
            pass
        # OPTIMIZATION: Composite index for faster filtering by date AND store
        try:
            await db.execute(text(f"CREATE INDEX idx_order_date_store ON {self_reco_table}(order_date, store_code)"))
        except:
            pass
        
        await db.commit()
        
        logger.info("=" * 80)
        logger.info(f"✅ SELF-RECO TABLE PREPARED SUCCESSFULLY")
        logger.info("=" * 80)
        
        return {
            "success": True,
            "message": "Self-reco table prepared successfully",
            "table": self_reco_table,
            "row_count": row_count_value
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error preparing self-reco table: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error preparing self-reco table: {str(e)}"
        )


@router.post("/prepare-cross-reco")
async def prepare_cross_reco_table(
    db: AsyncSession = Depends(get_main_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """
    Prepare cross-reco table with ALL matched zomato and orders data (no filtering).
    This should be auto-triggered periodically or on data sync.
    Creates/updates zomato_order table with all calculated columns.
    """
    try:
        logger.info("=" * 80)
        logger.info("🚀 PREPARING CROSS-RECO TABLE (ALL DATA)")
        logger.info("=" * 80)
        
        zomato_table = "zomato"
        orders_table = "orders"
        reconc_table = "zomato_order"
        
        # Configuration from your requirements
        key_col_list1 = ["store_code", "order_id"]  # zomato table
        key_col_list2 = ["store_name", "instance_id"]  # orders table
        condition1 = "action IN ('sale','addition')"
        condition2 = "online_order_taker = 'ZOMATO'"
        
        # Check if source tables exist
        check_zomato = text(f"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{zomato_table}'")
        check_orders = text(f"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{orders_table}'")
        
        if (await db.execute(check_zomato)).scalar() == 0:
            raise HTTPException(status_code=404, detail=f"Table '{zomato_table}' not found")
        if (await db.execute(check_orders)).scalar() == 0:
            raise HTTPException(status_code=404, detail=f"Table '{orders_table}' not found")
        
        # Create mapping columns
        logger.info("🔑 Creating mapping columns...")
        unique_key1 = "mapping_zomato_orders"  # Fixed name for consistency
        unique_key2 = "mapping_orders_zomato"  # Fixed name for consistency
        
        # Add mapping columns if they don't exist
        # For zomato table
        try:
            await db.execute(text(f"""
                ALTER TABLE {zomato_table}
                ADD COLUMN {unique_key1} TEXT
                GENERATED ALWAYS AS (CONCAT(COALESCE({key_col_list1[0]},' '),' ',COALESCE({key_col_list1[1]},' '),' ')) STORED
            """))
        except:
            pass  # Column may already exist
        
        # For orders table
        try:
            await db.execute(text(f"""
                ALTER TABLE {orders_table}
                ADD COLUMN {unique_key2} TEXT
                GENERATED ALWAYS AS (CONCAT(COALESCE({key_col_list2[0]},' '),' ',COALESCE({key_col_list2[1]},' '),' ')) STORED
            """))
        except:
            pass
        
        await db.commit()
        
        # Create reconciliation table using UNION of LEFT and RIGHT JOINs
        logger.info(f"🏗️ Creating reconciliation table {reconc_table}...")
        
        # Get actual column names from both tables
        zomato_desc = await db.execute(text(f"DESCRIBE {zomato_table}"))
        zomato_cols = [row[0] for row in zomato_desc.fetchall()]
        
        orders_desc = await db.execute(text(f"DESCRIBE {orders_table}"))
        orders_cols = [row[0] for row in orders_desc.fetchall()]
        
        # Build column selections with aliases to avoid conflicts
        zomato_select = ", ".join([f"z.{col} as zomato_{col}" if col not in ['order_id', 'store_code', 'order_date', 'action'] else f"z.{col}" for col in zomato_cols])
        orders_select = ", ".join([f"o.{col} as pos_{col}" if col not in ['instance_id', 'store_name', 'date', 'online_order_taker'] else f"o.{col}" for col in orders_cols])
        
        join_query = f"""
            SELECT 
                {zomato_select},
                {orders_select},
                z.{unique_key1}, o.{unique_key2}
            FROM {zomato_table} z
            LEFT JOIN {orders_table} o
            ON z.{unique_key1} = o.{unique_key2}
            AND {condition1}
            WHERE {condition2}
            
            UNION
            
            SELECT 
                {zomato_select},
                {orders_select},
                z.{unique_key1}, o.{unique_key2}
            FROM {zomato_table} z
            RIGHT JOIN {orders_table} o
            ON z.{unique_key1} = o.{unique_key2}
            AND {condition1}
            WHERE {condition2}
            AND z.{unique_key1} IS NULL
        """
        
        await db.execute(text(f"DROP TABLE IF EXISTS {reconc_table}"))
        await db.execute(text(f"CREATE TABLE {reconc_table} AS {join_query}"))
        await db.commit()
        
        row_count = (await db.execute(text(f"SELECT COUNT(*) FROM {reconc_table}"))).scalar()
        logger.info(f"✅ Table {reconc_table} created with {row_count} rows")
        
        # Add calculated columns based on your formulas
        logger.info("📊 Adding calculated columns...")
        
        # Get column names from reconciliation table to build formulas correctly
        reconc_desc = await db.execute(text(f"DESCRIBE {reconc_table}"))
        reconc_cols = {row[0] for row in reconc_desc.fetchall()}
        
        # Determine column names based on what exists in the table
        # Zomato columns might be prefixed or not
        zomato_net_amount_col = None
        for col in ['net_amount', 'zomato_net_amount', 'amount']:
            if col in reconc_cols or f'zomato_{col}' in reconc_cols:
                zomato_net_amount_col = col if col in reconc_cols else f'zomato_{col}'
                break
        
        payment_col = None
        for col in ['payment', 'pos_payment']:
            if col in reconc_cols:
                payment_col = col
                break
        
        tax_col = None
        for col in ['tax_paid_by_customer', 'zomato_tax_paid_by_customer']:
            if col in reconc_cols or f'zomato_{col}' in reconc_cols:
                tax_col = col if col in reconc_cols else f'zomato_{col}'
                break
        
        commission_col = None
        for col in ['commission_value', 'zomato_commission_value']:
            if col in reconc_cols or f'zomato_{col}' in reconc_cols:
                commission_col = col if col in reconc_cols else f'zomato_{col}'
                break
        
        pg_col = None
        for col in ['pg_applied_on', 'zomato_pg_applied_on']:
            if col in reconc_cols or f'zomato_{col}' in reconc_cols:
                pg_col = col if col in reconc_cols else f'zomato_{col}'
                break
        
        pgcharge_col = None
        for col in ['pgcharge', 'zomato_pgcharge']:
            if col in reconc_cols or f'zomato_{col}' in reconc_cols:
                pgcharge_col = col if col in reconc_cols else f'zomato_{col}'
                break
        
        taxes_col = None
        for col in ['taxes_zomato_fee', 'zomato_taxes_zomato_fee']:
            if col in reconc_cols or f'zomato_{col}' in reconc_cols:
                taxes_col = col if col in reconc_cols else f'zomato_{col}'
                break
        
        tds_col = None
        for col in ['tds_amount', 'zomato_tds_amount']:
            if col in reconc_cols or f'zomato_{col}' in reconc_cols:
                tds_col = col if col in reconc_cols else f'zomato_{col}'
                break
        
        final_col = None
        for col in ['final_amount', 'zomato_final_amount']:
            if col in reconc_cols or f'zomato_{col}' in reconc_cols:
                final_col = col if col in reconc_cols else f'zomato_{col}'
                break
        
        # Use found columns or defaults
        zomato_net_amount_col = zomato_net_amount_col or 'net_amount'
        payment_col = payment_col or 'payment'
        tax_col = tax_col or 'tax_paid_by_customer'
        commission_col = commission_col or 'commission_value'
        pg_col = pg_col or 'pg_applied_on'
        pgcharge_col = pgcharge_col or 'pgcharge'
        taxes_col = taxes_col or 'taxes_zomato_fee'
        tds_col = tds_col or 'tds_amount'
        final_col = final_col or 'final_amount'
        
        # Formulas from your requirements - using detected column names
        input_col_formula_dict = {
            "zomato_netamount": f"ABS({zomato_net_amount_col})",
            "delta_net_amount": f"zomato_netamount - {payment_col}",
            "pos_tax_paid_by_customer": f"0.05 * {payment_col}",
            "delta_tax_paid_by_customer": f"{tax_col} - pos_tax_paid_by_customer",
            "pos_commission_value": f"""CASE
                    WHEN {payment_col} < 400 THEN {payment_col} * 0.165
                    WHEN {payment_col} BETWEEN 400 AND 449.99 THEN {payment_col} * 0.1525
                    WHEN {payment_col} BETWEEN 450 AND 499.99 THEN {payment_col} * 0.145
                    WHEN {payment_col} BETWEEN 500 AND 549.99 THEN {payment_col} * 0.1375
                    WHEN {payment_col} BETWEEN 550 AND 599.99 THEN {payment_col} * 0.1325
                    ELSE {payment_col} * 0.1275
                    END""",
            "delta_commission_value": f"{commission_col} - pos_commission_value",
            "pos_pg_applied_on": f"{payment_col} + pos_tax_paid_by_customer",
            "delta_pg_applied_on": f"{pg_col} - pos_pg_applied_on",
            "pos_pgcharge": "0.011 * pos_pg_applied_on",
            "delta_pgcharge": f"{pgcharge_col} - pos_pgcharge",
            "pos_taxes_zomato_fee": "0.18 * (pos_commission_value + pos_pgcharge)",
            "delta_taxes_zomato_fee": f"{taxes_col} - pos_taxes_zomato_fee",
            "pos_tds_amount": f"0.001 * {payment_col}",
            "delta_tds_amount": f"{tds_col} - pos_tds_amount",
            "pos_final_amount": f"{payment_col} - pos_commission_value - pos_pgcharge - pos_taxes_zomato_fee - pos_tds_amount",
            "delta_final_amount": f"{final_col} - pos_final_amount"
        }
        
        col_name_list = list(input_col_formula_dict.keys())
        formula_list = list(input_col_formula_dict.values())
        
        # Get existing columns
        desc_query = text(f"DESCRIBE {reconc_table}")
        desc_result = await db.execute(desc_query)
        existing_columns = {row[0] for row in desc_result.fetchall()}
        
        # Add calculated columns
        for i, col_name in enumerate(col_name_list):
            if col_name not in existing_columns:
                if col_name.startswith("delta"):
                    # Delta columns: extract left and right from formula
                    formula = formula_list[i]
                    if ' - ' in formula:
                        left, right = formula.split(' - ', 1)
                        left = left.strip()
                        right = right.strip()
                        # Handle potential column name variations
                        formula_updated = f"COALESCE({left}, 0) - COALESCE({right}, 0)"
                        col_def = f"DECIMAL(10,2) GENERATED ALWAYS AS ({formula_updated}) STORED"
                    else:
                        col_def = f"DECIMAL(10,2) GENERATED ALWAYS AS ({formula_list[i]}) STORED"
                else:
                    col_def = f"DECIMAL(10,2) GENERATED ALWAYS AS ({formula_list[i]}) STORED"
                
                logger.info(f"   Adding column {col_name}")
                try:
                    await db.execute(text(f"ALTER TABLE {reconc_table} ADD COLUMN {col_name} {col_def}"))
                except Exception as e:
                    logger.warning(f"   Failed to add column {col_name}: {e}")
        
        await db.commit()
        
        # Add reconciliation status and discrepancy
        logger.info("📊 Adding reconciliation status...")
        delta_reconc_columns = [col for col in col_name_list if col.startswith('delta')]
        base_delta_columns = ['delta_net_amount', 'delta_tax_paid_by_customer', 
                            'delta_commission_value', 'delta_pgcharge']
        
        # Reconciliation status
        condition = " AND ".join([f"ABS({col}) <= 0.5" for col in delta_reconc_columns])
        try:
            await db.execute(text(f"""
                ALTER TABLE {reconc_table}
                ADD COLUMN reconc_status TEXT
                GENERATED ALWAYS AS (
                    IF({condition}, 'Reconciled', 'Unreconciled')
                ) STORED
            """))
        except:
            pass  # Column may already exist
        
        # Discrepancy source with base columns logic
        base_mismatch_conditions = []
        
        # Generate all possible combinations of base columns
        for r in range(len(base_delta_columns), 0, -1):
            for combo in combinations(base_delta_columns, r):
                combo_conditions = [f"ABS(COALESCE({col}, 0)) > 0.5" for col in combo]
                combo_condition = " AND ".join(combo_conditions)
                combo_names = [col.replace('delta_', '').replace('_', ' ').title() for col in combo]
                combo_message = f"'{', '.join(combo_names)} mismatch'"
                base_mismatch_conditions.append(f"WHEN ({combo_condition}) THEN {combo_message}")
        
        # Individual mismatch conditions for non-base columns
        non_base_delta_columns = [col for col in delta_reconc_columns if col not in base_delta_columns]
        non_base_mismatch_conditions = []
        for col in non_base_delta_columns:
            col_name = col.replace('delta_', '').replace('_', ' ').title()
            non_base_mismatch_conditions.append(f"WHEN ABS(COALESCE({col}, 0)) > 0.5 THEN '{col_name} mismatch'")
        
        discrepancy_expr = f"""CASE
                WHEN {unique_key1} IS NULL THEN 'Missing transaction in zomato'
                WHEN {unique_key2} IS NULL THEN 'Missing transaction in orders'
                WHEN reconc_status = 'Reconciled' THEN 'None'
                {' '.join(base_mismatch_conditions)}
                {' '.join(non_base_mismatch_conditions)}
                ELSE 'Unknown discrepancy'
            END"""
        
        try:
            await db.execute(text(f"""
                ALTER TABLE {reconc_table}
                ADD COLUMN discrepancy_source TEXT
                GENERATED ALWAYS AS ({discrepancy_expr}) STORED
            """))
        except:
            pass  # Column may already exist
        
        await db.commit()
        
        # Add indexes
        logger.info("📊 Creating indexes...")
        try:
            await db.execute(text(f"CREATE INDEX idx_order_date ON {reconc_table}(order_date)"))
        except:
            pass
        try:
            await db.execute(text(f"CREATE INDEX idx_store_code ON {reconc_table}(store_code)"))
        except:
            pass
        try:
            await db.execute(text(f"CREATE INDEX idx_store_name ON {reconc_table}(store_name)"))
        except:
            pass
        try:
            await db.execute(text(f"CREATE INDEX idx_date ON {reconc_table}(date)"))
        except:
            pass
        # OPTIMIZATION: Composite indexes for faster filtering
        try:
            await db.execute(text(f"CREATE INDEX idx_order_date_store ON {reconc_table}(order_date, store_code)"))
        except:
            pass
        try:
            await db.execute(text(f"CREATE INDEX idx_date_store_name ON {reconc_table}(date, store_name)"))
        except:
            pass
        try:
            await db.execute(text(f"CREATE INDEX idx_mapping1 ON {reconc_table}({unique_key1}(255))"))
        except:
            pass
        try:
            await db.execute(text(f"CREATE INDEX idx_mapping2 ON {reconc_table}({unique_key2}(255))"))
        except:
            pass
        
        await db.commit()
        
        logger.info("=" * 80)
        logger.info(f"✅ CROSS-RECO TABLE PREPARED SUCCESSFULLY")
        logger.info("=" * 80)
        
        return {
            "success": True,
            "message": "Cross-reco table prepared successfully",
            "table": reconc_table,
            "row_count": row_count
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error preparing cross-reco table: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error preparing cross-reco table: {str(e)}"
        )


@router.post("/summary-sheet")
async def generate_summary_sheet(
    request_data: SummarySheetRequest,
    db: AsyncSession = Depends(get_main_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """
    Generate summary sheet Excel - returns immediately with generationId.
    Task runs in a completely separate thread pool, completely isolated from main event loop.
    Similar to Node.js fork() approach - ensures main application is never blocked.
    """
    try:
        import asyncio
        from app.models.main.excel_generation import ExcelGeneration, ExcelGenerationStatus
        
        # Validate required parameters
        if not request_data.startDate or not request_data.endDate or not request_data.stores:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required parameters: startDate, endDate, or stores"
            )
        
        # Parse dates - handle multiple formats
        date_formats = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y"]
        start_date_dt = None
        end_date_dt = None
        
        for fmt in date_formats:
            try:
                start_date_dt = datetime.strptime(request_data.startDate, fmt)
                break
            except ValueError:
                continue
        
        if start_date_dt is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid startDate format: {request_data.startDate}"
            )
        
        for fmt in date_formats:
            try:
                end_date_dt = datetime.strptime(request_data.endDate, fmt)
                break
            except ValueError:
                continue
        
        if end_date_dt is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid endDate format: {request_data.endDate}"
            )
        
        # Create reports directory if it doesn't exist
        reports_dir = "reports"
        os.makedirs(reports_dir, exist_ok=True)
        
        # Create initial record in MongoDB
        store_code_label = f"SummarySheet_{len(request_data.stores)} store(s)"
        generation_record = await ExcelGeneration.create(
            None,  # db parameter not needed for MongoDB
            store_code=store_code_label,
            start_date=start_date_dt,
            end_date=end_date_dt,
            status=ExcelGenerationStatus.PENDING,
            progress=0,
            message="Initializing summary sheet generation..."
        )
        
        # 🔥 KEY CHANGE: Use multiprocessing.Process for TRUE isolation
        # Similar to Node.js fork() - runs in completely separate process
        # Main application is NEVER blocked - completely isolated execution
        # This ensures the main thread stays responsive for other requests
        import multiprocessing
        from app.workers.process_worker import run_summary_sheet_generation
        
        task_params = {
            "start_date": request_data.startDate,
            "end_date": request_data.endDate,
            "store_codes": request_data.stores,
            "reports_dir": reports_dir
        }
        
        # 🔥 MULTIPROCESSING: Start completely separate Python process
        # This is equivalent to Node.js fork() - main app continues immediately
        # The child process:
        # - Has its own memory space (not shared)
        # - Has its own CPU time (doesn't block main process)
        # - Has its own database connections (separate pool)
        # - Runs completely independently
        
        # Use 'spawn' method for better isolation (default on Windows, Mac)
        # This creates a fresh Python interpreter for the child process
        try:
            ctx = multiprocessing.get_context('spawn')
            process = ctx.Process(
                target=run_summary_sheet_generation,
                args=(generation_record.id, task_params),  # generation_record.id is now a string (ObjectId)
                daemon=False  # Don't kill when main process exits (let it finish)
            )
            process.start()
            logger.info(f"✅ Started multiprocessing worker for generation {generation_record.id}, PID: {process.pid}")
            
            # Don't wait for process, but check if it started successfully
            if not process.is_alive() and process.exitcode is not None:
                logger.error(f"❌ Process {generation_record.id} exited immediately with code {process.exitcode}")
                # Update status to failed
                await ExcelGeneration.update_status(
                    None,  # db parameter not needed for MongoDB
                    generation_record.id,
                    ExcelGenerationStatus.FAILED,
                    message=f"Process failed to start (exit code: {process.exitcode})",
                    error="Process worker failed to initialize"
                )
        except Exception as process_error:
            logger.error(f"❌ Failed to start multiprocessing worker: {process_error}", exc_info=True)
            # Update status to failed
            await ExcelGeneration.update_status(
                None,  # db parameter not needed for MongoDB
                generation_record.id,
                ExcelGenerationStatus.FAILED,
                message="Failed to start background process",
                error=str(process_error)
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to start background process"
            )
        
        # 🔥 IMPORTANT: Don't wait for process - return immediately
        # The process runs in completely separate memory space and CPU
        # Main application continues handling other requests normally
        # No blocking, no freezing, completely isolated execution
        
        # Return immediately with generation ID (non-blocking)
        return {
            "success": True,
            "message": "Summary sheet generation started",
            "generationId": generation_record.id,
            "status": "PENDING"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Summary sheet generation error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server error starting summary sheet generation"
        )


@router.post("/summary-sheet-sync")
async def generate_summary_sheet_sync(
    request_data: SummarySheetRequest,
    db: AsyncSession = Depends(get_main_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """
    Generate summary sheet Excel with filtered data from pre-calculated tables (SYNC VERSION - kept for backward compatibility).
    Filters self_reco_tender and zomato_order by date and stores.
    Returns Excel file with 6 sheets.
    """
    try:
        from fastapi.responses import StreamingResponse
        from io import BytesIO
        import pandas as pd
        
        logger.info("=" * 80)
        logger.info("🚀 GENERATING SUMMARY SHEET")
        logger.info("=" * 80)
        logger.info(f"📅 Date Range: {request_data.startDate} to {request_data.endDate}")
        logger.info(f"🏪 Stores: {len(request_data.stores)} stores")
        
        # Parse dates
        date_formats = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y"]
        start_date_dt = None
        end_date_dt = None
        
        for fmt in date_formats:
            try:
                start_date_dt = datetime.strptime(request_data.startDate, fmt).date()
                break
            except ValueError:
                continue
        
        for fmt in date_formats:
            try:
                end_date_dt = datetime.strptime(request_data.endDate, fmt).date()
                break
            except ValueError:
                continue
        
        if not start_date_dt or not end_date_dt:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid date format. Expected: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS"
            )
        
        self_reco_table = "self_reco_tender"
        cross_reco_table = "zomato_order"
        
        # Check if tables exist
        check_self = text(f"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{self_reco_table}'")
        check_cross = text(f"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{cross_reco_table}'")
        
        if (await db.execute(check_self)).scalar() == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Self-reco table '{self_reco_table}' not found. Please run /prepare-self-reco first."
            )
        if (await db.execute(check_cross)).scalar() == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Cross-reco table '{cross_reco_table}' not found. Please run /prepare-cross-reco first."
            )
        
        # Get sync connection for pandas
        # Create sync engine from database config with connection pooling for performance
        from app.config.settings import settings
        from sqlalchemy import create_engine, text as sync_text
        sync_db_url = f"mysql+pymysql://{settings.main_db_user}:{settings.main_db_password}@{settings.main_db_host}:{settings.main_db_port}/{settings.main_db_name}"
        sync_engine = create_engine(
            sync_db_url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            echo=False
        )
        
        # OPTIMIZATION: Use temporary table for large store list instead of huge IN clause
        # This is much faster for 556+ stores
        # Use a regular table (not temp) to ensure it's accessible across connections
        import time
        temp_store_table = f"filter_stores_{int(time.time())}_{id(request_data)}"
        logger.info(f"🔧 Creating store filter table: {temp_store_table}...")
        
        # Create table and insert stores using sync engine
        with sync_engine.begin() as sync_conn:
            sync_conn.execute(sync_text(f"DROP TABLE IF EXISTS {temp_store_table}"))
            sync_conn.execute(sync_text(f"CREATE TABLE {temp_store_table} (store_code VARCHAR(50) PRIMARY KEY, INDEX idx_store_code (store_code))"))
            
            # Batch insert stores (faster than individual inserts)
            if len(request_data.stores) > 0:
                # Insert in batches of 1000 to avoid SQL statement size limits
                batch_size = 1000
                for i in range(0, len(request_data.stores), batch_size):
                    batch = request_data.stores[i:i+batch_size]
                    store_values = ", ".join([f"('{str(store).replace(chr(39), chr(39)+chr(39))}')" for store in batch])
                    sync_conn.execute(sync_text(f"INSERT INTO {temp_store_table} VALUES {store_values}"))
        
        # Create Excel in memory
        output = BytesIO()
        
        try:
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # Sheet 1: Self-Reconciliation
                logger.info("📊 Generating Self-Reconciliation sheet...")
                # OPTIMIZATION: Use JOIN with temp table instead of huge IN clause
                self_reco_query_str = f"""
                    SELECT s.* 
                    FROM {self_reco_table} s
                    INNER JOIN {temp_store_table} t ON s.store_code = t.store_code
                    WHERE s.order_date BETWEEN '{start_date_dt}' AND '{end_date_dt}'
                """
                df_self_reco = pd.read_sql(self_reco_query_str, sync_engine)
                
                # Order columns: base, _new, _delta
                if not df_self_reco.empty and len(df_self_reco.columns) > 0:
                    base_cols = [col for col in df_self_reco.columns if not col.endswith('_new') and not col.endswith('_delta') and col not in ['reconc_status', 'discrepancy_source']]
                    ordered_cols = []
                    for col in base_cols:
                        ordered_cols.append(col)
                        if f"{col}_new" in df_self_reco.columns:
                            ordered_cols.append(f"{col}_new")
                        if f"{col}_delta" in df_self_reco.columns:
                            ordered_cols.append(f"{col}_delta")
                    ordered_cols.extend(['reconc_status', 'discrepancy_source'])
                    df_self_reco = df_self_reco[[col for col in ordered_cols if col in df_self_reco.columns]]
                
                df_self_reco.to_excel(writer, sheet_name='SelfReconciliation', index=False)
                logger.info(f"   ✅ {len(df_self_reco)} rows")
                
                # Sheet 2: Zomato vs Order (matched records from Zomato perspective)
                logger.info("📊 Generating Zomato vs Order sheet...")
                # OPTIMIZATION: Use JOIN and limit to one date column check per row
                zomato_vs_order_query = f"""
                    SELECT z.* 
                    FROM {cross_reco_table} z
                    INNER JOIN {temp_store_table} t ON (z.store_code = t.store_code OR z.store_name = t.store_code)
                    WHERE z.mapping_zomato_orders IS NOT NULL
                    AND (z.order_date BETWEEN '{start_date_dt}' AND '{end_date_dt}' OR z.date BETWEEN '{start_date_dt}' AND '{end_date_dt}')
                """
                df_zomato_vs_order = pd.read_sql(zomato_vs_order_query, sync_engine)
                df_zomato_vs_order.to_excel(writer, sheet_name='Zomato vs Order', index=False)
                logger.info(f"   ✅ {len(df_zomato_vs_order)} rows")
                
                # Sheet 3: Order vs Zomato (matched records from Order perspective)
                logger.info("📊 Generating Order vs Zomato sheet...")
                # OPTIMIZATION: Use JOIN instead of huge IN clause
                order_vs_zomato_query = f"""
                    SELECT z.* 
                    FROM {cross_reco_table} z
                    INNER JOIN {temp_store_table} t ON (z.store_code = t.store_code OR z.store_name = t.store_code)
                    WHERE z.mapping_orders_zomato IS NOT NULL
                    AND (z.order_date BETWEEN '{start_date_dt}' AND '{end_date_dt}' OR z.date BETWEEN '{start_date_dt}' AND '{end_date_dt}')
                """
                df_order_vs_zomato = pd.read_sql(order_vs_zomato_query, sync_engine)
                df_order_vs_zomato.to_excel(writer, sheet_name='Order vs Zomato', index=False)
                logger.info(f"   ✅ {len(df_order_vs_zomato)} rows")
                
                # Sheet 4: Not found in Order
                logger.info("📊 Generating Not found in Order sheet...")
                # OPTIMIZATION: Use JOIN instead of huge IN clause
                not_found_order_query = f"""
                    SELECT z.* 
                    FROM {cross_reco_table} z
                    INNER JOIN {temp_store_table} t ON z.store_code = t.store_code
                    WHERE z.mapping_orders_zomato IS NULL
                    AND z.order_date BETWEEN '{start_date_dt}' AND '{end_date_dt}'
                """
                df_not_found_order = pd.read_sql(not_found_order_query, sync_engine)
                # Just use all columns - they may have prefixes but that's fine
                df_not_found_order.to_excel(writer, sheet_name='Not found in Order', index=False)
                logger.info(f"   ✅ {len(df_not_found_order)} rows")
                
                # Sheet 5: Not found in Zomato
                logger.info("📊 Generating Not found in Zomato sheet...")
                # OPTIMIZATION: Use JOIN instead of huge IN clause
                not_found_zomato_query = f"""
                    SELECT z.* 
                    FROM {cross_reco_table} z
                    INNER JOIN {temp_store_table} t ON z.store_name = t.store_code
                    WHERE z.mapping_zomato_orders IS NULL
                    AND z.date BETWEEN '{start_date_dt}' AND '{end_date_dt}'
                """
                df_not_found_zomato = pd.read_sql(not_found_zomato_query, sync_engine)
                # Just use all columns - they may have prefixes but that's fine
                df_not_found_zomato.to_excel(writer, sheet_name='Not found in Zomato', index=False)
                logger.info(f"   ✅ {len(df_not_found_zomato)} rows")
                
                # Sheet 6: Detailed Summary Statistics (matching existing format)
                # OPTIMIZATION: Calculate all aggregations in SQL instead of loading all rows
                logger.info("📊 Generating Detailed Summary sheet with SQL aggregations...")
                
                # First, get column names to handle prefixed columns correctly (lightweight query)
                column_check_query = f"SELECT * FROM {cross_reco_table} LIMIT 1"
                sample_df = pd.read_sql(column_check_query, sync_engine)
                all_columns = set(sample_df.columns)
                
                # Determine actual column names (handle prefixed variations)
                def find_col(variants):
                    for variant in variants:
                        if variant in all_columns:
                            return variant
                    return None
                
                pos_payment_col = find_col(['pos_payment', 'payment', 'pos_payment'])
                zomato_netamount_col = find_col(['zomato_netamount', 'zomato_net_amount', 'net_amount'])
                pos_final_col = find_col(['pos_final_amount', 'final_amount'])
                zomato_final_col = find_col(['zomato_final_amount', 'zomato_final_amount'])
                action_col = find_col(['action', 'zomato_action', 'pos_action'])
                reconc_status_col = find_col(['reconc_status', 'reconc_status'])
                discrepancy_col = find_col(['discrepancy_source', 'discrepancy_source'])
                
                # Use safe defaults if columns not found
                pos_payment_col = pos_payment_col or 'pos_payment'
                zomato_netamount_col = zomato_netamount_col or 'zomato_netamount'
                pos_final_col = pos_final_col or 'pos_final_amount'
                zomato_final_col = zomato_final_col or 'zomato_final_amount'
                action_col = action_col or 'action'
                reconc_status_col = reconc_status_col or 'reconc_status'
                discrepancy_col = discrepancy_col or 'discrepancy_source'
                
                logger.info(f"📊 Using columns: pos_payment={pos_payment_col}, zomato_netamount={zomato_netamount_col}, action={action_col}, reconc_status={reconc_status_col}")
                
                # OPTIMIZATION: Single SQL query to get all POS vs Zomato aggregations grouped by category
                # Fix: Use string literals instead of column references in CONCAT to avoid only_full_group_by error
                pos_vs_zomato_agg_query = f"""
                    SELECT 
                        'ALL' as category,
                        COUNT(*) as order_count,
                        COALESCE(SUM(COALESCE({pos_payment_col}, 0)), 0) as pos_total,
                        COALESCE(SUM(COALESCE({zomato_netamount_col}, 0)), 0) as zomato_total,
                        COALESCE(SUM(COALESCE({pos_final_col}, 0)), 0) as pos_final_total
                    FROM {cross_reco_table} z
                    INNER JOIN {temp_store_table} t ON (z.store_code = t.store_code OR z.store_name = t.store_code)
                    WHERE z.mapping_orders_zomato IS NOT NULL
                    AND (z.order_date BETWEEN '{start_date_dt}' AND '{end_date_dt}' OR z.date BETWEEN '{start_date_dt}' AND '{end_date_dt}')
                    
                    UNION ALL
                    
                    SELECT 
                        'RECONCILED_{reconc_status_col}' as category,
                        COUNT(*) as order_count,
                        COALESCE(SUM(COALESCE({pos_payment_col}, 0)), 0) as pos_total,
                        COALESCE(SUM(COALESCE({zomato_netamount_col}, 0)), 0) as zomato_total,
                        COALESCE(SUM(COALESCE({pos_final_col}, 0)), 0) as pos_final_total
                    FROM {cross_reco_table} z
                    INNER JOIN {temp_store_table} t ON (z.store_code = t.store_code OR z.store_name = t.store_code)
                    WHERE z.mapping_orders_zomato IS NOT NULL
                    AND (z.order_date BETWEEN '{start_date_dt}' AND '{end_date_dt}' OR z.date BETWEEN '{start_date_dt}' AND '{end_date_dt}')
                    AND {reconc_status_col} = 'Reconciled'
                    
                    UNION ALL
                    
                    SELECT 
                        'UNRECONCILED_{reconc_status_col}' as category,
                        COUNT(*) as order_count,
                        COALESCE(SUM(COALESCE({pos_payment_col}, 0)), 0) as pos_total,
                        COALESCE(SUM(COALESCE({zomato_netamount_col}, 0)), 0) as zomato_total,
                        COALESCE(SUM(COALESCE({pos_final_col}, 0)), 0) as pos_final_total
                    FROM {cross_reco_table} z
                    INNER JOIN {temp_store_table} t ON (z.store_code = t.store_code OR z.store_name = t.store_code)
                    WHERE z.mapping_orders_zomato IS NOT NULL
                    AND (z.order_date BETWEEN '{start_date_dt}' AND '{end_date_dt}' OR z.date BETWEEN '{start_date_dt}' AND '{end_date_dt}')
                    AND {reconc_status_col} = 'Unreconciled'
                    
                    UNION ALL
                    
                    SELECT 
                        'MISSING_Missing transaction in zomato' as category,
                        COUNT(*) as order_count,
                        COALESCE(SUM(COALESCE({pos_payment_col}, 0)), 0) as pos_total,
                        COALESCE(SUM(COALESCE({zomato_netamount_col}, 0)), 0) as zomato_total,
                        COALESCE(SUM(COALESCE({pos_final_col}, 0)), 0) as pos_final_total
                    FROM {cross_reco_table} z
                    INNER JOIN {temp_store_table} t ON (z.store_code = t.store_code OR z.store_name = t.store_code)
                    WHERE z.mapping_orders_zomato IS NOT NULL
                    AND (z.order_date BETWEEN '{start_date_dt}' AND '{end_date_dt}' OR z.date BETWEEN '{start_date_dt}' AND '{end_date_dt}')
                    AND {discrepancy_col} = 'Missing transaction in zomato'
                    
                    UNION ALL
                    
                    SELECT 
                        COALESCE({discrepancy_col}, 'NULL') as category,
                        COUNT(*) as order_count,
                        COALESCE(SUM(COALESCE({pos_payment_col}, 0)), 0) as pos_total,
                        COALESCE(SUM(COALESCE({zomato_netamount_col}, 0)), 0) as zomato_total,
                        COALESCE(SUM(COALESCE({pos_final_col}, 0)), 0) as pos_final_total
                    FROM {cross_reco_table} z
                    INNER JOIN {temp_store_table} t ON (z.store_code = t.store_code OR z.store_name = t.store_code)
                    WHERE z.mapping_orders_zomato IS NOT NULL
                    AND (z.order_date BETWEEN '{start_date_dt}' AND '{end_date_dt}' OR z.date BETWEEN '{start_date_dt}' AND '{end_date_dt}')
                    AND {discrepancy_col} IS NOT NULL
                    AND {discrepancy_col} != 'None'
                    AND {discrepancy_col} != 'Missing transaction in zomato'
                    GROUP BY {discrepancy_col}
                """
                
                # OPTIMIZATION: Single SQL query to get all Zomato vs POS aggregations grouped by category
                # Fix: Use string literals instead of column references in CONCAT to avoid only_full_group_by error
                # FIX: For "ALL" category, count ALL orders in zomato_order table (not just those with mapping_zomato_orders)
                # The zomato_order table contains all orders - both matched and unmatched
                # Previous filter (mapping_zomato_orders IS NOT NULL) was excluding 89,721 orders with NULL action
                # Fix removes that filter to get total count of ~92,617 instead of 2,896
                zomato_vs_pos_agg_query = f"""
                    SELECT 
                        'ALL' as category,
                        COUNT(*) as order_count,
                        COALESCE(SUM(COALESCE({zomato_netamount_col}, 0)), 0) as zomato_total,
                        COALESCE(SUM(COALESCE({pos_payment_col}, 0)), 0) as pos_total,
                        COALESCE(SUM(COALESCE({zomato_final_col}, 0)), 0) as zomato_final_total
                    FROM {cross_reco_table} z
                    INNER JOIN {temp_store_table} t ON (z.store_code = t.store_code OR z.store_name = t.store_code)
                    WHERE (z.order_date BETWEEN '{start_date_dt}' AND '{end_date_dt}' OR z.date BETWEEN '{start_date_dt}' AND '{end_date_dt}')
                    
                    UNION ALL
                    
                    SELECT 
                        'SALE_{action_col}' as category,
                        COUNT(*) as order_count,
                        COALESCE(SUM(COALESCE({zomato_netamount_col}, 0)), 0) as zomato_total,
                        COALESCE(SUM(COALESCE({pos_payment_col}, 0)), 0) as pos_total,
                        COALESCE(SUM(COALESCE({zomato_final_col}, 0)), 0) as zomato_final_total
                    FROM {cross_reco_table} z
                    INNER JOIN {temp_store_table} t ON (z.store_code = t.store_code OR z.store_name = t.store_code)
                    WHERE z.mapping_zomato_orders IS NOT NULL
                    AND (z.order_date BETWEEN '{start_date_dt}' AND '{end_date_dt}' OR z.date BETWEEN '{start_date_dt}' AND '{end_date_dt}')
                    AND LOWER(COALESCE({action_col}, '')) = 'sale'
                    
                    UNION ALL
                    
                    SELECT 
                        'ADDITION_{action_col}' as category,
                        COUNT(*) as order_count,
                        COALESCE(SUM(COALESCE({zomato_netamount_col}, 0)), 0) as zomato_total,
                        COALESCE(SUM(COALESCE({pos_payment_col}, 0)), 0) as pos_total,
                        COALESCE(SUM(COALESCE({zomato_final_col}, 0)), 0) as zomato_final_total
                    FROM {cross_reco_table} z
                    INNER JOIN {temp_store_table} t ON (z.store_code = t.store_code OR z.store_name = t.store_code)
                    WHERE z.mapping_zomato_orders IS NOT NULL
                    AND (z.order_date BETWEEN '{start_date_dt}' AND '{end_date_dt}' OR z.date BETWEEN '{start_date_dt}' AND '{end_date_dt}')
                    AND LOWER(COALESCE({action_col}, '')) = 'addition'
                    
                    UNION ALL
                    
                    SELECT 
                        'RECONCILED_{reconc_status_col}' as category,
                        COUNT(*) as order_count,
                        COALESCE(SUM(COALESCE({zomato_netamount_col}, 0)), 0) as zomato_total,
                        COALESCE(SUM(COALESCE({pos_payment_col}, 0)), 0) as pos_total,
                        COALESCE(SUM(COALESCE({zomato_final_col}, 0)), 0) as zomato_final_total
                    FROM {cross_reco_table} z
                    INNER JOIN {temp_store_table} t ON (z.store_code = t.store_code OR z.store_name = t.store_code)
                    WHERE z.mapping_zomato_orders IS NOT NULL
                    AND (z.order_date BETWEEN '{start_date_dt}' AND '{end_date_dt}' OR z.date BETWEEN '{start_date_dt}' AND '{end_date_dt}')
                    AND {reconc_status_col} = 'Reconciled'
                    
                    UNION ALL
                    
                    SELECT 
                        'UNRECONCILED_{reconc_status_col}' as category,
                        COUNT(*) as order_count,
                        COALESCE(SUM(COALESCE({zomato_netamount_col}, 0)), 0) as zomato_total,
                        COALESCE(SUM(COALESCE({pos_payment_col}, 0)), 0) as pos_total,
                        COALESCE(SUM(COALESCE({zomato_final_col}, 0)), 0) as zomato_final_total
                    FROM {cross_reco_table} z
                    INNER JOIN {temp_store_table} t ON (z.store_code = t.store_code OR z.store_name = t.store_code)
                    WHERE z.mapping_zomato_orders IS NOT NULL
                    AND (z.order_date BETWEEN '{start_date_dt}' AND '{end_date_dt}' OR z.date BETWEEN '{start_date_dt}' AND '{end_date_dt}')
                    AND {reconc_status_col} = 'Unreconciled'
                    
                    UNION ALL
                    
                    SELECT 
                        'MISSING_Missing transaction in orders' as category,
                        COUNT(*) as order_count,
                        COALESCE(SUM(COALESCE({zomato_netamount_col}, 0)), 0) as zomato_total,
                        COALESCE(SUM(COALESCE({pos_payment_col}, 0)), 0) as pos_total,
                        COALESCE(SUM(COALESCE({zomato_final_col}, 0)), 0) as zomato_final_total
                    FROM {cross_reco_table} z
                    INNER JOIN {temp_store_table} t ON (z.store_code = t.store_code OR z.store_name = t.store_code)
                    WHERE z.mapping_zomato_orders IS NOT NULL
                    AND (z.order_date BETWEEN '{start_date_dt}' AND '{end_date_dt}' OR z.date BETWEEN '{start_date_dt}' AND '{end_date_dt}')
                    AND {discrepancy_col} = 'Missing transaction in orders'
                    
                    UNION ALL
                    
                    SELECT 
                        COALESCE({discrepancy_col}, 'NULL') as category,
                        COUNT(*) as order_count,
                        COALESCE(SUM(COALESCE({zomato_netamount_col}, 0)), 0) as zomato_total,
                        COALESCE(SUM(COALESCE({pos_payment_col}, 0)), 0) as pos_total,
                        COALESCE(SUM(COALESCE({zomato_final_col}, 0)), 0) as zomato_final_total
                    FROM {cross_reco_table} z
                    INNER JOIN {temp_store_table} t ON (z.store_code = t.store_code OR z.store_name = t.store_code)
                    WHERE z.mapping_zomato_orders IS NOT NULL
                    AND (z.order_date BETWEEN '{start_date_dt}' AND '{end_date_dt}' OR z.date BETWEEN '{start_date_dt}' AND '{end_date_dt}')
                    AND {discrepancy_col} IS NOT NULL
                    AND {discrepancy_col} != 'None'
                    AND {discrepancy_col} != 'Missing transaction in orders'
                    GROUP BY {discrepancy_col}
                """
                
                # Execute aggregation queries (these return small result sets, not full rows)
                logger.info("   🔍 Running SQL aggregations...")
                df_pos_agg = pd.read_sql(pos_vs_zomato_agg_query, sync_engine)
                df_zomato_agg = pd.read_sql(zomato_vs_pos_agg_query, sync_engine)
                
                # NEW: Separate queries for Business Date (order_date) vs Transaction Date (date)
                # Business Date (S1+S2): Filter by order_date only
                pos_business_date_query = f"""
                    SELECT 
                        COUNT(*) as order_count,
                        COALESCE(SUM(COALESCE({pos_payment_col}, 0)), 0) as pos_total
                    FROM {cross_reco_table} z
                    INNER JOIN {temp_store_table} t ON (z.store_code = t.store_code OR z.store_name = t.store_code)
                    WHERE z.mapping_orders_zomato IS NOT NULL
                    AND z.order_date BETWEEN '{start_date_dt}' AND '{end_date_dt}'
                """
                
                # Transaction Date (S1): Filter by date only
                pos_transaction_date_query = f"""
                    SELECT 
                        COUNT(*) as order_count,
                        COALESCE(SUM(COALESCE({pos_payment_col}, 0)), 0) as pos_total
                    FROM {cross_reco_table} z
                    INNER JOIN {temp_store_table} t ON (z.store_code = t.store_code OR z.store_name = t.store_code)
                    WHERE z.mapping_orders_zomato IS NOT NULL
                    AND z.date BETWEEN '{start_date_dt}' AND '{end_date_dt}'
                """
                
                logger.info("   🔍 Running Business Date and Transaction Date aggregations...")
                df_pos_business = pd.read_sql(pos_business_date_query, sync_engine)
                df_pos_transaction = pd.read_sql(pos_transaction_date_query, sync_engine)
                
                # Extract Business Date (S1+S2) values
                pos_business_count = int(df_pos_business.iloc[0]['order_count']) if len(df_pos_business) > 0 and pd.notna(df_pos_business.iloc[0]['order_count']) else 0
                pos_business_total = float(df_pos_business.iloc[0]['pos_total']) if len(df_pos_business) > 0 and pd.notna(df_pos_business.iloc[0]['pos_total']) else 0
                
                # Extract Transaction Date (S1) values
                pos_transaction_count = int(df_pos_transaction.iloc[0]['order_count']) if len(df_pos_transaction) > 0 and pd.notna(df_pos_transaction.iloc[0]['order_count']) else 0
                pos_transaction_total = float(df_pos_transaction.iloc[0]['pos_total']) if len(df_pos_transaction) > 0 and pd.notna(df_pos_transaction.iloc[0]['pos_total']) else 0
                
                # Calculate S2 (Difference)
                pos_s2_count = pos_business_count - pos_transaction_count
                pos_s2_total = pos_business_total - pos_transaction_total
                
                logger.info(f"   📊 Business Date (S1+S2): {pos_business_count} orders, {pos_business_total} amount")
                logger.info(f"   📊 Transaction Date (S1): {pos_transaction_count} orders, {pos_transaction_total} amount")
                logger.info(f"   📊 Difference (S2): {pos_s2_count} orders, {pos_s2_total} amount")
                
                # Convert to dictionaries for easy lookup
                pos_data = {}
                for _, row in df_pos_agg.iterrows():
                    pos_data[row['category']] = {
                        'count': int(row['order_count']) if pd.notna(row['order_count']) else 0,
                        'pos_total': float(row['pos_total']) if pd.notna(row['pos_total']) else 0,
                        'zomato_total': float(row['zomato_total']) if pd.notna(row['zomato_total']) else 0,
                        'pos_final': float(row['pos_final_total']) if pd.notna(row['pos_final_total']) else 0
                    }
                
                zomato_data = {}
                for _, row in df_zomato_agg.iterrows():
                    zomato_data[row['category']] = {
                        'count': int(row['order_count']) if pd.notna(row['order_count']) else 0,
                        'zomato_total': float(row['zomato_total']) if pd.notna(row['zomato_total']) else 0,
                        'pos_total': float(row['pos_total']) if pd.notna(row['pos_total']) else 0,
                        'zomato_final': float(row['zomato_final_total']) if pd.notna(row['zomato_final_total']) else 0
                    }
                
                # Helper function to safely get aggregated values
                def get_pos_data(category, field, default=0.0):
                    data = pos_data.get(category, {})
                    return data.get(field, default)
                
                def get_zomato_data(category, field, default=0.0):
                    data = zomato_data.get(category, {})
                    return data.get(field, default)
                
                def get_discrepancy_data(discrepancy_type):
                    """Get aggregated data for a specific discrepancy type"""
                    pos_discrep = pos_data.get(discrepancy_type, {})
                    zomato_discrep = zomato_data.get(discrepancy_type, {})
                    return {
                        'pos': {
                            'count': pos_discrep.get('count', 0),
                            'pos_total': pos_discrep.get('pos_total', 0),
                            'zomato_total': pos_discrep.get('zomato_total', 0),
                            'pos_final': pos_discrep.get('pos_final', 0)
                        },
                        'zomato': {
                            'count': zomato_discrep.get('count', 0),
                            'zomato_total': zomato_discrep.get('zomato_total', 0),
                            'pos_total': zomato_discrep.get('pos_total', 0),
                            'zomato_final': zomato_discrep.get('zomato_final', 0)
                        }
                    }
                
                # Format dates for display
                start_display = start_date_dt.strftime('%b %d, %Y')
                end_display = end_date_dt.strftime('%b %d, %Y')
                
                # Build detailed summary matching existing format using aggregated SQL data
                summary_rows = []
                
                # Header rows
                summary_rows.append({'A': 'Debtor Name', 'B': 'zomato'})
                summary_rows.append({'A': 'Recon Period', 'B': f'{start_display} - {end_display}'})
                summary_rows.append({'A': '', 'B': 'No. of orders', 'C': 'POS Amount'})
                
                # POS Sale data - using separate Business Date and Transaction Date calculations
                # S1+S2: Business Date (order_date filter)
                # S1: Transaction Date (date filter)
                # S2: Difference (S1+S2 - S1)
                summary_rows.append({
                    'A': 'POS Sale as per Business Date (S1+S2)',
                    'B': pos_business_count,
                    'C': pos_business_total
                })
                summary_rows.append({
                    'A': 'POS Sale as per Transaction Date (S1)',
                    'B': pos_transaction_count,
                    'C': pos_transaction_total
                })
                summary_rows.append({
                    'A': 'Difference in POS Sale that falls in subsequent time period (S2)',
                    'B': pos_s2_count,
                    'C': pos_s2_total
                })
                
                # Section header for POS vs 3PO
                summary_rows.append({
                    'A': 'POS Sale as per Business Date (S1+S2)',
                    'B': 'As per POS data (POS vs 3PO)',
                    'C': 'As per POS data (POS vs 3PO)',
                    'D': 'As per POS data (POS vs 3PO)',
                    'E': 'As per POS data (POS vs 3PO)',
                    'F': 'As per POS data (POS vs 3PO)',
                    'G': 'As per 3PO Data (3PO vs POS)',
                    'H': 'As per 3PO Data (3PO vs POS)',
                    'I': 'As per 3PO Data (3PO vs POS)',
                    'J': 'As per 3PO Data (3PO vs POS)',
                    'K': 'As per 3PO Data (3PO vs POS)'
                })
                
                summary_rows.append({
                    'A': 'Parameters',
                    'B': 'No. of orders',
                    'C': 'POS Amount/Calculated',
                    'D': '3PO Amount/Actual',
                    'E': 'Diff. in Amount',
                    'F': 'Amount Receivable',
                    'G': 'No. of orders',
                    'H': '3PO Amount/Actual',
                    'I': 'POS Amount/Calculated',
                    'J': 'Diff. in Amount',
                    'K': 'Amount Receivable'
                })
                
                # DELIVERED row (from ALL category)
                # Use business date values (S1+S2) for POS data to match the summary section above
                zomato_all = get_zomato_data('ALL', 'count', 0)
                zomato_netamount_pos = get_pos_data('ALL', 'zomato_total', 0)
                pos_final_pos = get_pos_data('ALL', 'pos_final', 0)
                zomato_netamount_zom = get_zomato_data('ALL', 'zomato_total', 0)
                pos_payment_zom = get_zomato_data('ALL', 'pos_total', 0)
                zomato_final_zom = get_zomato_data('ALL', 'zomato_final', 0)
                
                summary_rows.append({
                    'A': 'DELIVERED',
                    'B': pos_business_count,  # Use business date count (S1+S2)
                    'C': pos_business_total,   # Use business date total (S1+S2)
                    'D': zomato_netamount_pos,
                    'E': pos_business_total - zomato_netamount_pos,  # Use business date total
                    'F': pos_final_pos,
                    'G': zomato_all,
                    'H': zomato_netamount_zom,
                    'I': pos_payment_zom,
                    'J': zomato_netamount_zom - pos_payment_zom,
                    'K': zomato_final_zom
                })
                
                # SALE row (from SALE category)
                sale_cat = 'SALE_' + action_col
                sale_data = get_zomato_data(sale_cat, 'count', 0)
                sale_zomato_total = get_zomato_data(sale_cat, 'zomato_total', 0)
                sale_pos_total = get_zomato_data(sale_cat, 'pos_total', 0)
                sale_zomato_final = get_zomato_data(sale_cat, 'zomato_final', 0)
                
                summary_rows.append({
                    'A': 'SALE',
                    'B': '', 'C': '', 'D': '', 'E': '', 'F': '',
                    'G': sale_data,
                    'H': sale_zomato_total,
                    'I': sale_pos_total,
                    'J': sale_zomato_total - sale_pos_total,
                    'K': sale_zomato_final
                })
                
                # ADDITION row (from ADDITION category)
                add_cat = 'ADDITION_' + action_col
                add_data = get_zomato_data(add_cat, 'count', 0)
                add_zomato_total = get_zomato_data(add_cat, 'zomato_total', 0)
                add_pos_total = get_zomato_data(add_cat, 'pos_total', 0)
                add_zomato_final = get_zomato_data(add_cat, 'zomato_final', 0)
                
                summary_rows.append({
                    'A': 'ADDITION',
                    'B': '', 'C': '', 'D': '', 'E': '', 'F': '',
                    'G': add_data,
                    'H': add_zomato_total,
                    'I': add_pos_total,
                    'J': add_zomato_total - add_pos_total,
                    'K': add_zomato_final
                })
                
                # Reconciled orders
                summary_rows.append({'A': 'Reconciled orders'})
                rec_cat = 'RECONCILED_' + reconc_status_col
                pos_rec_count = get_pos_data(rec_cat, 'count', 0)
                pos_rec_total = get_pos_data(rec_cat, 'pos_total', 0)
                pos_rec_zomato = get_pos_data(rec_cat, 'zomato_total', 0)
                pos_rec_final = get_pos_data(rec_cat, 'pos_final', 0)
                zomato_rec_count = get_zomato_data(rec_cat, 'count', 0)
                zomato_rec_zomato = get_zomato_data(rec_cat, 'zomato_total', 0)
                zomato_rec_pos = get_zomato_data(rec_cat, 'pos_total', 0)
                zomato_rec_final = get_zomato_data(rec_cat, 'zomato_final', 0)
                
                summary_rows.append({
                    'A': 'RECONCILED',
                    'B': pos_rec_count,
                    'C': pos_rec_total,
                    'D': pos_rec_zomato,
                    'E': pos_rec_total - pos_rec_zomato,
                    'F': pos_rec_final,
                    'G': zomato_rec_count,
                    'H': zomato_rec_zomato,
                    'I': zomato_rec_pos,
                    'J': zomato_rec_zomato - zomato_rec_pos,
                    'K': zomato_rec_final
                })
                
                # Blank row
                summary_rows.append({})
                
                # Unreconciled orders
                summary_rows.append({'A': 'Unreconciled orders'})
                unrec_cat = 'UNRECONCILED_' + reconc_status_col
                pos_unrec_count = get_pos_data(unrec_cat, 'count', 0)
                pos_unrec_total = get_pos_data(unrec_cat, 'pos_total', 0)
                pos_unrec_zomato = get_pos_data(unrec_cat, 'zomato_total', 0)
                pos_unrec_final = get_pos_data(unrec_cat, 'pos_final', 0)
                zomato_unrec_count = get_zomato_data(unrec_cat, 'count', 0)
                zomato_unrec_zomato = get_zomato_data(unrec_cat, 'zomato_total', 0)
                zomato_unrec_pos = get_zomato_data(unrec_cat, 'pos_total', 0)
                zomato_unrec_final = get_zomato_data(unrec_cat, 'zomato_final', 0)
                
                summary_rows.append({
                    'A': 'UNRECONCILED',
                    'B': pos_unrec_count,
                    'C': pos_unrec_total,
                    'D': pos_unrec_zomato,
                    'E': pos_unrec_total - pos_unrec_zomato,
                    'F': pos_unrec_final,
                    'G': zomato_unrec_count,
                    'H': zomato_unrec_zomato,
                    'I': zomato_unrec_pos,
                    'J': zomato_unrec_zomato - zomato_unrec_pos,
                    'K': zomato_unrec_final
                })
                
                # Order Not found
                summary_rows.append({'A': 'Order Not found in 3PO/POS'})
                pos_missing_cat = 'MISSING_Missing transaction in zomato'
                zomato_missing_cat = 'MISSING_Missing transaction in orders'
                pos_missing_count = get_pos_data(pos_missing_cat, 'count', 0)
                pos_missing_total = get_pos_data(pos_missing_cat, 'pos_total', 0)
                pos_missing_zomato = get_pos_data(pos_missing_cat, 'zomato_total', 0)
                pos_missing_final = get_pos_data(pos_missing_cat, 'pos_final', 0)
                zomato_missing_count = get_zomato_data(zomato_missing_cat, 'count', 0)
                zomato_missing_zomato = get_zomato_data(zomato_missing_cat, 'zomato_total', 0)
                zomato_missing_pos = get_zomato_data(zomato_missing_cat, 'pos_total', 0)
                zomato_missing_final = get_zomato_data(zomato_missing_cat, 'zomato_final', 0)
                
                summary_rows.append({
                    'A': 'ORDER NOT FOUND',
                    'B': pos_missing_count,
                    'C': pos_missing_total,
                    'D': pos_missing_zomato,
                    'E': pos_missing_total - pos_missing_zomato,
                    'F': pos_missing_final,
                    'G': zomato_missing_count,
                    'H': zomato_missing_zomato,
                    'I': zomato_missing_pos,
                    'J': zomato_missing_zomato - zomato_missing_pos,
                    'K': zomato_missing_final
                })
                
                # All mismatch categories (using aggregated data from SQL)
                discrepancy_types = [
                    'Net Amount mismatch',
                    'Tax Paid By Customer mismatch',
                    'Commission Value mismatch',
                    'Pgcharge mismatch',
                    'Net Amount, Tax Paid By Customer mismatch',
                    'Net Amount, Commission Value mismatch',
                    'Net Amount, Pgcharge mismatch',
                    'Tax Paid By Customer, Commission Value mismatch',
                    'Tax Paid By Customer, Pgcharge mismatch',
                    'Commission Value, Pgcharge mismatch',
                    'Net Amount, Tax Paid By Customer, Commission Value mismatch',
                    'Net Amount, Tax Paid By Customer, Pgcharge mismatch',
                    'Net Amount, Commission Value, Pgcharge mismatch',
                    'Tax Paid By Customer, Commission Value, Pgcharge mismatch',
                    'Net Amount, Tax Paid By Customer, Commission Value, Pgcharge mismatch',
                    'Pg Applied On mismatch',
                    'Taxes Zomato Fee mismatch',
                    'Tds Amount mismatch',
                    'Final Amount mismatch',
                    'Unknown discrepancy'
                ]
                
                for dtype in discrepancy_types:
                    discrep = get_discrepancy_data(dtype)
                    
                    summary_rows.append({
                        'A': dtype,
                        'B': discrep['pos']['count'],
                        'C': discrep['pos']['pos_total'],
                        'D': discrep['pos']['zomato_total'],
                        'E': discrep['pos']['pos_total'] - discrep['pos']['zomato_total'],
                        'F': discrep['pos']['pos_final'],
                        'G': discrep['zomato']['count'],
                        'H': discrep['zomato']['zomato_total'],
                        'I': discrep['zomato']['pos_total'],
                        'J': discrep['zomato']['zomato_total'] - discrep['zomato']['pos_total'],
                        'K': discrep['zomato']['zomato_final']
                    })
                
                # Convert to DataFrame and write
                summary_df = pd.DataFrame(summary_rows)
                summary_df.to_excel(writer, sheet_name='Summary', index=False, header=False)
                logger.info(f"   ✅ Detailed Summary generated with {len(summary_rows)} rows (using SQL aggregations - optimized)")
        finally:
            # Cleanup: Drop the temporary filter table
            try:
                with sync_engine.begin() as sync_conn:
                    sync_conn.execute(sync_text(f"DROP TABLE IF EXISTS {temp_store_table}"))
            except:
                pass
        
        output.seek(0)
        
        logger.info("=" * 80)
        logger.info(f"✅ SUMMARY SHEET GENERATED SUCCESSFULLY")
        logger.info("=" * 80)
        
        # Return as streaming response
        return StreamingResponse(
            output,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={
                "Content-Disposition": f'attachment; filename="summary_sheet_{start_date_dt}_{end_date_dt}.xlsx"'
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating summary sheet: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating summary sheet: {str(e)}"
        )


# ============================================================================
# DAILY SALES SUMMARY APIs
# ============================================================================

class PopulateDailySalesRequest(BaseModel):
    """Request model for populating daily sales summary"""
    model_config = ConfigDict(populate_by_name=True)
    
    start_date: Optional[str] = Field(None, alias="startDate")
    end_date: Optional[str] = Field(None, alias="endDate")
    # If not provided, will process last 7 days


class DashboardSalesRequest(BaseModel):
    """Request model for dashboard sales query"""
    model_config = ConfigDict(populate_by_name=True)
    
    tender: str  # Tender name (e.g., "zomato")
    start_date: str = Field(..., alias="startDate")
    end_date: str = Field(..., alias="endDate")
    stores: Optional[List[str]] = None
    cities: Optional[List[str]] = None


@router.post("/populate-daily-sales-summary")
async def populate_daily_sales_summary(
    request_data: PopulateDailySalesRequest = PopulateDailySalesRequest(),
    db: AsyncSession = Depends(get_main_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """
    Populate daily_sales_summary table from orders and zomato tables
    This calculates and stores pre-computed sales data for fast dashboard queries
    """
    try:
        from datetime import datetime, timedelta, date
        from decimal import Decimal
        
        logger.info("===========================================")
        logger.info("🚀 /populate-daily-sales-summary API IS HIT")
        logger.info("===========================================")
        
        # Determine date range
        if request_data.start_date and request_data.end_date:
            start_date = datetime.strptime(request_data.start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(request_data.end_date, "%Y-%m-%d").date()
        else:
            # Default: last 7 days
            end_date = date.today()
            start_date = end_date - timedelta(days=7)
        
        logger.info(f"📅 Processing date range: {start_date} to {end_date}")
        
        # Step 1: Get all unique store+date combinations from orders table
        logger.info("📍 Step 1: Getting unique store+date combinations from orders...")
        unique_stores_dates_query = text("""
            SELECT DISTINCT 
                date AS sales_date,
                store_name AS store_code
            FROM orders
            WHERE date BETWEEN :start_date AND :end_date
            ORDER BY date, store_name
        """)
        result = await db.execute(unique_stores_dates_query, {
            "start_date": start_date,
            "end_date": end_date
        })
        store_date_combos = result.fetchall()
        logger.info(f"📊 Found {len(store_date_combos)} unique store+date combinations")
        
        # Step 2: Get store metadata (city_id, zone) from devyani_stores
        logger.info("📍 Step 2: Fetching store metadata...")
        store_codes = list(set([row.store_code for row in store_date_combos]))
        if not store_codes:
            return {
                "success": True,
                "message": "No data found for the specified date range",
                "records_processed": 0
            }
        
        store_placeholders = ",".join([f":store_{i}" for i in range(len(store_codes))])
        store_params = {f"store_{i}": code for i, code in enumerate(store_codes)}
        
        store_metadata_query = text(f"""
            SELECT store_code, city_id, zone
            FROM devyani_stores
            WHERE store_code IN ({store_placeholders})
        """)
        store_metadata_result = await db.execute(store_metadata_query, store_params)
        store_metadata = {row.store_code: {"city_id": row.city_id, "zone": row.zone} 
                         for row in store_metadata_result.fetchall()}
        logger.info(f"📊 Found metadata for {len(store_metadata)} stores")
        
        # Step 3: Calculate In-Store Sales from orders table
        logger.info("📍 Step 3: Calculating In-Store Sales from orders table...")
        instore_placeholders = ",".join([f":store_{i}" for i in range(len(store_codes))])
        instore_query = text(f"""
            SELECT 
                date AS sales_date,
                store_name AS store_code,
                SUM(CASE WHEN UPPER(TRIM(online_order_taker)) = 'CASH' THEN COALESCE(payment, 0) ELSE 0 END) AS instore_cash,
                SUM(CASE WHEN UPPER(TRIM(online_order_taker)) = 'CARD' THEN COALESCE(payment, 0) ELSE 0 END) AS instore_card,
                SUM(CASE WHEN UPPER(TRIM(online_order_taker)) = 'UPI' THEN COALESCE(payment, 0) ELSE 0 END) AS instore_upi,
                SUM(CASE WHEN UPPER(TRIM(online_order_taker)) = 'INSTORE' THEN COALESCE(payment, 0) ELSE 0 END) AS instore_other,
                SUM(CASE WHEN UPPER(TRIM(online_order_taker)) IN ('CASH', 'CARD', 'UPI', 'INSTORE') THEN COALESCE(payment, 0) ELSE 0 END) AS instore_total,
                COUNT(CASE WHEN UPPER(TRIM(online_order_taker)) IN ('CASH', 'CARD', 'UPI', 'INSTORE') THEN 1 END) AS instore_count
            FROM orders
            WHERE date BETWEEN :start_date AND :end_date
            AND store_name IN ({instore_placeholders})
            GROUP BY date, store_name
        """)
        instore_params = {
            "start_date": start_date,
            "end_date": end_date,
            **store_params
        }
        instore_result = await db.execute(instore_query, instore_params)
        instore_data = {(row.sales_date, row.store_code): row for row in instore_result.fetchall()}
        logger.info(f"📊 Calculated In-Store Sales for {len(instore_data)} store+date combinations")
        
        # Step 4: Calculate Aggregator Sales from orders table
        logger.info("📍 Step 4: Calculating Aggregator Sales from orders table...")
        aggregator_query = text(f"""
            SELECT 
                date AS sales_date,
                store_name AS store_code,
                SUM(CASE WHEN online_order_taker = 'Zomato' THEN COALESCE(payment, 0) ELSE 0 END) AS aggregator_zomato,
                SUM(CASE WHEN online_order_taker = 'Swiggy' THEN COALESCE(payment, 0) ELSE 0 END) AS aggregator_swiggy,
                SUM(CASE WHEN online_order_taker = 'MagicPin' THEN COALESCE(payment, 0) ELSE 0 END) AS aggregator_magicpin,
                SUM(CASE WHEN online_order_taker IN ('Zomato', 'Swiggy', 'MagicPin') THEN COALESCE(payment, 0) ELSE 0 END) AS aggregator_total,
                COUNT(CASE WHEN online_order_taker IN ('Zomato', 'Swiggy', 'MagicPin') THEN 1 END) AS aggregator_count
            FROM orders
            WHERE date BETWEEN :start_date AND :end_date
            AND store_name IN ({instore_placeholders})
            GROUP BY date, store_name
        """)
        aggregator_result = await db.execute(aggregator_query, instore_params)
        aggregator_data = {(row.sales_date, row.store_code): row for row in aggregator_result.fetchall()}
        logger.info(f"📊 Calculated Aggregator Sales for {len(aggregator_data)} store+date combinations")
        
        # Step 5: Calculate Zomato formula values from zomato table
        logger.info("📍 Step 5: Calculating Zomato formula values from zomato table...")
        zomato_store_codes = [code for code in store_codes]  # Use same store codes
        zomato_placeholders = ",".join([f":store_{i}" for i in range(len(zomato_store_codes))])
        zomato_params = {f"store_{i}": code for i, code in enumerate(zomato_store_codes)}
        
        zomato_query = text(f"""
            SELECT 
                order_date AS sales_date,
                store_code,
                SUM(
                    CASE 
                        WHEN net_amount IS NOT NULL AND net_amount > 0 THEN net_amount
                        ELSE (bill_subtotal - COALESCE(mvd, 0) + COALESCE(merchant_pack_charge, 0))
                    END
                ) AS zomato_net_amount,
                SUM(bill_subtotal - COALESCE(mvd, 0) + COALESCE(merchant_pack_charge, 0)) AS zomato_calculated_amount,
                SUM(COALESCE(final_amount, 0)) AS zomato_final_amount,
                COUNT(*) AS zomato_order_count
            FROM zomato
            WHERE order_date BETWEEN :start_date AND :end_date
            AND store_code IN ({zomato_placeholders})
            AND action IN ('sale', 'addition')
            GROUP BY order_date, store_code
        """)
        zomato_params = {
            "start_date": start_date,
            "end_date": end_date,
            **zomato_params
        }
        zomato_result = await db.execute(zomato_query, zomato_params)
        zomato_data = {(row.sales_date, row.store_code): row for row in zomato_result.fetchall()}
        logger.info(f"📊 Calculated Zomato values for {len(zomato_data)} store+date combinations")
        
        # Step 6: Merge and insert/update daily_sales_summary
        logger.info("📍 Step 6: Merging data and inserting/updating daily_sales_summary...")
        records_processed = 0
        
        for sales_date, store_code in store_date_combos:
            # Get data for this store+date combination
            instore_row = instore_data.get((sales_date, store_code))
            aggregator_row = aggregator_data.get((sales_date, store_code))
            zomato_row = zomato_data.get((sales_date, store_code))
            store_info = store_metadata.get(store_code, {"city_id": None, "zone": None})
            
            # Calculate totals
            instore_total = Decimal(str(instore_row.instore_total)) if instore_row else Decimal('0')
            aggregator_total = Decimal(str(aggregator_row.aggregator_total)) if aggregator_row else Decimal('0')
            total_sales = instore_total + aggregator_total
            
            instore_count = instore_row.instore_count if instore_row else 0
            aggregator_count = aggregator_row.aggregator_count if aggregator_row else 0
            total_order_count = instore_count + aggregator_count
            
            # Insert or update
            upsert_query = text("""
                INSERT INTO daily_sales_summary (
                    sales_date, store_code, city_id, zone,
                    instore_cash, instore_card, instore_upi, instore_other, instore_total, instore_count,
                    aggregator_zomato, aggregator_swiggy, aggregator_magicpin, aggregator_total, aggregator_count,
                    zomato_net_amount, zomato_calculated_amount, zomato_final_amount, zomato_order_count,
                    total_sales, total_order_count,
                    created_at, updated_at
                ) VALUES (
                    :sales_date, :store_code, :city_id, :zone,
                    :instore_cash, :instore_card, :instore_upi, :instore_other, :instore_total, :instore_count,
                    :aggregator_zomato, :aggregator_swiggy, :aggregator_magicpin, :aggregator_total, :aggregator_count,
                    :zomato_net_amount, :zomato_calculated_amount, :zomato_final_amount, :zomato_order_count,
                    :total_sales, :total_order_count,
                    NOW(), NOW()
                )
                ON DUPLICATE KEY UPDATE
                    city_id = VALUES(city_id),
                    zone = VALUES(zone),
                    instore_cash = VALUES(instore_cash),
                    instore_card = VALUES(instore_card),
                    instore_upi = VALUES(instore_upi),
                    instore_other = VALUES(instore_other),
                    instore_total = VALUES(instore_total),
                    instore_count = VALUES(instore_count),
                    aggregator_zomato = VALUES(aggregator_zomato),
                    aggregator_swiggy = VALUES(aggregator_swiggy),
                    aggregator_magicpin = VALUES(aggregator_magicpin),
                    aggregator_total = VALUES(aggregator_total),
                    aggregator_count = VALUES(aggregator_count),
                    zomato_net_amount = VALUES(zomato_net_amount),
                    zomato_calculated_amount = VALUES(zomato_calculated_amount),
                    zomato_final_amount = VALUES(zomato_final_amount),
                    zomato_order_count = VALUES(zomato_order_count),
                    total_sales = VALUES(total_sales),
                    total_order_count = VALUES(total_order_count),
                    updated_at = NOW()
            """)
            
            await db.execute(upsert_query, {
                "sales_date": sales_date,
                "store_code": store_code,
                "city_id": store_info["city_id"],
                "zone": store_info["zone"],
                "instore_cash": Decimal(str(instore_row.instore_cash)) if instore_row else Decimal('0'),
                "instore_card": Decimal(str(instore_row.instore_card)) if instore_row else Decimal('0'),
                "instore_upi": Decimal(str(instore_row.instore_upi)) if instore_row else Decimal('0'),
                "instore_other": Decimal(str(instore_row.instore_other)) if instore_row else Decimal('0'),
                "instore_total": instore_total,
                "instore_count": instore_count,
                "aggregator_zomato": Decimal(str(aggregator_row.aggregator_zomato)) if aggregator_row else Decimal('0'),
                "aggregator_swiggy": Decimal(str(aggregator_row.aggregator_swiggy)) if aggregator_row else Decimal('0'),
                "aggregator_magicpin": Decimal(str(aggregator_row.aggregator_magicpin)) if aggregator_row else Decimal('0'),
                "aggregator_total": aggregator_total,
                "aggregator_count": aggregator_count,
                "zomato_net_amount": Decimal(str(zomato_row.zomato_net_amount)) if zomato_row else Decimal('0'),
                "zomato_calculated_amount": Decimal(str(zomato_row.zomato_calculated_amount)) if zomato_row else Decimal('0'),
                "zomato_final_amount": Decimal(str(zomato_row.zomato_final_amount)) if zomato_row else Decimal('0'),
                "zomato_order_count": zomato_row.zomato_order_count if zomato_row else 0,
                "total_sales": total_sales,
                "total_order_count": total_order_count
            })
            records_processed += 1
        
        await db.commit()
        logger.info(f"✅ Successfully processed {records_processed} records")
        
        return {
            "success": True,
            "message": f"Successfully populated daily_sales_summary for {start_date} to {end_date}",
            "records_processed": records_processed,
            "date_range": {
                "start_date": str(start_date),
                "end_date": str(end_date)
            }
        }
        
    except Exception as e:
        await db.rollback()
        logger.error(f"❌ Error populating daily_sales_summary: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error populating daily sales summary: {str(e)}"
        )


@router.post("/dashboard-sales")
async def get_dashboard_sales(
    request_data: DashboardSalesRequest,
    db: AsyncSession = Depends(get_main_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """
    Get dashboard sales data from pre-calculated daily_sales_summary table
    Fast query with filtering by date range, cities, and stores
    """
    try:
        from datetime import datetime
        
        logger.info("===========================================")
        logger.info("🚀 /dashboard-sales API IS HIT")
        logger.info("===========================================")
        
        start_date_str = request_data.start_date
        end_date_str = request_data.end_date
        stores = request_data.stores or []
        cities = request_data.cities or []
        
        # Parse dates
        start_date = datetime.strptime(start_date_str.split()[0], "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date_str.split()[0], "%Y-%m-%d").date()
        
        logger.info(f"📅 Querying date range: {start_date} to {end_date}")
        logger.info(f"🏪 Stores filter: {len(stores)} stores")
        logger.info(f"🏙️ Cities filter: {len(cities)} cities")
        
        # Build query with filters
        query_parts = ["SELECT"]
        query_parts.append("""
            SUM(instore_total) AS instoreSales,
            SUM(aggregator_total) AS aggregatorSales,
            SUM(total_sales) AS totalSales,
            SUM(instore_count + aggregator_count) AS totalOrders,
            SUM(instore_cash) AS instoreCash,
            SUM(instore_card) AS instoreCard,
            SUM(instore_upi) AS instoreUpi,
            SUM(instore_other) AS instoreOther,
            SUM(aggregator_zomato) AS aggregatorZomato,
            SUM(aggregator_swiggy) AS aggregatorSwiggy,
            SUM(aggregator_magicpin) AS aggregatorMagicpin,
            SUM(zomato_net_amount) AS zomatoNetAmount,
            SUM(zomato_calculated_amount) AS zomatoCalculatedAmount
        """)
        query_parts.append("FROM daily_sales_summary")
        query_parts.append("WHERE sales_date BETWEEN :start_date AND :end_date")
        
        params = {
            "start_date": start_date,
            "end_date": end_date
        }
        
        # Add store filter
        if stores:
            store_placeholders = ",".join([f":store_{i}" for i in range(len(stores))])
            query_parts.append(f"AND store_code IN ({store_placeholders})")
            for i, store in enumerate(stores):
                params[f"store_{i}"] = store
        
        # Add city filter
        if cities:
            city_placeholders = ",".join([f":city_{i}" for i in range(len(cities))])
            query_parts.append(f"AND city_id IN ({city_placeholders})")
            for i, city in enumerate(cities):
                params[f"city_{i}"] = str(city)  # Ensure string
        
        query_str = " ".join(query_parts)
        logger.info(f"📝 Executing query: {query_str[:200]}...")
        
        query = text(query_str)
        result = await db.execute(query, params)
        row = result.fetchone()
        
        if not row:
            return {
                "success": True,
                "data": {
                    "totalSales": 0,
                    "instoreSales": 0,
                    "aggregatorSales": 0,
                    "totalOrders": 0,
                    "breakdown": {
                        "instore": {
                            "cash": 0,
                            "card": 0,
                            "upi": 0,
                            "other": 0
                        },
                        "aggregator": {
                            "zomato": 0,
                            "swiggy": 0,
                            "magicpin": 0
                        },
                        "zomato": {
                            "netAmount": 0,
                            "calculatedAmount": 0
                        }
                    }
                }
            }
        
        # Format response
        response_data = {
            "success": True,
            "data": {
                "totalSales": float(row.totalSales or 0),
                "instoreSales": float(row.instoreSales or 0),
                "aggregatorSales": float(row.aggregatorSales or 0),
                "totalOrders": int(row.totalOrders or 0),
                "breakdown": {
                    "instore": {
                        "cash": float(row.instoreCash or 0),
                        "card": float(row.instoreCard or 0),
                        "upi": float(row.instoreUpi or 0),
                        "other": float(row.instoreOther or 0)
                    },
                    "aggregator": {
                        "zomato": float(row.aggregatorZomato or 0),
                        "swiggy": float(row.aggregatorSwiggy or 0),
                        "magicpin": float(row.aggregatorMagicpin or 0)
                    },
                    "zomato": {
                        "netAmount": float(row.zomatoNetAmount or 0),
                        "calculatedAmount": float(row.zomatoCalculatedAmount or 0)
                    }
                }
            }
        }
        
        logger.info(f"✅ Query successful. Total Sales: ₹{response_data['data']['totalSales']:,.2f}")
        
        return response_data
        
    except Exception as e:
        logger.error(f"❌ Error querying dashboard sales: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error querying dashboard sales: {str(e)}"
        )


@router.post("/new-dashboard")
async def get_new_dashboard(
    request_data: DashboardSalesRequest,
    db: AsyncSession = Depends(get_main_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """
    Get new dashboard data - aggregates total_sales from tender_dashboard collection
    """
    try:
        from app.services.mongodb_service import mongodb_service
        
        logger.info("===========================================")
        logger.info("🚀 /new-dashboard API IS HIT")
        logger.info("===========================================")
        logger.info(f"Request: tender={request_data.tender}, startDate={request_data.start_date}, endDate={request_data.end_date}")
        
        # Validate request data
        if not request_data.tender:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Tender is required"
            )
        
        if not request_data.start_date or not request_data.end_date:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Start date and end date are required"
            )
        
        # Check MongoDB connection
        if not mongodb_service.is_connected() or mongodb_service.db is None:
            logger.error("❌ MongoDB not connected")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="MongoDB service is not available"
            )
        
        # Build dashboard collection name: {tender}_dashboard
        tender_lower = request_data.tender.lower()
        dashboard_collection_name = f"{tender_lower}_dashboard"
        
        logger.info(f"📊 Querying collection: '{dashboard_collection_name}'")
        
        # Get collection
        dashboard_collection = mongodb_service.db[dashboard_collection_name]
        
        # Check if collection exists
        existing_collections = mongodb_service.db.list_collection_names()
        if dashboard_collection_name not in existing_collections:
            logger.warning(f"⚠️ Collection '{dashboard_collection_name}' does not exist")
            return {
                "success": True,
                "message": f"Collection '{dashboard_collection_name}' not found",
                "data": {
                    "total_sales": 0,
                    "tender": request_data.tender,
                    "start_date": request_data.start_date,
                    "end_date": request_data.end_date,
                    "document_count": 0
                }
            }
        
        # Parse date strings to datetime objects
        try:
            start_datetime = datetime.strptime(request_data.start_date, "%Y-%m-%d %H:%M:%S")
            end_datetime = datetime.strptime(request_data.end_date, "%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            logger.error(f"❌ Invalid date format: {e}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid date format. Expected: 'YYYY-MM-DD HH:MM:SS'. Error: {str(e)}"
            )
        
        # MongoDB aggregation pipeline to sum total_sales
        pipeline = [
            {
                "$match": {
                    "order_date": {
                        "$gte": start_datetime,
                        "$lte": end_datetime
                    }
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total_sales": {
                        "$sum": {
                            "$ifNull": ["$total_sales", 0]  # Treat null/undefined as 0
                        }
                    },
                    "document_count": {"$sum": 1}
                }
            }
        ]
        
        logger.info(f"📊 Executing aggregation pipeline on '{dashboard_collection_name}'")
        logger.info(f"Date range: {start_datetime} to {end_datetime}")
        
        # Execute aggregation
        result = list(dashboard_collection.aggregate(pipeline))
        
        # Extract results
        if result and len(result) > 0:
            total_sales = result[0].get("total_sales", 0)
            document_count = result[0].get("document_count", 0)
        else:
            # No documents in date range
            total_sales = 0
            document_count = 0
        
        logger.info(f"✅ Aggregation complete: total_sales={total_sales}, document_count={document_count}")
        
        # Build response
        response_data = {
            "success": True,
            "message": "Dashboard data fetched successfully",
            "data": {
                "total_sales": float(total_sales) if total_sales else 0.0,
                "tender": request_data.tender,
                "start_date": request_data.start_date,
                "end_date": request_data.end_date,
                "document_count": document_count
            }
        }
        
        logger.info(f"✅ New dashboard query successful")
        
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error in new dashboard: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error in new dashboard: {str(e)}"
        )
