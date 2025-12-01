"""
Daily Sales Summary Scheduler
Runs every 10 seconds to populate daily_sales_summary table
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, date
from typing import Optional

from app.config import database as db_config

logger = logging.getLogger(__name__)

_scheduler_task: Optional[asyncio.Task] = None
_stop_event: Optional[asyncio.Event] = None
_scheduler_lock = asyncio.Lock()
_initialized = False

# Allow overriding via env var, default 10 seconds
SCHEDULER_INTERVAL_SECONDS = int(os.getenv("DAILY_SALES_SCHEDULER_INTERVAL_SECONDS", "10"))


async def _populate_daily_sales_summary_internal():
    """
    Internal function to populate daily_sales_summary table
    Processes last 7 days of data
    """
    try:
        if not db_config.main_session_factory:
            await db_config.create_engines()
        
        async with db_config.main_session_factory() as session:
            from sqlalchemy.sql import text
            from decimal import Decimal
            
            # Find the actual date range of data in orders table
            # Process last 90 days OR the actual date range, whichever is wider
            today = date.today()
            default_start = today - timedelta(days=90)
            
            # Find min/max dates in orders table
            date_range_query = text("""
                SELECT 
                    MIN(date) AS min_date,
                    MAX(date) AS max_date
                FROM orders
            """)
            date_range_result = await session.execute(date_range_query)
            date_range_row = date_range_result.fetchone()
            
            if date_range_row and date_range_row.min_date and date_range_row.max_date:
                # Use the actual date range from orders table
                actual_start = date_range_row.min_date if isinstance(date_range_row.min_date, date) else date_range_row.min_date.date()
                actual_end = date_range_row.max_date if isinstance(date_range_row.max_date, date) else date_range_row.max_date.date()
                
                # If data is recent (within last 90 days), process last 90 days
                # If data is old, process the actual date range
                if actual_end >= default_start:
                    # Recent data: process last 90 days
                    start_date = default_start
                    end_date = min(today, actual_end)
                else:
                    # Old data: process the entire range
                    start_date = actual_start
                    end_date = actual_end
                
                logger.info(f"[DAILY_SALES_SCHEDULER] Data range in orders table: {actual_start} to {actual_end}")
                logger.info(f"[DAILY_SALES_SCHEDULER] Processing date range: {start_date} to {end_date}")
            else:
                # No data in orders table, use default range
                end_date = today
                start_date = default_start
                logger.info(f"[DAILY_SALES_SCHEDULER] No data in orders table, using default range: {start_date} to {end_date}")
            
            # Step 1: Get unique store+date combinations
            unique_stores_dates_query = text("""
                SELECT DISTINCT 
                    date AS sales_date,
                    store_name AS store_code
                FROM orders
                WHERE date BETWEEN :start_date AND :end_date
                ORDER BY date, store_name
            """)
            result = await session.execute(unique_stores_dates_query, {
                "start_date": start_date,
                "end_date": end_date
            })
            store_date_combos = result.fetchall()
            
            if not store_date_combos:
                logger.info(f"[DAILY_SALES_SCHEDULER] No data found for date range {start_date} to {end_date}")
                return {"success": True, "records_processed": 0}
            
            logger.info(f"[DAILY_SALES_SCHEDULER] Found {len(store_date_combos)} unique store+date combinations")
            
            # Step 2: Get store metadata
            store_codes = list(set([row.store_code for row in store_date_combos]))
            store_placeholders = ",".join([f":store_{i}" for i in range(len(store_codes))])
            store_params = {f"store_{i}": code for i, code in enumerate(store_codes)}
            
            # Check if zone column exists in devyani_stores table
            check_zone_query = text("""
                SELECT COUNT(*) as col_exists
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = 'devyani_stores'
                AND COLUMN_NAME = 'zone'
            """)
            zone_check_result = await session.execute(check_zone_query)
            zone_exists = zone_check_result.fetchone().col_exists > 0
            
            # Query store metadata (with or without zone column)
            if zone_exists:
                store_metadata_query = text(f"""
                    SELECT store_code, city_id, zone
                    FROM devyani_stores
                    WHERE store_code IN ({store_placeholders})
                """)
            else:
                store_metadata_query = text(f"""
                    SELECT store_code, city_id
                    FROM devyani_stores
                    WHERE store_code IN ({store_placeholders})
                """)
            
            store_metadata_result = await session.execute(store_metadata_query, store_params)
            if zone_exists:
                store_metadata = {row.store_code: {"city_id": row.city_id, "zone": row.zone} 
                                 for row in store_metadata_result.fetchall()}
            else:
                store_metadata = {row.store_code: {"city_id": row.city_id, "zone": None} 
                                 for row in store_metadata_result.fetchall()}
            
            # Step 3: Calculate In-Store Sales
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
                AND store_name IN ({store_placeholders})
                GROUP BY date, store_name
            """)
            instore_params = {
                "start_date": start_date,
                "end_date": end_date,
                **store_params
            }
            instore_result = await session.execute(instore_query, instore_params)
            instore_data = {(row.sales_date, row.store_code): row for row in instore_result.fetchall()}
            
            # Step 4: Calculate Aggregator Sales
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
                AND store_name IN ({store_placeholders})
                GROUP BY date, store_name
            """)
            aggregator_result = await session.execute(aggregator_query, instore_params)
            aggregator_data = {(row.sales_date, row.store_code): row for row in aggregator_result.fetchall()}
            
            # Step 5: Calculate Zomato formula values
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
                AND store_code IN ({store_placeholders})
                AND action IN ('sale', 'addition')
                GROUP BY order_date, store_code
            """)
            zomato_result = await session.execute(zomato_query, instore_params)
            zomato_data = {(row.sales_date, row.store_code): row for row in zomato_result.fetchall()}
            
            # Step 6: Merge and insert/update
            records_processed = 0
            
            for sales_date, store_code in store_date_combos:
                instore_row = instore_data.get((sales_date, store_code))
                aggregator_row = aggregator_data.get((sales_date, store_code))
                zomato_row = zomato_data.get((sales_date, store_code))
                store_info = store_metadata.get(store_code, {"city_id": None, "zone": None})
                
                instore_total = Decimal(str(instore_row.instore_total)) if instore_row else Decimal('0')
                aggregator_total = Decimal(str(aggregator_row.aggregator_total)) if aggregator_row else Decimal('0')
                total_sales = instore_total + aggregator_total
                
                instore_count = instore_row.instore_count if instore_row else 0
                aggregator_count = aggregator_row.aggregator_count if aggregator_row else 0
                total_order_count = instore_count + aggregator_count
                
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
                
                await session.execute(upsert_query, {
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
            
            await session.commit()
            logger.info(f"[DAILY_SALES_SCHEDULER] ✅ Successfully processed {records_processed} records")
            
            return {"success": True, "records_processed": records_processed}
            
    except Exception as e:
        logger.error(f"[DAILY_SALES_SCHEDULER] ❌ Error: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


async def _scheduler_loop():
    """Background loop that runs the population task every 10 seconds"""
    global _stop_event, _initialized
    logger.info(f"[DAILY_SALES_SCHEDULER] Starting scheduler loop (interval={SCHEDULER_INTERVAL_SECONDS}s)")
    
    while not _stop_event.is_set():
        try:
            async with _scheduler_lock:
                result = await _populate_daily_sales_summary_internal()
                if result.get("success"):
                    logger.debug(f"[DAILY_SALES_SCHEDULER] ✅ Run successful: {result.get('records_processed', 0)} records")
                else:
                    logger.warning(f"[DAILY_SALES_SCHEDULER] ⚠️ Run failed: {result.get('error', 'Unknown error')}")
            
            if not _initialized:
                _initialized = True
                logger.info("[DAILY_SALES_SCHEDULER] ✅ Scheduler initialized and running")
            
        except Exception as e:
            logger.error(f"[DAILY_SALES_SCHEDULER] Error in scheduler loop: {e}", exc_info=True)
        
        try:
            await asyncio.wait_for(
                _stop_event.wait(),
                timeout=SCHEDULER_INTERVAL_SECONDS
            )
        except asyncio.TimeoutError:
            continue
    
    logger.info("[DAILY_SALES_SCHEDULER] Scheduler loop stopped")


async def start_daily_sales_scheduler():
    """Start the daily sales summary scheduler"""
    global _scheduler_task, _stop_event
    
    if _scheduler_task and not _scheduler_task.done():
        logger.info("[DAILY_SALES_SCHEDULER] Scheduler already running")
        return
    
    _stop_event = asyncio.Event()
    _scheduler_task = asyncio.create_task(_scheduler_loop(), name="daily_sales_scheduler")
    logger.info("[DAILY_SALES_SCHEDULER] Scheduler task created")


async def stop_daily_sales_scheduler():
    """Stop the daily sales summary scheduler"""
    global _scheduler_task, _stop_event
    
    if _scheduler_task:
        if _stop_event:
            _stop_event.set()
        await _scheduler_task
        _scheduler_task = None
        _stop_event = None
        logger.info("[DAILY_SALES_SCHEDULER] Scheduler task stopped")

