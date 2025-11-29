"""
Background tasks and scheduled jobs
"""

import asyncio
from datetime import datetime
from app.config.database import get_main_db
from app.config.executor import get_task_executor, run_in_executor
from app.utils.email import send_email
import logging

logger = logging.getLogger(__name__)


async def update_subscriptions():
    """Update subscription status - runs twice daily"""
    try:
        logger.info("Running subscription update job...")
        
        # This would typically update subscription statuses
        # For now, just log the execution
        logger.info("Subscription update job completed successfully")
        
    except Exception as e:
        logger.error(f"Error in subscription update job: {e}")


async def check_reconciliation_status():
    """Check reconciliation status"""
    try:
        logger.info("Running reconciliation check...")
        
        # This would check reconciliation status
        # For now, just log the execution
        logger.info("Reconciliation check completed successfully")
        
    except Exception as e:
        logger.error(f"Error in reconciliation check: {e}")


async def populate_sheet_data_tables():
    """Populate sheet data tables"""
    try:
        logger.info("Running sheet data population...")
        
        # This would populate sheet data tables
        # For now, just log the execution
        logger.info("Sheet data population completed successfully")
        
    except Exception as e:
        logger.error(f"Error in sheet data population: {e}")


async def send_notification_email(recipients: list, subject: str, body: str):
    """Send notification email"""
    try:
        await send_email(subject, recipients, body)
        logger.info(f"Notification email sent to {recipients}")
    except Exception as e:
        logger.error(f"Error sending notification email: {e}")


async def process_upload_file(upload_id: int, file_path: str, upload_type: str):
    """Process uploaded file in background"""
    try:
        logger.info(f"Starting background processing for upload {upload_id}")
        
        # Get database session
        async with get_main_db() as db:
            from app.models.main.upload_record import UploadRecord
            
            # Update status to processing
            await UploadRecord.update(db, upload_id, status="processing")
            
            # Process file based on type
            if upload_type == "reconciliation":
                await process_reconciliation_file(db, upload_id, file_path)
            elif upload_type == "sheet_data":
                await process_sheet_data_file(db, upload_id, file_path)
            else:
                await process_generic_file(db, upload_id, file_path)
            
            # Update status to completed
            await UploadRecord.update(db, upload_id, status="completed")
            logger.info(f"Background processing completed for upload {upload_id}")
            
    except Exception as e:
        logger.error(f"Error processing upload {upload_id}: {e}")
        # Update status to failed
        try:
            async with get_main_db() as db:
                from app.models.main.upload_record import UploadRecord
                await UploadRecord.update(db, upload_id, status="failed", message=str(e))
        except:
            pass


async def process_reconciliation_file(db, upload_id: int, file_path: str):
    """Process reconciliation file"""
    # TODO: Implement reconciliation file processing logic
    logger.info(f"Processing reconciliation file: {file_path}")
    pass


async def process_sheet_data_file(db, upload_id: int, file_path: str):
    """Process sheet data file"""
    # TODO: Implement sheet data file processing logic
    logger.info(f"Processing sheet data file: {file_path}")
    pass


async def process_generic_file(db, upload_id: int, file_path: str):
    """Process generic file"""
    # TODO: Implement generic file processing logic
    logger.info(f"Processing generic file: {file_path}")
    pass


async def process_sheet_data_generation(job_id: str, request_data):
    """Process sheet data generation in background - populates all 5 sheet data tables"""
    try:
        logger.info(f"Starting sheet data generation for job {job_id}")
        
        # Use main_db (devyani) for all operations since all tables are in devyani database
        from app.config.database import get_main_db
        from app.config.database import main_session_factory, create_engines
        
        # Ensure engines are created
        if not main_session_factory:
            await create_engines()
        
        async with main_session_factory() as db:
            from sqlalchemy.sql import text
            
            # Truncate all sheet data tables first
            logger.info("Truncating existing data from sheet data tables...")
            truncate_queries = [
                "TRUNCATE TABLE zomato_pos_vs_3po_data",
                "TRUNCATE TABLE zomato_3po_vs_pos_data",
                "TRUNCATE TABLE zomato_3po_vs_pos_refund_data",
                "TRUNCATE TABLE orders_not_in_pos_data",
                "TRUNCATE TABLE orders_not_in_3po_data"
            ]
            
            for query_str in truncate_queries:
                try:
                    await db.execute(text(query_str))
                    await db.commit()
                    logger.info(f"Truncated table: {query_str}")
                except Exception as e:
                    logger.warning(f"Could not truncate {query_str}: {e}")
            
            logger.info("Tables truncated successfully")
            
            # Process all 5 sheet data tables (all using main_db/devyani)
            logger.info("Populating Zomato POS vs 3PO data...")
            await process_zomato_pos_vs_3po_data(db, request_data)
            
            logger.info("Populating Zomato 3PO vs POS data...")
            await process_zomato_3po_vs_pos_data(db, request_data)
            
            logger.info("Populating Zomato 3PO vs POS Refund data...")
            await process_zomato_3po_vs_pos_refund_data(db, request_data)
            
            logger.info("Populating Orders not in POS data...")
            await process_orders_not_in_pos_data(db, request_data)
            
            logger.info("Populating Orders not in 3PO data...")
            await process_orders_not_in_3po_data(db, request_data)
            
            logger.info(f"Sheet data generation completed for job {job_id}")
            
    except Exception as e:
        logger.error(f"Error in sheet data generation for job {job_id}: {e}", exc_info=True)
        raise


async def process_zomato_pos_vs_3po_data(db, request_data):
    """Process Zomato POS vs 3PO data - all tables in devyani (main_db)"""
    from sqlalchemy.sql import text
    from decimal import Decimal
    from datetime import datetime
    
    logger.info("Processing Zomato POS vs 3PO data")
    
    try:
        # Query zomato_vs_pos_summary where pos_order_id is not null (using main_db)
        query = text("""
            SELECT * FROM zomato_vs_pos_summary
            WHERE pos_order_id IS NOT NULL
        """)
        
        result = await db.execute(query)
        records = result.fetchall()
        
        if not records:
            logger.info("No records found for Zomato POS vs 3PO data")
            return
        
        # Prepare bulk insert data
        bulk_data = []
        for record in records:
            record_dict = dict(record._mapping) if hasattr(record, '_mapping') else dict(record)
            # Ensure all POS amounts are positive (convert negative to positive)
            pos_net_amount = abs(float(record_dict.get('pos_net_amount') or 0))
            pos_tax_paid = abs(float(record_dict.get('pos_tax_paid_by_customer') or 0))
            pos_commission = abs(float(record_dict.get('pos_commission_value') or 0))
            pos_pg_applied = abs(float(record_dict.get('pos_pg_applied_on') or 0))
            pos_pg_charge = abs(float(record_dict.get('pos_pg_charge') or 0))
            pos_taxes = abs(float(record_dict.get('pos_taxes_zomato_fee') or 0))
            pos_tds = abs(float(record_dict.get('pos_tds_amount') or 0))
            pos_final = abs(float(record_dict.get('pos_final_amount') or 0))
            
            bulk_data.append({
                "id": f"ZPV3_{record_dict.get('pos_order_id', '')}",
                "pos_order_id": record_dict.get('pos_order_id'),
                "zomato_order_id": record_dict.get('zomato_order_id'),
                "order_date": record_dict.get('order_date'),
                "store_name": record_dict.get('store_name'),
                "pos_net_amount": Decimal(str(pos_net_amount)),
                "zomato_net_amount": Decimal(str(record_dict.get('zomato_net_amount') or 0)),
                "pos_vs_zomato_net_amount_delta": Decimal(str(abs(float(record_dict.get('pos_vs_zomato_net_amount_delta') or 0)))),
                "pos_tax_paid_by_customer": Decimal(str(pos_tax_paid)),
                "zomato_tax_paid_by_customer": Decimal(str(record_dict.get('zomato_tax_paid_by_customer') or 0)),
                "pos_vs_zomato_tax_paid_by_customer_delta": Decimal(str(abs(float(record_dict.get('pos_vs_zomato_tax_paid_by_customer_delta') or 0)))),
                "pos_commission_value": Decimal(str(pos_commission)),
                "zomato_commission_value": Decimal(str(record_dict.get('zomato_commission_value') or 0)),
                "pos_vs_zomato_commission_value_delta": Decimal(str(abs(float(record_dict.get('pos_vs_zomato_commission_value_delta') or 0)))),
                "pos_pg_applied_on": Decimal(str(pos_pg_applied)),
                "zomato_pg_applied_on": Decimal(str(record_dict.get('zomato_pg_applied_on') or 0)),
                "pos_vs_zomato_pg_applied_on_delta": Decimal(str(abs(float(record_dict.get('pos_vs_zomato_pg_applied_on_delta') or 0)))),
                "pos_pg_charge": Decimal(str(pos_pg_charge)),
                "zomato_pg_charge": Decimal(str(record_dict.get('zomato_pg_charge') or 0)),
                "pos_vs_zomato_pg_charge_delta": Decimal(str(abs(float(record_dict.get('pos_vs_zomato_pg_charge_delta') or 0)))),
                "pos_taxes_zomato_fee": Decimal(str(pos_taxes)),
                "zomato_taxes_zomato_fee": Decimal(str(record_dict.get('zomato_taxes_zomato_fee') or 0)),
                "pos_vs_zomato_taxes_zomato_fee_delta": Decimal(str(abs(float(record_dict.get('pos_vs_zomato_taxes_zomato_fee_delta') or 0)))),
                "pos_tds_amount": Decimal(str(pos_tds)),
                "zomato_tds_amount": Decimal(str(record_dict.get('zomato_tds_amount') or 0)),
                "pos_vs_zomato_tds_amount_delta": Decimal(str(abs(float(record_dict.get('pos_vs_zomato_tds_amount_delta') or 0)))),
                "pos_final_amount": Decimal(str(pos_final)),
                "zomato_final_amount": Decimal(str(record_dict.get('zomato_final_amount') or 0)),
                "pos_vs_zomato_final_amount_delta": Decimal(str(abs(float(record_dict.get('pos_vs_zomato_final_amount_delta') or 0)))),
                "reconciled_status": record_dict.get('reconciled_status'),
                "reconciled_amount": Decimal(str(record_dict.get('reconciled_amount') or 0)),
                "unreconciled_amount": Decimal(str(record_dict.get('unreconciled_amount') or 0)),
                "pos_vs_zomato_reason": record_dict.get('pos_vs_zomato_reason'),
                "order_status_pos": record_dict.get('order_status_pos'),
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            })
        
        # Bulk insert using ON DUPLICATE KEY UPDATE (using same db - devyani)
        if bulk_data:
            insert_query = text("""
                INSERT INTO zomato_pos_vs_3po_data (
                    id, pos_order_id, zomato_order_id, order_date, store_name,
                    pos_net_amount, zomato_net_amount, pos_vs_zomato_net_amount_delta,
                    pos_tax_paid_by_customer, zomato_tax_paid_by_customer, pos_vs_zomato_tax_paid_by_customer_delta,
                    pos_commission_value, zomato_commission_value, pos_vs_zomato_commission_value_delta,
                    pos_pg_applied_on, zomato_pg_applied_on, pos_vs_zomato_pg_applied_on_delta,
                    pos_pg_charge, zomato_pg_charge, pos_vs_zomato_pg_charge_delta,
                    pos_taxes_zomato_fee, zomato_taxes_zomato_fee, pos_vs_zomato_taxes_zomato_fee_delta,
                    pos_tds_amount, zomato_tds_amount, pos_vs_zomato_tds_amount_delta,
                    pos_final_amount, zomato_final_amount, pos_vs_zomato_final_amount_delta,
                    reconciled_status, reconciled_amount, unreconciled_amount,
                    pos_vs_zomato_reason, order_status_pos, created_at, updated_at
                ) VALUES (
                    :id, :pos_order_id, :zomato_order_id, :order_date, :store_name,
                    :pos_net_amount, :zomato_net_amount, :pos_vs_zomato_net_amount_delta,
                    :pos_tax_paid_by_customer, :zomato_tax_paid_by_customer, :pos_vs_zomato_tax_paid_by_customer_delta,
                    :pos_commission_value, :zomato_commission_value, :pos_vs_zomato_commission_value_delta,
                    :pos_pg_applied_on, :zomato_pg_applied_on, :pos_vs_zomato_pg_applied_on_delta,
                    :pos_pg_charge, :zomato_pg_charge, :pos_vs_zomato_pg_charge_delta,
                    :pos_taxes_zomato_fee, :zomato_taxes_zomato_fee, :pos_vs_zomato_taxes_zomato_fee_delta,
                    :pos_tds_amount, :zomato_tds_amount, :pos_vs_zomato_tds_amount_delta,
                    :pos_final_amount, :zomato_final_amount, :pos_vs_zomato_final_amount_delta,
                    :reconciled_status, :reconciled_amount, :unreconciled_amount,
                    :pos_vs_zomato_reason, :order_status_pos, :created_at, :updated_at
                )
                ON DUPLICATE KEY UPDATE
                    updated_at = VALUES(updated_at)
            """)
            
            BATCH_SIZE = 1000
            for i in range(0, len(bulk_data), BATCH_SIZE):
                batch = bulk_data[i:i + BATCH_SIZE]
                for record in batch:
                    await db.execute(insert_query, record)
                await db.commit()
                logger.info(f"Inserted batch {i//BATCH_SIZE + 1}: {len(batch)} records")
        
        logger.info(f"Successfully processed {len(bulk_data)} Zomato POS vs 3PO records")
        
    except Exception as e:
        logger.error(f"Error processing Zomato POS vs 3PO data: {e}", exc_info=True)
        raise


async def process_zomato_3po_vs_pos_data(db, request_data):
    """Process Zomato 3PO vs POS data"""
    from sqlalchemy.sql import text
    from decimal import Decimal
    from datetime import datetime
    
    logger.info("Processing Zomato 3PO vs POS data")
    
    try:
        # Query zomato_vs_pos_summary where zomato_order_id is not null
        query = text("""
            SELECT * FROM zomato_vs_pos_summary
            WHERE zomato_order_id IS NOT NULL
        """)
        
        result = await db.execute(query)
        records = result.fetchall()
        
        if not records:
            logger.info("No records found for Zomato 3PO vs POS data")
            return
        
        # Prepare bulk insert data
        bulk_data = []
        for record in records:
            record_dict = dict(record._mapping) if hasattr(record, '_mapping') else dict(record)
            
            # Ensure all POS amounts are positive (convert negative to positive)
            pos_net_amount = abs(float(record_dict.get('pos_net_amount') or 0))
            pos_tax_paid = abs(float(record_dict.get('pos_tax_paid_by_customer') or 0))
            pos_commission = abs(float(record_dict.get('pos_commission_value') or 0))
            pos_pg_applied = abs(float(record_dict.get('pos_pg_applied_on') or 0))
            pos_pg_charge = abs(float(record_dict.get('pos_pg_charge') or 0))
            pos_taxes = abs(float(record_dict.get('pos_taxes_zomato_fee') or 0))
            pos_tds = abs(float(record_dict.get('pos_tds_amount') or 0))
            pos_final = abs(float(record_dict.get('pos_final_amount') or 0))
            
            bulk_data.append({
                "id": f"Z3PVP_{record_dict.get('zomato_order_id', '')}",
                "zomato_order_id": record_dict.get('zomato_order_id'),
                "pos_order_id": record_dict.get('pos_order_id'),
                "order_date": record_dict.get('order_date'),
                "store_name": record_dict.get('store_name'),
                "zomato_net_amount": Decimal(str(record_dict.get('zomato_net_amount') or 0)),
                "pos_net_amount": Decimal(str(pos_net_amount)),
                "zomato_vs_pos_net_amount_delta": Decimal(str(abs(float(record_dict.get('zomato_vs_pos_net_amount_delta') or 0)))),
                "zomato_tax_paid_by_customer": Decimal(str(record_dict.get('zomato_tax_paid_by_customer') or 0)),
                "pos_tax_paid_by_customer": Decimal(str(pos_tax_paid)),
                "zomato_vs_pos_tax_paid_by_customer_delta": Decimal(str(abs(float(record_dict.get('zomato_vs_pos_tax_paid_by_customer_delta') or 0)))),
                "zomato_commission_value": Decimal(str(record_dict.get('zomato_commission_value') or 0)),
                "pos_commission_value": Decimal(str(pos_commission)),
                "zomato_vs_pos_commission_value_delta": Decimal(str(abs(float(record_dict.get('zomato_vs_pos_commission_value_delta') or 0)))),
                "zomato_pg_applied_on": Decimal(str(record_dict.get('zomato_pg_applied_on') or 0)),
                "pos_pg_applied_on": Decimal(str(pos_pg_applied)),
                "zomato_vs_pos_pg_applied_on_delta": Decimal(str(abs(float(record_dict.get('zomato_vs_pos_pg_applied_on_delta') or 0)))),
                "zomato_pg_charge": Decimal(str(record_dict.get('zomato_pg_charge') or 0)),
                "pos_pg_charge": Decimal(str(pos_pg_charge)),
                "zomato_vs_pos_pg_charge_delta": Decimal(str(abs(float(record_dict.get('zomato_vs_pos_pg_charge_delta') or 0)))),
                "zomato_taxes_zomato_fee": Decimal(str(record_dict.get('zomato_taxes_zomato_fee') or 0)),
                "pos_taxes_zomato_fee": Decimal(str(pos_taxes)),
                "zomato_vs_pos_taxes_zomato_fee_delta": Decimal(str(abs(float(record_dict.get('zomato_vs_pos_taxes_zomato_fee_delta') or 0)))),
                "zomato_tds_amount": Decimal(str(record_dict.get('zomato_tds_amount') or 0)),
                "pos_tds_amount": Decimal(str(pos_tds)),
                "zomato_vs_pos_tds_amount_delta": Decimal(str(abs(float(record_dict.get('zomato_vs_pos_tds_amount_delta') or 0)))),
                "zomato_final_amount": Decimal(str(record_dict.get('zomato_final_amount') or 0)),
                "pos_final_amount": Decimal(str(pos_final)),
                "zomato_vs_pos_final_amount_delta": Decimal(str(abs(float(record_dict.get('zomato_vs_pos_final_amount_delta') or 0)))),
                "calculated_zomato_net_amount": Decimal(str(record_dict.get('calculated_zomato_net_amount') or 0)),
                "calculated_zomato_tax_paid_by_customer": Decimal(str(record_dict.get('calculated_zomato_tax_paid_by_customer') or 0)),
                "calculated_zomato_commission_value": Decimal(str(record_dict.get('calculated_zomato_commission_value') or 0)),
                "calculated_zomato_pg_applied_on": Decimal(str(record_dict.get('calculated_zomato_pg_applied_on') or 0)),
                "calculated_zomato_pg_charge": Decimal(str(record_dict.get('calculated_zomato_pg_charge') or 0)),
                "calculated_zomato_taxes_zomato_fee": Decimal(str(record_dict.get('calculated_zomato_taxes_zomato_fee') or 0)),
                "calculated_zomato_tds_amount": Decimal(str(record_dict.get('calculated_zomato_tds_amount') or 0)),
                "calculated_zomato_final_amount": Decimal(str(record_dict.get('calculated_zomato_final_amount') or 0)),
                "fixed_credit_note_amount": Decimal(str(record_dict.get('fixed_credit_note_amount') or 0)),
                "fixed_pro_discount_passthrough": Decimal(str(record_dict.get('fixed_pro_discount_passthrough') or 0)),
                "fixed_customer_discount": Decimal(str(record_dict.get('fixed_customer_discount') or 0)),
                "fixed_rejection_penalty_charge": Decimal(str(record_dict.get('fixed_rejection_penalty_charge') or 0)),
                "fixed_user_credits_charge": Decimal(str(record_dict.get('fixed_user_credits_charge') or 0)),
                "fixed_promo_recovery_adj": Decimal(str(record_dict.get('fixed_promo_recovery_adj') or 0)),
                "fixed_icecream_handling": Decimal(str(record_dict.get('fixed_icecream_handling') or 0)),
                "fixed_icecream_deductions": Decimal(str(record_dict.get('fixed_icecream_deductions') or 0)),
                "fixed_order_support_cost": Decimal(str(record_dict.get('fixed_order_support_cost') or 0)),
                "fixed_merchant_delivery_charge": Decimal(str(record_dict.get('fixed_merchant_delivery_charge') or 0)),
                "reconciled_status": record_dict.get('reconciled_status'),
                "reconciled_amount": Decimal(str(record_dict.get('reconciled_amount') or 0)),
                "unreconciled_amount": Decimal(str(record_dict.get('unreconciled_amount') or 0)),
                "zomato_vs_pos_reason": record_dict.get('zomato_vs_pos_reason'),
                "order_status_zomato": record_dict.get('order_status_zomato'),
                "action": record_dict.get('action'),  # Copy action from zomato_vs_pos_summary
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            })
        
        # Bulk insert using ON DUPLICATE KEY UPDATE
        if bulk_data:
            insert_query = text("""
                INSERT INTO zomato_3po_vs_pos_data (
                    id, zomato_order_id, pos_order_id, order_date, store_name,
                    zomato_net_amount, pos_net_amount, zomato_vs_pos_net_amount_delta,
                    zomato_tax_paid_by_customer, pos_tax_paid_by_customer, zomato_vs_pos_tax_paid_by_customer_delta,
                    zomato_commission_value, pos_commission_value, zomato_vs_pos_commission_value_delta,
                    zomato_pg_applied_on, pos_pg_applied_on, zomato_vs_pos_pg_applied_on_delta,
                    zomato_pg_charge, pos_pg_charge, zomato_vs_pos_pg_charge_delta,
                    zomato_taxes_zomato_fee, pos_taxes_zomato_fee, zomato_vs_pos_taxes_zomato_fee_delta,
                    zomato_tds_amount, pos_tds_amount, zomato_vs_pos_tds_amount_delta,
                    zomato_final_amount, pos_final_amount, zomato_vs_pos_final_amount_delta,
                    calculated_zomato_net_amount, calculated_zomato_tax_paid_by_customer,
                    calculated_zomato_commission_value, calculated_zomato_pg_applied_on,
                    calculated_zomato_pg_charge, calculated_zomato_taxes_zomato_fee,
                    calculated_zomato_tds_amount, calculated_zomato_final_amount,
                    fixed_credit_note_amount, fixed_pro_discount_passthrough,
                    fixed_customer_discount, fixed_rejection_penalty_charge,
                    fixed_user_credits_charge, fixed_promo_recovery_adj,
                    fixed_icecream_handling, fixed_icecream_deductions,
                    fixed_order_support_cost, fixed_merchant_delivery_charge,
                    reconciled_status, reconciled_amount, unreconciled_amount,
                    zomato_vs_pos_reason, order_status_zomato, action, created_at, updated_at
                ) VALUES (
                    :id, :zomato_order_id, :pos_order_id, :order_date, :store_name,
                    :zomato_net_amount, :pos_net_amount, :zomato_vs_pos_net_amount_delta,
                    :zomato_tax_paid_by_customer, :pos_tax_paid_by_customer, :zomato_vs_pos_tax_paid_by_customer_delta,
                    :zomato_commission_value, :pos_commission_value, :zomato_vs_pos_commission_value_delta,
                    :zomato_pg_applied_on, :pos_pg_applied_on, :zomato_vs_pos_pg_applied_on_delta,
                    :zomato_pg_charge, :pos_pg_charge, :zomato_vs_pos_pg_charge_delta,
                    :zomato_taxes_zomato_fee, :pos_taxes_zomato_fee, :zomato_vs_pos_taxes_zomato_fee_delta,
                    :zomato_tds_amount, :pos_tds_amount, :zomato_vs_pos_tds_amount_delta,
                    :zomato_final_amount, :pos_final_amount, :zomato_vs_pos_final_amount_delta,
                    :calculated_zomato_net_amount, :calculated_zomato_tax_paid_by_customer,
                    :calculated_zomato_commission_value, :calculated_zomato_pg_applied_on,
                    :calculated_zomato_pg_charge, :calculated_zomato_taxes_zomato_fee,
                    :calculated_zomato_tds_amount, :calculated_zomato_final_amount,
                    :fixed_credit_note_amount, :fixed_pro_discount_passthrough,
                    :fixed_customer_discount, :fixed_rejection_penalty_charge,
                    :fixed_user_credits_charge, :fixed_promo_recovery_adj,
                    :fixed_icecream_handling, :fixed_icecream_deductions,
                    :fixed_order_support_cost, :fixed_merchant_delivery_charge,
                    :reconciled_status, :reconciled_amount, :unreconciled_amount,
                    :zomato_vs_pos_reason, :order_status_zomato, :action, :created_at, :updated_at
                )
                ON DUPLICATE KEY UPDATE
                    updated_at = VALUES(updated_at)
            """)
            
            BATCH_SIZE = 1000
            for i in range(0, len(bulk_data), BATCH_SIZE):
                batch = bulk_data[i:i + BATCH_SIZE]
                for record in batch:
                    await db.execute(insert_query, record)
                await db.commit()
                logger.info(f"Inserted batch {i//BATCH_SIZE + 1}: {len(batch)} records")
        
        logger.info(f"Successfully processed {len(bulk_data)} Zomato 3PO vs POS records")
        
    except Exception as e:
        logger.error(f"Error processing Zomato 3PO vs POS data: {e}", exc_info=True)
        raise


async def process_zomato_3po_vs_pos_refund_data(db, request_data):
    """Process Zomato 3PO vs POS refund data"""
    from sqlalchemy.sql import text
    from decimal import Decimal
    from datetime import datetime
    
    logger.info("Processing Zomato 3PO vs POS refund data")
    
    try:
        # Query zomato_vs_pos_summary where zomato_order_id is not null and order_status_zomato is 'refund'
        query = text("""
            SELECT * FROM zomato_vs_pos_summary
            WHERE zomato_order_id IS NOT NULL
            AND order_status_zomato = 'refund'
        """)
        
        result = await db.execute(query)
        records = result.fetchall()
        
        if not records:
            logger.info("No records found for Zomato 3PO vs POS refund data")
            return
        
        # Prepare bulk insert data (same structure as zomato_3po_vs_pos_data)
        bulk_data = []
        for record in records:
            record_dict = dict(record._mapping) if hasattr(record, '_mapping') else dict(record)
            
            # Ensure all POS amounts are positive (convert negative to positive)
            pos_net_amount = abs(float(record_dict.get('pos_net_amount') or 0))
            pos_tax_paid = abs(float(record_dict.get('pos_tax_paid_by_customer') or 0))
            pos_commission = abs(float(record_dict.get('pos_commission_value') or 0))
            pos_pg_applied = abs(float(record_dict.get('pos_pg_applied_on') or 0))
            pos_pg_charge = abs(float(record_dict.get('pos_pg_charge') or 0))
            pos_taxes = abs(float(record_dict.get('pos_taxes_zomato_fee') or 0))
            pos_tds = abs(float(record_dict.get('pos_tds_amount') or 0))
            pos_final = abs(float(record_dict.get('pos_final_amount') or 0))
            
            bulk_data.append({
                "id": f"Z3PVPR_{record_dict.get('zomato_order_id', '')}",
                "zomato_order_id": record_dict.get('zomato_order_id'),
                "pos_order_id": record_dict.get('pos_order_id'),
                "order_date": record_dict.get('order_date'),
                "store_name": record_dict.get('store_name'),
                "zomato_net_amount": Decimal(str(record_dict.get('zomato_net_amount') or 0)),
                "pos_net_amount": Decimal(str(pos_net_amount)),
                "zomato_vs_pos_net_amount_delta": Decimal(str(abs(float(record_dict.get('zomato_vs_pos_net_amount_delta') or 0)))),
                "zomato_tax_paid_by_customer": Decimal(str(record_dict.get('zomato_tax_paid_by_customer') or 0)),
                "pos_tax_paid_by_customer": Decimal(str(pos_tax_paid)),
                "zomato_vs_pos_tax_paid_by_customer_delta": Decimal(str(abs(float(record_dict.get('zomato_vs_pos_tax_paid_by_customer_delta') or 0)))),
                "zomato_commission_value": Decimal(str(record_dict.get('zomato_commission_value') or 0)),
                "pos_commission_value": Decimal(str(pos_commission)),
                "zomato_vs_pos_commission_value_delta": Decimal(str(abs(float(record_dict.get('zomato_vs_pos_commission_value_delta') or 0)))),
                "zomato_pg_applied_on": Decimal(str(record_dict.get('zomato_pg_applied_on') or 0)),
                "pos_pg_applied_on": Decimal(str(pos_pg_applied)),
                "zomato_vs_pos_pg_applied_on_delta": Decimal(str(abs(float(record_dict.get('zomato_vs_pos_pg_applied_on_delta') or 0)))),
                "zomato_pg_charge": Decimal(str(record_dict.get('zomato_pg_charge') or 0)),
                "pos_pg_charge": Decimal(str(pos_pg_charge)),
                "zomato_vs_pos_pg_charge_delta": Decimal(str(abs(float(record_dict.get('zomato_vs_pos_pg_charge_delta') or 0)))),
                "zomato_taxes_zomato_fee": Decimal(str(record_dict.get('zomato_taxes_zomato_fee') or 0)),
                "pos_taxes_zomato_fee": Decimal(str(pos_taxes)),
                "zomato_vs_pos_taxes_zomato_fee_delta": Decimal(str(abs(float(record_dict.get('zomato_vs_pos_taxes_zomato_fee_delta') or 0)))),
                "zomato_tds_amount": Decimal(str(record_dict.get('zomato_tds_amount') or 0)),
                "pos_tds_amount": Decimal(str(pos_tds)),
                "zomato_vs_pos_tds_amount_delta": Decimal(str(abs(float(record_dict.get('zomato_vs_pos_tds_amount_delta') or 0)))),
                "zomato_final_amount": Decimal(str(record_dict.get('zomato_final_amount') or 0)),
                "pos_final_amount": Decimal(str(pos_final)),
                "zomato_vs_pos_final_amount_delta": Decimal(str(abs(float(record_dict.get('zomato_vs_pos_final_amount_delta') or 0)))),
                "calculated_zomato_net_amount": Decimal(str(record_dict.get('calculated_zomato_net_amount') or 0)),
                "calculated_zomato_tax_paid_by_customer": Decimal(str(record_dict.get('calculated_zomato_tax_paid_by_customer') or 0)),
                "calculated_zomato_commission_value": Decimal(str(record_dict.get('calculated_zomato_commission_value') or 0)),
                "calculated_zomato_pg_applied_on": Decimal(str(record_dict.get('calculated_zomato_pg_applied_on') or 0)),
                "calculated_zomato_pg_charge": Decimal(str(record_dict.get('calculated_zomato_pg_charge') or 0)),
                "calculated_zomato_taxes_zomato_fee": Decimal(str(record_dict.get('calculated_zomato_taxes_zomato_fee') or 0)),
                "calculated_zomato_tds_amount": Decimal(str(record_dict.get('calculated_zomato_tds_amount') or 0)),
                "calculated_zomato_final_amount": Decimal(str(record_dict.get('calculated_zomato_final_amount') or 0)),
                "fixed_credit_note_amount": Decimal(str(record_dict.get('fixed_credit_note_amount') or 0)),
                "fixed_pro_discount_passthrough": Decimal(str(record_dict.get('fixed_pro_discount_passthrough') or 0)),
                "fixed_customer_discount": Decimal(str(record_dict.get('fixed_customer_discount') or 0)),
                "fixed_rejection_penalty_charge": Decimal(str(record_dict.get('fixed_rejection_penalty_charge') or 0)),
                "fixed_user_credits_charge": Decimal(str(record_dict.get('fixed_user_credits_charge') or 0)),
                "fixed_promo_recovery_adj": Decimal(str(record_dict.get('fixed_promo_recovery_adj') or 0)),
                "fixed_icecream_handling": Decimal(str(record_dict.get('fixed_icecream_handling') or 0)),
                "fixed_icecream_deductions": Decimal(str(record_dict.get('fixed_icecream_deductions') or 0)),
                "fixed_order_support_cost": Decimal(str(record_dict.get('fixed_order_support_cost') or 0)),
                "fixed_merchant_delivery_charge": Decimal(str(record_dict.get('fixed_merchant_delivery_charge') or 0)),
                "reconciled_status": record_dict.get('reconciled_status'),
                "reconciled_amount": Decimal(str(record_dict.get('reconciled_amount') or 0)),
                "unreconciled_amount": Decimal(str(record_dict.get('unreconciled_amount') or 0)),
                "zomato_vs_pos_reason": record_dict.get('zomato_vs_pos_reason'),
                "order_status_zomato": record_dict.get('order_status_zomato'),
                "action": record_dict.get('action'),  # Copy action from zomato_vs_pos_summary
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            })
        
        # Bulk insert using ON DUPLICATE KEY UPDATE
        if bulk_data:
            insert_query = text("""
                INSERT INTO zomato_3po_vs_pos_refund_data (
                    id, zomato_order_id, pos_order_id, order_date, store_name,
                    zomato_net_amount, pos_net_amount, zomato_vs_pos_net_amount_delta,
                    zomato_tax_paid_by_customer, pos_tax_paid_by_customer, zomato_vs_pos_tax_paid_by_customer_delta,
                    zomato_commission_value, pos_commission_value, zomato_vs_pos_commission_value_delta,
                    zomato_pg_applied_on, pos_pg_applied_on, zomato_vs_pos_pg_applied_on_delta,
                    zomato_pg_charge, pos_pg_charge, zomato_vs_pos_pg_charge_delta,
                    zomato_taxes_zomato_fee, pos_taxes_zomato_fee, zomato_vs_pos_taxes_zomato_fee_delta,
                    zomato_tds_amount, pos_tds_amount, zomato_vs_pos_tds_amount_delta,
                    zomato_final_amount, pos_final_amount, zomato_vs_pos_final_amount_delta,
                    calculated_zomato_net_amount, calculated_zomato_tax_paid_by_customer,
                    calculated_zomato_commission_value, calculated_zomato_pg_applied_on,
                    calculated_zomato_pg_charge, calculated_zomato_taxes_zomato_fee,
                    calculated_zomato_tds_amount, calculated_zomato_final_amount,
                    fixed_credit_note_amount, fixed_pro_discount_passthrough,
                    fixed_customer_discount, fixed_rejection_penalty_charge,
                    fixed_user_credits_charge, fixed_promo_recovery_adj,
                    fixed_icecream_handling, fixed_icecream_deductions,
                    fixed_order_support_cost, fixed_merchant_delivery_charge,
                    reconciled_status, reconciled_amount, unreconciled_amount,
                    zomato_vs_pos_reason, order_status_zomato, action, created_at, updated_at
                ) VALUES (
                    :id, :zomato_order_id, :pos_order_id, :order_date, :store_name,
                    :zomato_net_amount, :pos_net_amount, :zomato_vs_pos_net_amount_delta,
                    :zomato_tax_paid_by_customer, :pos_tax_paid_by_customer, :zomato_vs_pos_tax_paid_by_customer_delta,
                    :zomato_commission_value, :pos_commission_value, :zomato_vs_pos_commission_value_delta,
                    :zomato_pg_applied_on, :pos_pg_applied_on, :zomato_vs_pos_pg_applied_on_delta,
                    :zomato_pg_charge, :pos_pg_charge, :zomato_vs_pos_pg_charge_delta,
                    :zomato_taxes_zomato_fee, :pos_taxes_zomato_fee, :zomato_vs_pos_taxes_zomato_fee_delta,
                    :zomato_tds_amount, :pos_tds_amount, :zomato_vs_pos_tds_amount_delta,
                    :zomato_final_amount, :pos_final_amount, :zomato_vs_pos_final_amount_delta,
                    :calculated_zomato_net_amount, :calculated_zomato_tax_paid_by_customer,
                    :calculated_zomato_commission_value, :calculated_zomato_pg_applied_on,
                    :calculated_zomato_pg_charge, :calculated_zomato_taxes_zomato_fee,
                    :calculated_zomato_tds_amount, :calculated_zomato_final_amount,
                    :fixed_credit_note_amount, :fixed_pro_discount_passthrough,
                    :fixed_customer_discount, :fixed_rejection_penalty_charge,
                    :fixed_user_credits_charge, :fixed_promo_recovery_adj,
                    :fixed_icecream_handling, :fixed_icecream_deductions,
                    :fixed_order_support_cost, :fixed_merchant_delivery_charge,
                    :reconciled_status, :reconciled_amount, :unreconciled_amount,
                    :zomato_vs_pos_reason, :order_status_zomato, :action, :created_at, :updated_at
                )
                ON DUPLICATE KEY UPDATE
                    updated_at = VALUES(updated_at)
            """)
            
            BATCH_SIZE = 1000
            for i in range(0, len(bulk_data), BATCH_SIZE):
                batch = bulk_data[i:i + BATCH_SIZE]
                for record in batch:
                    await db.execute(insert_query, record)
                await db.commit()
                logger.info(f"Inserted batch {i//BATCH_SIZE + 1}: {len(batch)} records")
        
        logger.info(f"Successfully processed {len(bulk_data)} Zomato 3PO vs POS refund records")
        
    except Exception as e:
        logger.error(f"Error processing Zomato 3PO vs POS refund data: {e}", exc_info=True)
        raise


async def process_orders_not_in_pos_data(db, request_data):
    """Process orders not in POS data"""
    from sqlalchemy.sql import text
    from decimal import Decimal
    from datetime import datetime
    
    logger.info("Processing orders not in POS data")
    
    try:
        # Query zomato_vs_pos_summary where pos_order_id is null and zomato_order_id is not null
        query = text("""
            SELECT * FROM zomato_vs_pos_summary
            WHERE pos_order_id IS NULL
            AND zomato_order_id IS NOT NULL
        """)
        
        result = await db.execute(query)
        records = result.fetchall()
        
        if not records:
            logger.info("No records found for orders not in POS data")
            return
        
        # Prepare bulk insert data
        bulk_data = []
        for record in records:
            record_dict = dict(record._mapping) if hasattr(record, '_mapping') else dict(record)
            bulk_data.append({
                "id": f"ONIP_{record_dict.get('zomato_order_id', '')}",
                "zomato_order_id": record_dict.get('zomato_order_id'),
                "order_date": record_dict.get('order_date'),
                "store_name": record_dict.get('store_name'),
                "zomato_net_amount": Decimal(str(record_dict.get('zomato_net_amount') or 0)),
                "zomato_tax_paid_by_customer": Decimal(str(record_dict.get('zomato_tax_paid_by_customer') or 0)),
                "zomato_commission_value": Decimal(str(record_dict.get('zomato_commission_value') or 0)),
                "zomato_pg_applied_on": Decimal(str(record_dict.get('zomato_pg_applied_on') or 0)),
                "zomato_pg_charge": Decimal(str(record_dict.get('zomato_pg_charge') or 0)),
                "zomato_taxes_zomato_fee": Decimal(str(record_dict.get('zomato_taxes_zomato_fee') or 0)),
                "zomato_tds_amount": Decimal(str(record_dict.get('zomato_tds_amount') or 0)),
                "zomato_final_amount": Decimal(str(record_dict.get('zomato_final_amount') or 0)),
                "calculated_zomato_net_amount": Decimal(str(record_dict.get('calculated_zomato_net_amount') or 0)),
                "calculated_zomato_tax_paid_by_customer": Decimal(str(record_dict.get('calculated_zomato_tax_paid_by_customer') or 0)),
                "calculated_zomato_commission_value": Decimal(str(record_dict.get('calculated_zomato_commission_value') or 0)),
                "calculated_zomato_pg_applied_on": Decimal(str(record_dict.get('calculated_zomato_pg_applied_on') or 0)),
                "calculated_zomato_pg_charge": Decimal(str(record_dict.get('calculated_zomato_pg_charge') or 0)),
                "calculated_zomato_taxes_zomato_fee": Decimal(str(record_dict.get('calculated_zomato_taxes_zomato_fee') or 0)),
                "calculated_zomato_tds_amount": Decimal(str(record_dict.get('calculated_zomato_tds_amount') or 0)),
                "calculated_zomato_final_amount": Decimal(str(record_dict.get('calculated_zomato_final_amount') or 0)),
                "fixed_credit_note_amount": Decimal(str(record_dict.get('fixed_credit_note_amount') or 0)),
                "fixed_pro_discount_passthrough": Decimal(str(record_dict.get('fixed_pro_discount_passthrough') or 0)),
                "fixed_customer_discount": Decimal(str(record_dict.get('fixed_customer_discount') or 0)),
                "fixed_rejection_penalty_charge": Decimal(str(record_dict.get('fixed_rejection_penalty_charge') or 0)),
                "fixed_user_credits_charge": Decimal(str(record_dict.get('fixed_user_credits_charge') or 0)),
                "fixed_promo_recovery_adj": Decimal(str(record_dict.get('fixed_promo_recovery_adj') or 0)),
                "fixed_icecream_handling": Decimal(str(record_dict.get('fixed_icecream_handling') or 0)),
                "fixed_icecream_deductions": Decimal(str(record_dict.get('fixed_icecream_deductions') or 0)),
                "fixed_order_support_cost": Decimal(str(record_dict.get('fixed_order_support_cost') or 0)),
                "fixed_merchant_delivery_charge": Decimal(str(record_dict.get('fixed_merchant_delivery_charge') or 0)),
                "reconciled_status": record_dict.get('reconciled_status'),
                "reconciled_amount": Decimal(str(record_dict.get('reconciled_amount') or 0)),
                "unreconciled_amount": Decimal(str(record_dict.get('unreconciled_amount') or 0)),
                "zomato_vs_pos_reason": record_dict.get('zomato_vs_pos_reason'),
                "order_status_zomato": record_dict.get('order_status_zomato'),
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            })
        
        # Bulk insert using ON DUPLICATE KEY UPDATE
        if bulk_data:
            insert_query = text("""
                INSERT INTO orders_not_in_pos_data (
                    id, zomato_order_id, order_date, store_name,
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
                    reconciled_status, reconciled_amount, unreconciled_amount,
                    zomato_vs_pos_reason, order_status_zomato, created_at, updated_at
                ) VALUES (
                    :id, :zomato_order_id, :order_date, :store_name,
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
                    :reconciled_status, :reconciled_amount, :unreconciled_amount,
                    :zomato_vs_pos_reason, :order_status_zomato, :created_at, :updated_at
                )
                ON DUPLICATE KEY UPDATE
                    updated_at = VALUES(updated_at)
            """)
            
            BATCH_SIZE = 1000
            for i in range(0, len(bulk_data), BATCH_SIZE):
                batch = bulk_data[i:i + BATCH_SIZE]
                for record in batch:
                    await db.execute(insert_query, record)
                await db.commit()
                logger.info(f"Inserted batch {i//BATCH_SIZE + 1}: {len(batch)} records")
        
        logger.info(f"Successfully processed {len(bulk_data)} orders not in POS records")
        
    except Exception as e:
        logger.error(f"Error processing orders not in POS data: {e}", exc_info=True)
        raise


async def process_orders_not_in_3po_data(db, request_data):
    """Process orders not in 3PO data"""
    from sqlalchemy.sql import text
    from decimal import Decimal
    from datetime import datetime
    
    logger.info("Processing orders not in 3PO data")
    
    try:
        # Query zomato_vs_pos_summary where zomato_order_id is null and pos_order_id is not null
        query = text("""
            SELECT * FROM zomato_vs_pos_summary
            WHERE zomato_order_id IS NULL
            AND pos_order_id IS NOT NULL
        """)
        
        result = await db.execute(query)
        records = result.fetchall()
        
        if not records:
            logger.info("No records found for orders not in 3PO data")
            return
        
        # Prepare bulk insert data
        bulk_data = []
        for record in records:
            record_dict = dict(record._mapping) if hasattr(record, '_mapping') else dict(record)
            bulk_data.append({
                "id": f"ONI3_{record_dict.get('pos_order_id', '')}",
                "pos_order_id": record_dict.get('pos_order_id'),
                "order_date": record_dict.get('order_date'),
                "store_name": record_dict.get('store_name'),
                "pos_net_amount": Decimal(str(record_dict.get('pos_net_amount') or 0)),
                "pos_tax_paid_by_customer": Decimal(str(record_dict.get('pos_tax_paid_by_customer') or 0)),
                "pos_commission_value": Decimal(str(record_dict.get('pos_commission_value') or 0)),
                "pos_pg_applied_on": Decimal(str(record_dict.get('pos_pg_applied_on') or 0)),
                "pos_pg_charge": Decimal(str(record_dict.get('pos_pg_charge') or 0)),
                "pos_taxes_zomato_fee": Decimal(str(record_dict.get('pos_taxes_zomato_fee') or 0)),
                "pos_tds_amount": Decimal(str(record_dict.get('pos_tds_amount') or 0)),
                "pos_final_amount": Decimal(str(record_dict.get('pos_final_amount') or 0)),
                "reconciled_status": record_dict.get('reconciled_status'),
                "reconciled_amount": Decimal(str(record_dict.get('reconciled_amount') or 0)),
                "unreconciled_amount": Decimal(str(record_dict.get('unreconciled_amount') or 0)),
                "pos_vs_zomato_reason": record_dict.get('pos_vs_zomato_reason'),
                "order_status_pos": record_dict.get('order_status_pos'),
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            })
        
        # Bulk insert using ON DUPLICATE KEY UPDATE
        if bulk_data:
            insert_query = text("""
                INSERT INTO orders_not_in_3po_data (
                    id, pos_order_id, order_date, store_name,
                    pos_net_amount, pos_tax_paid_by_customer, pos_commission_value,
                    pos_pg_applied_on, pos_pg_charge, pos_taxes_zomato_fee,
                    pos_tds_amount, pos_final_amount,
                    reconciled_status, reconciled_amount, unreconciled_amount,
                    pos_vs_zomato_reason, order_status_pos, created_at, updated_at
                ) VALUES (
                    :id, :pos_order_id, :order_date, :store_name,
                    :pos_net_amount, :pos_tax_paid_by_customer, :pos_commission_value,
                    :pos_pg_applied_on, :pos_pg_charge, :pos_taxes_zomato_fee,
                    :pos_tds_amount, :pos_final_amount,
                    :reconciled_status, :reconciled_amount, :unreconciled_amount,
                    :pos_vs_zomato_reason, :order_status_pos, :created_at, :updated_at
                )
                ON DUPLICATE KEY UPDATE
                    updated_at = VALUES(updated_at)
            """)
            
            BATCH_SIZE = 1000
            for i in range(0, len(bulk_data), BATCH_SIZE):
                batch = bulk_data[i:i + BATCH_SIZE]
                for record in batch:
                    await db.execute(insert_query, record)
                await db.commit()
                logger.info(f"Inserted batch {i//BATCH_SIZE + 1}: {len(batch)} records")
        
        logger.info(f"Successfully processed {len(bulk_data)} orders not in 3PO records")
        
    except Exception as e:
        logger.error(f"Error processing orders not in 3PO data: {e}", exc_info=True)
        raise


# Scheduled tasks
async def run_scheduled_tasks():
    """Run all scheduled tasks"""
    try:
        # Run subscription update
        await update_subscriptions()
        
        # Run reconciliation check
        await check_reconciliation_status()
        
        # Run sheet data population
        await populate_sheet_data_tables()
        
    except Exception as e:
        logger.error(f"Error running scheduled tasks: {e}")


async def process_receivable_receipt_excel_generation(generation_id: int, params: dict):
    """Process receivable receipt Excel generation in background - similar to Node.js worker"""
    try:
        from app.config.database import main_session_factory, create_engines
        from app.models.main.excel_generation import ExcelGeneration, ExcelGenerationStatus
        from sqlalchemy.sql import text
        from datetime import datetime
        import pandas as pd
        import os
        
        logger.info(f"[Excel Generation {generation_id}] Starting receivable receipt Excel generation")
        
        # Ensure engines are created
        if not main_session_factory:
            await create_engines()
        
        async with main_session_factory() as db:
            # Update status to processing
            await ExcelGeneration.update_status(
                db,
                generation_id,
                ExcelGenerationStatus.PROCESSING,
                progress=0,
                message="Starting receivable receipt Excel generation..."
            )
            
            start_date = params["start_date"]
            end_date = params["end_date"]
            store_codes = params["store_codes"]
            reports_dir = params["reports_dir"]
            
            # Parse dates
            date_formats = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y"]
            start_date_dt = None
            end_date_dt = None
            
            for fmt in date_formats:
                try:
                    start_date_dt = datetime.strptime(start_date, fmt).date()
                    break
                except ValueError:
                    continue
            
            for fmt in date_formats:
                try:
                    end_date_dt = datetime.strptime(end_date, fmt).date()
                    break
                except ValueError:
                    continue
            
            # Build store params for SQL query (matching pattern from routes)
            stores_placeholder = ",".join([f":store_{i}" for i in range(len(store_codes))])
            stores_params = {f"store_{i}": store_code for i, store_code in enumerate(store_codes)}
            
            # Query receivable vs receipt data grouped by UTR
            receivables_query_str = f"""
                SELECT 
                    utr_number,
                    utr_date,
                    SUM(final_amount) as receivable,
                    MAX(deposit_amount) as receipt
                FROM zomato_receivables_vs_receipts
                WHERE order_date BETWEEN :start_date AND :end_date
                AND store_name IN ({stores_placeholder})
                AND utr_number IS NOT NULL
                GROUP BY utr_number, utr_date
                ORDER BY utr_number ASC
            """
            
            receivables_query = text(receivables_query_str)
            
            params_query = {
                "start_date": start_date_dt,
                "end_date": end_date_dt,
                **stores_params
            }
            
            logger.info(f"[Excel Generation {generation_id}] Executing receivables query")
            receivables_result = await db.execute(receivables_query, params_query)
            receivables_records = receivables_result.fetchall()
            
            # Convert to list of dictionaries with delta calculation
            receivable_data = []
            for rec in receivables_records:
                receivable = float(rec.receivable or 0)
                receipt = float(rec.receipt or 0)
                delta = receivable - receipt
                
                if delta < -1:
                    remarks = "Excess Payment Received"
                elif delta > 1:
                    remarks = "Short Payment Received"
                else:
                    remarks = "Equal Payment Received"
                
                receivable_data.append({
                    "utr_number": rec.utr_number,
                    "utr_date": rec.utr_date.isoformat() if rec.utr_date else None,
                    "receivable": receivable,
                    "receipt": receipt,
                    "delta": delta,
                    "remarks": remarks
                })
            
            # Generate filename
            filename = f"receivable_vs_receipt_{len(store_codes)}_stores_{start_date_dt.strftime('%d-%m-%Y')}_{end_date_dt.strftime('%d-%m-%Y')}_{generation_id}.xlsx"
            filepath = os.path.join(reports_dir, filename)
            
            # Create Excel file with pandas
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                # Summary sheet
                summary_df = pd.DataFrame([{
                    "Report Type": "Receivable vs Receipt",
                    "Store Count": len(store_codes),
                    "Start Date": start_date_dt.strftime('%Y-%m-%d'),
                    "End Date": end_date_dt.strftime('%Y-%m-%d'),
                    "Total Records": len(receivable_data),
                    "Total Receivable": sum(r["receivable"] for r in receivable_data),
                    "Total Receipt": sum(r["receipt"] for r in receivable_data),
                    "Total Delta": sum(r["delta"] for r in receivable_data),
                    "Generated At": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                }])
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Receivable vs Receipt sheet
                if receivable_data:
                    receivables_df = pd.DataFrame(receivable_data)
                    receivables_df.to_excel(writer, sheet_name='ReceivableVsReceipt', index=False)
                
                # Update progress
                await ExcelGeneration.update_status(
                    db,
                    generation_id,
                    ExcelGenerationStatus.PROCESSING,
                    progress=50,
                    message=f"Processing {len(receivable_data)} records..."
                )
            
            # Update final status
            await ExcelGeneration.update_status(
                db,
                generation_id,
                ExcelGenerationStatus.COMPLETED,
                progress=100,
                message="Excel generation completed successfully",
                filename=filename
            )
            
            logger.info(f"[Excel Generation {generation_id}] Receivable receipt generation completed successfully")
            
    except Exception as e:
        logger.error(f"[Excel Generation {generation_id}] Error: {e}", exc_info=True)
        try:
            from app.config.database import main_session_factory, create_engines
            if not main_session_factory:
                await create_engines()
            
            async with main_session_factory() as db:
                await ExcelGeneration.update_status(
                    db,
                    generation_id,
                    ExcelGenerationStatus.FAILED,
                    message="Error generating receivable receipt Excel file",
                    error=str(e)
                )
        except Exception as update_error:
            logger.error(f"[Excel Generation {generation_id}] Failed to update error status: {update_error}")
            pass


async def process_excel_generation(generation_id: int, params: dict):
    """Process Excel generation in background - similar to Node.js worker"""
    try:
        from app.config.database import main_session_factory, create_engines
        from app.models.main.excel_generation import ExcelGeneration, ExcelGenerationStatus
        from app.models.main.reconciliation import ZomatoVsPosSummary, ThreepoDashboard
        from sqlalchemy import select, and_
        from datetime import datetime
        import pandas as pd
        import os
        
        logger.info(f"[Excel Generation {generation_id}] Starting Excel generation")
        
        # Ensure engines are created
        if not main_session_factory:
            await create_engines()
        
        async with main_session_factory() as db:
            # Update status to processing
            await ExcelGeneration.update_status(
                db,
                generation_id,
                ExcelGenerationStatus.PROCESSING,
                progress=0,
                message="Starting Excel generation..."
            )
            
            start_date = params["start_date"]
            end_date = params["end_date"]
            store_codes = params["store_codes"]
            reports_dir = params["reports_dir"]
            
            # Parse dates
            date_formats = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y"]
            start_date_dt = None
            end_date_dt = None
            
            for fmt in date_formats:
                try:
                    start_date_dt = datetime.strptime(start_date, fmt).date()
                    break
                except ValueError:
                    continue
            
            for fmt in date_formats:
                try:
                    end_date_dt = datetime.strptime(end_date, fmt).date()
                    break
                except ValueError:
                    continue
            
            # Generate filename
            filename = f"reconciliation_{len(store_codes)}_stores_{start_date_dt.strftime('%d-%m-%Y')}_{end_date_dt.strftime('%d-%m-%Y')}_{generation_id}.xlsx"
            filepath = os.path.join(reports_dir, filename)
            
            # Create Excel file with pandas
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                # Generate Summary sheet (simplified version)
                summary_df = pd.DataFrame([{
                    "Store Count": len(store_codes),
                    "Start Date": start_date_dt.strftime('%Y-%m-%d'),
                    "End Date": end_date_dt.strftime('%Y-%m-%d'),
                    "Generated At": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                }])
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Query and write Summary data
                summary_conditions = [
                    ZomatoVsPosSummary.order_date >= start_date_dt,
                    ZomatoVsPosSummary.order_date <= end_date_dt
                ]
                
                if store_codes:
                    summary_conditions.append(ZomatoVsPosSummary.store_name.in_(store_codes))
                
                summary_query = select(ZomatoVsPosSummary).where(
                    and_(*summary_conditions)
                ).limit(10000)
                
                summary_result = await db.execute(summary_query)
                summary_data = summary_result.scalars().all()
                
                if summary_data:
                    summary_records_df = pd.DataFrame([record.to_dict() for record in summary_data])
                    summary_records_df.to_excel(writer, sheet_name='Zomato vs POS Summary', index=False)
                
                # Query and write Dashboard data
                start_date_str = start_date_dt.strftime('%Y-%m-%d')
                end_date_str = end_date_dt.strftime('%Y-%m-%d')
                
                dashboard_conditions = [
                    ThreepoDashboard.business_date >= start_date_str,
                    ThreepoDashboard.business_date <= end_date_str
                ]
                
                if store_codes:
                    dashboard_conditions.append(ThreepoDashboard.store_code.in_(store_codes))
                
                dashboard_query = select(ThreepoDashboard).where(
                    and_(*dashboard_conditions)
                ).limit(10000)
                
                dashboard_result = await db.execute(dashboard_query)
                dashboard_data = dashboard_result.scalars().all()
                
                if dashboard_data:
                    dashboard_records_df = pd.DataFrame([record.to_dict() for record in dashboard_data])
                    dashboard_records_df.to_excel(writer, sheet_name='3PO Dashboard', index=False)
                
                # Update progress
                await ExcelGeneration.update_status(
                    db,
                    generation_id,
                    ExcelGenerationStatus.PROCESSING,
                    progress=50,
                    message="Excel file generated successfully"
                )
            
            # Update final status
            await ExcelGeneration.update_status(
                db,
                generation_id,
                ExcelGenerationStatus.COMPLETED,
                progress=100,
                message="Excel generation completed successfully",
                filename=filename
            )
            
            logger.info(f"[Excel Generation {generation_id}] Generation completed successfully")
            
    except Exception as e:
        logger.error(f"[Excel Generation {generation_id}] Error: {e}", exc_info=True)
        try:
            from app.config.database import main_session_factory, create_engines
            if not main_session_factory:
                await create_engines()
            
            async with main_session_factory() as db:
                await ExcelGeneration.update_status(
                    db,
                    generation_id,
                    ExcelGenerationStatus.FAILED,
                    message="Error generating Excel file",
                    error=str(e)
                )
        except Exception as update_error:
            logger.error(f"[Excel Generation {generation_id}] Failed to update error status: {update_error}")
            pass


async def process_summary_sheet_generation(generation_id: int, params: dict):
    """
    Process summary sheet Excel generation in background.
    Completely isolated from main event loop - runs in dedicated thread pool.
    Similar to Node.js fork() approach - ensures main application never blocks.
    """
    try:
        from app.config.database import main_session_factory, create_engines
        from app.models.main.excel_generation import ExcelGeneration, ExcelGenerationStatus
        from datetime import datetime
        import os
        from app.utils.summary_sheet_helper import generate_summary_sheet_to_file
        
        logger.info(f"[Summary Sheet Generation {generation_id}] Starting summary sheet generation")
        
        # Ensure engines are created
        if not main_session_factory:
            await create_engines()
        
        # Update status to processing (quick DB operation, then close connection)
        async with main_session_factory() as db:
            await ExcelGeneration.update_status(
                db,
                generation_id,
                ExcelGenerationStatus.PROCESSING,
                progress=0,
                message="Starting summary sheet generation..."
            )
        # DB connection closed here - don't hold it during heavy computation
        
        start_date = params["start_date"]
        end_date = params["end_date"]
        store_codes = params["store_codes"]
        reports_dir = params["reports_dir"]
        
        # Parse dates
        date_formats = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y"]
        start_date_dt = None
        end_date_dt = None
        
        for fmt in date_formats:
            try:
                start_date_dt = datetime.strptime(start_date, fmt).date()
                break
            except ValueError:
                continue
        
        for fmt in date_formats:
            try:
                end_date_dt = datetime.strptime(end_date, fmt).date()
                break
            except ValueError:
                continue
        
        # Generate filename
        filename = f"summary_sheet_{len(store_codes)}_stores_{start_date_dt.strftime('%d-%m-%Y')}_{end_date_dt.strftime('%d-%m-%Y')}_{generation_id}.xlsx"
        filepath = os.path.join(reports_dir, filename)
        
        # 🔥 KEY: Run heavy CPU-bound work in dedicated thread pool executor
        # This completely isolates it from the main event loop
        executor = get_task_executor()
        loop = asyncio.get_event_loop()
        
        # Update progress before starting generation
        async with main_session_factory() as db:
            await ExcelGeneration.update_status(
                db,
                generation_id,
                ExcelGenerationStatus.PROCESSING,
                progress=10,
                message="Starting Excel generation in background thread..."
            )
        # DB connection closed - heavy work runs without holding DB connection
        
        # Run in thread pool executor for true parallel processing
        # This completely blocks only the worker thread, NOT the main event loop
        def generate_with_progress():
            try:
                # Call the helper function (CPU-bound pandas operations)
                generate_summary_sheet_to_file(
                    filepath=filepath,
                    start_date_dt=start_date_dt,
                    end_date_dt=end_date_dt,
                    store_codes=store_codes,
                    progress_callback=None  # Progress handled separately via DB
                )
                return True
            except Exception as e:
                logger.error(f"[Summary Sheet Generation {generation_id}] Error in thread: {e}", exc_info=True)
                raise
        
        # This runs in a separate thread - main event loop continues normally
        await loop.run_in_executor(executor, generate_with_progress)
        
        # Update status to completed (open DB connection only when needed)
        async with main_session_factory() as db:
            await ExcelGeneration.update_status(
                db,
                generation_id,
                ExcelGenerationStatus.COMPLETED,
                progress=100,
                message="Summary sheet generation completed successfully",
                filename=filename
            )
        
        logger.info(f"[Summary Sheet Generation {generation_id}] Generation completed successfully")
            
    except Exception as e:
        logger.error(f"[Summary Sheet Generation {generation_id}] Error: {e}", exc_info=True)
        try:
            from app.config.database import main_session_factory, create_engines
            if not main_session_factory:
                await create_engines()
            
            async with main_session_factory() as db:
                await ExcelGeneration.update_status(
                    db,
                    generation_id,
                    ExcelGenerationStatus.FAILED,
                    message="Error generating summary sheet",
                    error=str(e)
                )
        except Exception as update_error:
            logger.error(f"[Summary Sheet Generation {generation_id}] Failed to update error status: {update_error}")
            pass
