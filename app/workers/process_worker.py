"""
Separate process worker for Excel generation - similar to Node.js fork()
This runs in a completely separate Python process, fully isolated from main application.
This ensures the main application NEVER blocks - true parallel processing.
"""
import sys
import os
import logging
from datetime import datetime

# 🔥 CRITICAL: Ensure we can import 'app' module in child process
# When using multiprocessing with 'spawn', child process needs proper Python path
if __name__ == "__main__" or True:  # Always ensure path setup
    # Get the parent directory to add to Python path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(os.path.dirname(current_dir))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

# Setup logging - separate from main app logger
# Log to both stdout and a file for debugging
log_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'process_worker.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [Process Worker PID:%(process)d] - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, mode='a')
    ]
)
logger = logging.getLogger(__name__)


def run_summary_sheet_generation(generation_id, params: dict):
    """
    Run summary sheet generation in a completely separate process (MongoDB-based).
    This function is called by multiprocessing.Process and runs independently.
    Similar to Node.js worker.fork() - completely isolated execution.
    
    This process has its own:
    - Memory space (not shared with main process)
    - CPU time slice (doesn't block main process)
    - Database connections (separate connection pool for MySQL, MongoDB connection for status updates)
    - Event loop (separate async context)
    """
    try:
        import asyncio
        
        # Convert generation_id to string if it's an integer (for backward compatibility)
        if isinstance(generation_id, int):
            generation_id = str(generation_id)
        
        # Set up event loop for this process (separate from main app)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        logger.info(f"[Process {generation_id}] Starting summary sheet generation in separate process")
        logger.info(f"[Process {generation_id}] Python path: {sys.path[:3]}")
        
        async def process_in_async():
            try:
                # Import inside async function to ensure proper initialization in child process
                logger.info(f"[Process {generation_id}] Importing modules...")
                import app.config.database as db_module  # Import module, not just functions
                from app.models.main.excel_generation import ExcelGeneration, ExcelGenerationStatus
                from app.utils.summary_sheet_helper import generate_summary_sheet_to_file
                logger.info(f"[Process {generation_id}] Modules imported successfully")
                
                # Create engines for THIS process (separate connection pool for MySQL queries)
                logger.info(f"[Process {generation_id}] Creating database engines...")
                await db_module.create_engines()
                logger.info(f"[Process {generation_id}] Database engines created")
                
                # 🔥 CRITICAL: Access main_session_factory directly from module AFTER create_engines()
                # In child process, we need to access it from the module object, not via import
                if db_module.main_session_factory is None:
                    raise RuntimeError("main_session_factory is None after create_engines()")
                
                logger.info(f"[Process {generation_id}] Session factory ready: {db_module.main_session_factory is not None}")
            except Exception as import_error:
                logger.error(f"[Process {generation_id}] Import/Initialization error: {import_error}", exc_info=True)
                raise
            
            # Update status to processing (MongoDB - no db session needed)
            await ExcelGeneration.update_status(
                None,  # db parameter not needed for MongoDB
                generation_id,
                ExcelGenerationStatus.PROCESSING,
                progress=0,
                message="Starting summary sheet generation in separate process..."
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
            
            if not start_date_dt:
                raise ValueError(f"Invalid start_date format: {start_date}")
            
            for fmt in date_formats:
                try:
                    end_date_dt = datetime.strptime(end_date, fmt).date()
                    break
                except ValueError:
                    continue
            
            if not end_date_dt:
                raise ValueError(f"Invalid end_date format: {end_date}")
            
            # Generate filename
            filename = f"summary_sheet_{len(store_codes)}_stores_{start_date_dt.strftime('%d-%m-%Y')}_{end_date_dt.strftime('%d-%m-%Y')}_{generation_id}.xlsx"
            filepath = os.path.join(reports_dir, filename)
            
            # Update progress before starting heavy work (MongoDB - no db session needed)
            await ExcelGeneration.update_status(
                None,  # db parameter not needed for MongoDB
                generation_id,
                ExcelGenerationStatus.PROCESSING,
                progress=10,
                message="Starting Excel generation in separate process..."
            )
            
            # 🔥 HEAVY CPU-BOUND WORK - blocks only THIS process, NOT main app
            logger.info(f"[Process {generation_id}] Starting Excel generation (CPU-bound work)")
            logger.info(f"[Process {generation_id}] This work runs in separate process - main app is NOT blocked")
            logger.info(f"[Process {generation_id}] Filepath: {filepath}")
            logger.info(f"[Process {generation_id}] Date range: {start_date_dt} to {end_date_dt}")
            logger.info(f"[Process {generation_id}] Store count: {len(store_codes)}")
            
            # Update progress before heavy work starts (MongoDB - no db session needed)
            await ExcelGeneration.update_status(
                None,  # db parameter not needed for MongoDB
                generation_id,
                ExcelGenerationStatus.PROCESSING,
                progress=15,
                message="Excel generation in progress - querying database..."
            )
            
            # This is where the heavy pandas/Excel work happens
            # It blocks THIS process, but main application continues normally
            try:
                logger.info(f"[Process {generation_id}] Calling generate_summary_sheet_to_file...")
                generate_summary_sheet_to_file(
                    filepath=filepath,
                    start_date_dt=start_date_dt,
                    end_date_dt=end_date_dt,
                    store_codes=store_codes,
                    progress_callback=None
                )
                logger.info(f"[Process {generation_id}] Excel generation completed successfully")
            except Exception as excel_error:
                logger.error(f"[Process {generation_id}] Error in Excel generation: {excel_error}", exc_info=True)
                # Update error status before re-raising (MongoDB - no db session needed)
                await ExcelGeneration.update_status(
                    None,  # db parameter not needed for MongoDB
                    generation_id,
                    ExcelGenerationStatus.FAILED,
                    message=f"Error during Excel generation: {str(excel_error)[:200]}",
                    error=str(excel_error)[:500]
                )
                raise
            
            logger.info(f"[Process {generation_id}] Excel generation completed, updating status...")
            
            # Update progress to 90% before finalizing (MongoDB - no db session needed)
            await ExcelGeneration.update_status(
                None,  # db parameter not needed for MongoDB
                generation_id,
                ExcelGenerationStatus.PROCESSING,
                progress=90,
                message="Excel file generated, finalizing..."
            )
            
            # Update status to completed (MongoDB - no db session needed)
            await ExcelGeneration.update_status(
                None,  # db parameter not needed for MongoDB
                generation_id,
                ExcelGenerationStatus.COMPLETED,
                progress=100,
                message="Summary sheet generation completed successfully",
                filename=filename
            )
            
            logger.info(f"[Process {generation_id}] Generation completed successfully")
        
        # Run the async function in this process's event loop
        loop.run_until_complete(process_in_async())
        
    except Exception as e:
        logger.error(f"[Process {generation_id}] Error: {e}", exc_info=True)
        try:
            import asyncio
            from app.models.main.excel_generation import ExcelGeneration, ExcelGenerationStatus
            
            # Try to update error status (MongoDB - no db session needed)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            async def update_error():
                await ExcelGeneration.update_status(
                    None,  # db parameter not needed for MongoDB
                    generation_id,
                    ExcelGenerationStatus.FAILED,
                    message="Error generating summary sheet",
                    error=str(e)[:500]  # Limit error message length
                )
            
            loop.run_until_complete(update_error())
            loop.close()
        except Exception as update_error:
            logger.error(f"[Process {generation_id}] Failed to update error status: {update_error}")
        
        # Exit with error code
        sys.exit(1)
    
    finally:
        # Clean up event loop
        try:
            loop.close()
        except:
            pass
        
        logger.info(f"[Process {generation_id}] Process finished")


def run_report_excel_generation(generation_id, params: dict):
    """
    Run report Excel generation in a completely separate process (MongoDB-based).
    This function is called by multiprocessing.Process and runs independently.
    Similar to Node.js worker.fork() - completely isolated execution.
    
    This process has its own:
    - Memory space (not shared with main process)
    - CPU time slice (doesn't block main process)
    - Database connections (separate MongoDB connection for queries and status updates)
    - Event loop (separate async context)
    """
    try:
        import asyncio
        
        # Convert generation_id to string if it's an integer (for backward compatibility)
        if isinstance(generation_id, int):
            generation_id = str(generation_id)
        
        # Set up event loop for this process (separate from main app)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        logger.info(f"[Process {generation_id}] Starting report Excel generation in separate process")
        logger.info(f"[Process {generation_id}] Python path: {sys.path[:3]}")
        
        async def process_in_async():
            try:
                # Import inside async function to ensure proper initialization in child process
                logger.info(f"[Process {generation_id}] Importing modules...")
                from app.models.main.excel_generation import ExcelGeneration, ExcelGenerationStatus
                from app.services.mongodb_service import mongodb_service
                from datetime import datetime
                import pandas as pd
                logger.info(f"[Process {generation_id}] Modules imported successfully")
            except Exception as import_error:
                logger.error(f"[Process {generation_id}] Import/Initialization error: {import_error}", exc_info=True)
                raise
            
            # Update status to processing (MongoDB - no db session needed)
            await ExcelGeneration.update_status(
                None,  # db parameter not needed for MongoDB
                generation_id,
                ExcelGenerationStatus.PROCESSING,
                progress=10,
                message="Starting report Excel generation in separate process..."
            )
            
            # Extract parameters
            report_name = params["report_name"]
            columns = params["columns"]
            start_date_str = params["start_date"]
            end_date_str = params["end_date"]
            start_date_dt_str = params.get("start_date_dt")
            end_date_dt_str = params.get("end_date_dt")
            reports_dir = params["reports_dir"]
            
            # Parse dates
            date_formats = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d-%m-%Y"]
            start_date_dt = None
            end_date_dt = None
            
            # Try to parse from ISO format first (from start_date_dt_str)
            if start_date_dt_str:
                try:
                    start_date_dt = datetime.fromisoformat(start_date_dt_str).date()
                except:
                    pass
            
            if end_date_dt_str:
                try:
                    end_date_dt = datetime.fromisoformat(end_date_dt_str).date()
                except:
                    pass
            
            # Fallback to parsing from string formats
            if not start_date_dt:
                for fmt in date_formats:
                    try:
                        start_date_dt = datetime.strptime(start_date_str, fmt).date()
                        break
                    except ValueError:
                        continue
            
            if not end_date_dt:
                for fmt in date_formats:
                    try:
                        end_date_dt = datetime.strptime(end_date_str, fmt).date()
                        break
                    except ValueError:
                        continue
            
            if not start_date_dt or not end_date_dt:
                raise ValueError(f"Invalid date format. start_date: {start_date_str}, end_date: {end_date_str}")
            
            # 🔥 Create reports directory if it doesn't exist
            os.makedirs(reports_dir, exist_ok=True)
            
            # Update progress
            await ExcelGeneration.update_status(
                None,
                generation_id,
                ExcelGenerationStatus.PROCESSING,
                progress=20,
                message="Validating collection and querying data..."
            )
            
            # 🔥 HEAVY CPU-BOUND WORK - blocks only THIS process, NOT main app
            logger.info(f"[Process {generation_id}] Starting MongoDB query (CPU-bound work)")
            logger.info(f"[Process {generation_id}] This work runs in separate process - main app is NOT blocked")
            logger.info(f"[Process {generation_id}] Report: {report_name}, Date range: {start_date_dt} to {end_date_dt}")
            logger.info(f"[Process {generation_id}] Columns: {len(columns)}")
            
            # Query MongoDB collection for data
            # 🔥 Collection validation happens here - if collection doesn't exist, it will fail gracefully
            try:
                # Convert date objects to datetime for MongoDB query
                start_datetime = datetime.combine(start_date_dt, datetime.min.time())
                end_datetime = datetime.combine(end_date_dt, datetime.max.time())
                
                # Query collection with date filter on order_date field
                collection_name = report_name.lower().strip()
                
                # 🔥 Validate collection exists by trying to query (background worker handles this)
                data = mongodb_service.query_collection_by_date_range(
                    collection_name=collection_name,
                    columns=columns,
                    start_date=start_datetime,
                    end_date=end_datetime,
                    date_field="order_date"
                )
                
                logger.info(f"[Process {generation_id}] Retrieved {len(data)} record(s) from collection '{collection_name}'")
                
            except ValueError as e:
                error_msg = str(e)
                if "does not exist" in error_msg or "not found" in error_msg.lower():
                    error_message = f"Collection '{collection_name}' does not exist in MongoDB"
                    await ExcelGeneration.update_status(
                        None,
                        generation_id,
                        ExcelGenerationStatus.FAILED,
                        message=error_message,
                        error=error_message
                    )
                    raise ValueError(error_message)
                else:
                    raise ValueError(error_msg)
            except ConnectionError as e:
                error_message = f"MongoDB connection error: {str(e)}"
                await ExcelGeneration.update_status(
                    None,
                    generation_id,
                    ExcelGenerationStatus.FAILED,
                    message=error_message,
                    error=error_message
                )
                raise ConnectionError(error_message)
            
            # Update progress
            await ExcelGeneration.update_status(
                None,
                generation_id,
                ExcelGenerationStatus.PROCESSING,
                progress=50,
                message=f"Processing {len(data)} record(s)..."
            )
            
            # 🔥 HEAVY PANDAS OPERATIONS - blocks only THIS process
            logger.info(f"[Process {generation_id}] Creating DataFrame from {len(data)} records...")
            
            # Create DataFrame from MongoDB data
            if data:
                # Create DataFrame with data
                df = pd.DataFrame(data)
                # Ensure columns are in the order specified in request
                # Only include columns that exist in the data
                available_columns = [col for col in columns if col in df.columns]
                if available_columns:
                    df = df[available_columns]
                else:
                    # If none of the requested columns exist, use all available columns
                    logger.warning(f"[Process {generation_id}] None of the requested columns found, using all available columns")
            else:
                # No data found, create empty DataFrame with specified columns
                df = pd.DataFrame(columns=columns)
                logger.info(f"[Process {generation_id}] No data found for the specified date range")
            
            logger.info(f"[Process {generation_id}] DataFrame created: {len(df)} rows, {len(df.columns)} columns")
            
            # Update progress
            await ExcelGeneration.update_status(
                None,
                generation_id,
                ExcelGenerationStatus.PROCESSING,
                progress=80,
                message="Generating Excel file..."
            )
            
            # Generate filename: report_name_start_date_to_end_date_generation_id.xlsx
            filename = f"{report_name}_{start_date_dt.strftime('%Y-%m-%d')}_to_{end_date_dt.strftime('%Y-%m-%d')}_{generation_id}.xlsx"
            filepath = os.path.join(reports_dir, filename)
            
            # 🔥 HEAVY EXCEL GENERATION - blocks only THIS process
            logger.info(f"[Process {generation_id}] Generating Excel file: {filepath}")
            logger.info(f"[Process {generation_id}] This Excel generation runs in separate process - main app is NOT blocked")
            
            # Create Excel file
            try:
                with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name='Report', index=False)
                
                logger.info(f"[Process {generation_id}] Generated Excel file: {filename} with {len(df)} row(s) and columns: {list(df.columns)}")
            except Exception as excel_error:
                logger.error(f"[Process {generation_id}] Error in Excel generation: {excel_error}", exc_info=True)
                # Update error status before re-raising
                await ExcelGeneration.update_status(
                    None,
                    generation_id,
                    ExcelGenerationStatus.FAILED,
                    message=f"Error during Excel generation: {str(excel_error)[:200]}",
                    error=str(excel_error)[:500]
                )
                raise
            
            logger.info(f"[Process {generation_id}] Excel generation completed, updating status...")
            
            # Update progress to 90% before finalizing
            await ExcelGeneration.update_status(
                None,
                generation_id,
                ExcelGenerationStatus.PROCESSING,
                progress=90,
                message="Excel file generated, finalizing..."
            )
            
            # Update status to completed (MongoDB - no db session needed)
            await ExcelGeneration.update_status(
                None,  # db parameter not needed for MongoDB
                generation_id,
                ExcelGenerationStatus.COMPLETED,
                progress=100,
                message="Excel generation completed successfully",
                filename=filename
            )
            
            logger.info(f"[Process {generation_id}] Generation completed successfully")
        
        # Run the async function in this process's event loop
        loop.run_until_complete(process_in_async())
        
    except Exception as e:
        logger.error(f"[Process {generation_id}] Error: {e}", exc_info=True)
        try:
            import asyncio
            from app.models.main.excel_generation import ExcelGeneration, ExcelGenerationStatus
            
            # Try to update error status (MongoDB - no db session needed)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            async def update_error():
                await ExcelGeneration.update_status(
                    None,  # db parameter not needed for MongoDB
                    generation_id,
                    ExcelGenerationStatus.FAILED,
                    message="Error generating report Excel file",
                    error=str(e)[:500]  # Limit error message length
                )
            
            loop.run_until_complete(update_error())
            loop.close()
        except Exception as update_error:
            logger.error(f"[Process {generation_id}] Failed to update error status: {update_error}")
        
        # Exit with error code
        sys.exit(1)
    
    finally:
        # Clean up event loop
        try:
            loop.close()
        except:
            pass
        
        logger.info(f"[Process {generation_id}] Process finished")


def run_summary_report_excel_generation(generation_id, params: dict):
    """
    Run summary report Excel generation in a completely separate process (MongoDB-based).
    This function is called by multiprocessing.Process and runs independently.
    Similar to Node.js worker.fork() - completely isolated execution.
    
    This process generates a summary report with:
    1. Auto-selected columns in sequence (base columns + delta columns + status/reason)
    2. Two Excel sheets: "Report" (main data) and "Summary" (additional info)
    """
    try:
        import asyncio
        
        # Convert generation_id to string if it's an integer
        if isinstance(generation_id, int):
            generation_id = str(generation_id)
        
        # Set up event loop for this process
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        logger.info(f"[Process {generation_id}] Starting summary report Excel generation in separate process")
        
        async def process_in_async():
            try:
                from app.models.main.excel_generation import ExcelGeneration, ExcelGenerationStatus
                from app.services.mongodb_service import mongodb_service
                from app.controllers.formulas_controller import FormulasController
                from datetime import datetime
                import pandas as pd
                import os
            except Exception as import_error:
                logger.error(f"[Process {generation_id}] Import error: {import_error}", exc_info=True)
                raise
            
            # Update status to processing
            await ExcelGeneration.update_status(
                None,
                generation_id,
                ExcelGenerationStatus.PROCESSING,
                progress=10,
                message="Starting summary report generation..."
            )
            
            # Extract parameters
            report_name = params["report_name"]
            start_date_str = params["start_date"]
            end_date_str = params["end_date"]
            start_date_dt_str = params.get("start_date_dt")
            end_date_dt_str = params.get("end_date_dt")
            reports_dir = params["reports_dir"]
            
            # Parse dates
            date_formats = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]
            start_date_dt = None
            end_date_dt = None
            
            if start_date_dt_str:
                try:
                    start_date_dt = datetime.fromisoformat(start_date_dt_str).date()
                except:
                    pass
            
            if end_date_dt_str:
                try:
                    end_date_dt = datetime.fromisoformat(end_date_dt_str).date()
                except:
                    pass
            
            if not start_date_dt:
                for fmt in date_formats:
                    try:
                        start_date_dt = datetime.strptime(start_date_str, fmt).date()
                        break
                    except ValueError:
                        continue
            
            if not end_date_dt:
                for fmt in date_formats:
                    try:
                        end_date_dt = datetime.strptime(end_date_str, fmt).date()
                        break
                    except ValueError:
                        continue
            
            if not start_date_dt or not end_date_dt:
                raise ValueError(f"Invalid date format. start_date: {start_date_str}, end_date: {end_date_str}")
            
            # Create reports directory
            os.makedirs(reports_dir, exist_ok=True)
            
            # Update progress
            await ExcelGeneration.update_status(
                None,
                generation_id,
                ExcelGenerationStatus.PROCESSING,
                progress=20,
                message="Constructing column sequence..."
            )
            
            # Construct column sequence
            collection_name = report_name.lower().strip()
            
            # Get all available columns
            try:
                all_keys = mongodb_service.get_all_collection_keys(collection_name)
            except ValueError as e:
                error_message = f"Collection '{collection_name}' does not exist in MongoDB"
                await ExcelGeneration.update_status(
                    None,
                    generation_id,
                    ExcelGenerationStatus.FAILED,
                    message=error_message,
                    error=error_message
                )
                raise ValueError(error_message)
            
            # Get delta columns
            formulas_controller = FormulasController()
            delta_columns_data = await formulas_controller.get_delta_columns(report_name)
            delta_columns = delta_columns_data.get("data", {}).get("delta_columns", []) if delta_columns_data else []
            
            # Create case-insensitive mapping from all_keys (formula names -> actual column names)
            # This handles cases where formulas are uppercase but actual columns are lowercase
            key_lower_map = {key.lower(): key for key in all_keys}
            
            # Helper function to find actual column name (case-insensitive)
            def find_actual_column(formula_name):
                if not formula_name:
                    return None
                # Try exact match first
                if formula_name in all_keys:
                    return formula_name
                # Try case-insensitive match
                formula_lower = formula_name.lower()
                if formula_lower in key_lower_map:
                    return key_lower_map[formula_lower]
                return None
            
            # Build column sequence
            column_sequence = []
            used_columns = set()
            
            # Map delta formulas to actual column names and identify delta-related columns
            delta_related_columns = set()
            processed_deltas = []  # Store processed deltas with actual column names
            
            for delta in delta_columns:
                first_formula = delta.get("first_formula")
                second_formula = delta.get("second_formula")
                delta_column_name = delta.get("delta_column_name")
                
                # Find actual column names (case-insensitive)
                actual_first = find_actual_column(first_formula)
                actual_second = find_actual_column(second_formula)
                actual_delta = find_actual_column(delta_column_name)
                
                if actual_first:
                    delta_related_columns.add(actual_first)
                if actual_second:
                    delta_related_columns.add(actual_second)
                if actual_delta:
                    delta_related_columns.add(actual_delta)
                
                # Store processed delta with actual column names
                if actual_first or actual_second or actual_delta:
                    processed_deltas.append({
                        'first': actual_first,
                        'second': actual_second,
                        'delta': actual_delta
                    })
            
            # Status and reason field names to exclude from base columns
            status_reason_fields = {
                'reconciliation_status', 'reconciled_status', 'reconc_status',
                'reason', 'pos_reason', 'trm_reason', 'zomato_vs_pos_reason', 
                'pos_vs_zomato_reason', '_id'
            }
            
            # Add base columns first (columns not part of any delta and not status/reason)
            # IMPORTANT: Exclude delta-related columns from base columns - they'll be added in sequence later
            for col in all_keys:
                if col not in delta_related_columns and col not in status_reason_fields:
                    column_sequence.append(col)
                    used_columns.add(col)
            
            # For each delta, add: first_formula, second_formula, delta_column_name in sequence
            # IMPORTANT: Add columns in exact sequence for each delta (Column A, Column B, Delta)
            # The columns used to calculate the delta MUST appear BEFORE the delta itself
            logger.info(f"[Summary Report Generation {generation_id}] Processing {len(processed_deltas)} delta(s)")
            
            for idx, delta_info in enumerate(processed_deltas):
                actual_first = delta_info['first']
                actual_second = delta_info['second']
                actual_delta = delta_info['delta']
                
                logger.info(f"[Summary Report Generation {generation_id}] Delta {idx + 1}: {actual_first} -> {actual_second} -> {actual_delta}")
                
                # If any of these columns are already in sequence (from base columns), remove them first
                # so we can add them in the correct delta sequence
                if actual_first and actual_first in column_sequence:
                    # Remove it from current position to re-add in correct sequence
                    column_sequence.remove(actual_first)
                    logger.info(f"[Summary Report Generation {generation_id}] Removed {actual_first} from base columns to add in delta sequence")
                if actual_second and actual_second in column_sequence:
                    column_sequence.remove(actual_second)
                    logger.info(f"[Summary Report Generation {generation_id}] Removed {actual_second} from base columns to add in delta sequence")
                if actual_delta and actual_delta in column_sequence:
                    column_sequence.remove(actual_delta)
                    logger.info(f"[Summary Report Generation {generation_id}] Removed {actual_delta} from base columns to add in delta sequence")
                
                # Now add them in the correct sequence: Column A (first), Column B (second), Delta
                # This ensures the columns used to calculate the delta appear BEFORE the delta
                if actual_first:
                    column_sequence.append(actual_first)
                    used_columns.add(actual_first)
                    logger.info(f"[Summary Report Generation {generation_id}] Added Column A: {actual_first}")
                
                if actual_second:
                    column_sequence.append(actual_second)
                    used_columns.add(actual_second)
                    logger.info(f"[Summary Report Generation {generation_id}] Added Column B: {actual_second}")
                
                if actual_delta:
                    column_sequence.append(actual_delta)
                    used_columns.add(actual_delta)
                    logger.info(f"[Summary Report Generation {generation_id}] Added Delta: {actual_delta}")
            
            # Add reconciliation status and reason at the end
            status_fields = ['reconciliation_status', 'reconciled_status', 'reconc_status']
            reason_fields = ['reason', 'pos_reason', 'trm_reason', 'zomato_vs_pos_reason', 'pos_vs_zomato_reason']
            
            for field in status_fields:
                if field in all_keys and field not in used_columns:
                    column_sequence.append(field)
                    used_columns.add(field)
            
            for field in reason_fields:
                if field in all_keys and field not in used_columns:
                    column_sequence.append(field)
                    used_columns.add(field)
            
            logger.info(f"[Summary Report Generation {generation_id}] Column sequence constructed: {len(column_sequence)} columns")
            
            # Update progress
            await ExcelGeneration.update_status(
                None,
                generation_id,
                ExcelGenerationStatus.PROCESSING,
                progress=30,
                message="Querying data from MongoDB..."
            )
            
            # Query MongoDB collection
            start_datetime = datetime.combine(start_date_dt, datetime.min.time())
            end_datetime = datetime.combine(end_date_dt, datetime.max.time())
            
            try:
                data = mongodb_service.query_collection_by_date_range(
                    collection_name=collection_name,
                    columns=column_sequence,
                    start_date=start_datetime,
                    end_date=end_datetime,
                    date_field="order_date"
                )
                logger.info(f"[Summary Report Generation {generation_id}] Retrieved {len(data)} record(s)")
            except Exception as query_error:
                error_message = f"Error querying collection: {str(query_error)}"
                await ExcelGeneration.update_status(
                    None,
                    generation_id,
                    ExcelGenerationStatus.FAILED,
                    message=error_message,
                    error=error_message
                )
                raise
            
            # Update progress
            await ExcelGeneration.update_status(
                None,
                generation_id,
                ExcelGenerationStatus.PROCESSING,
                progress=50,
                message=f"Processing {len(data)} record(s)..."
            )
            
            # Create DataFrame
            if data:
                df = pd.DataFrame(data)
                # Ensure columns are in the correct sequence
                available_columns = [col for col in column_sequence if col in df.columns]
                if available_columns:
                    df = df[available_columns]
                else:
                    logger.warning(f"[Summary Report Generation {generation_id}] None of the requested columns found, using all available columns")
            else:
                df = pd.DataFrame(columns=column_sequence)
                logger.info(f"[Summary Report Generation {generation_id}] No data found for the specified date range")
            
            # Calculate summary statistics
            total_orders = len(data)
            
            # Find reconciliation status field
            status_field = None
            for field in ['reconciliation_status', 'reconciled_status', 'reconc_status']:
                if field in df.columns:
                    status_field = field
                    break
            
            reconciled_count = 0
            unreconciled_count = 0
            
            if status_field:
                reconciled_count = len(df[df[status_field].astype(str).str.upper().str.contains('RECONCILED', na=False)])
                unreconciled_count = len(df[df[status_field].astype(str).str.upper().str.contains('UNRECONCILED', na=False)])
            else:
                # If no status field, try to infer from data
                reconciled_count = 0
                unreconciled_count = total_orders
            
            # Count orders for both sides (try to find order count fields)
            # This is a simplified version - you may need to adjust based on your data structure
            side1_orders = total_orders  # Default to total if we can't determine
            side2_orders = total_orders  # Default to total if we can't determine
            
            # Try to find distinct order identifiers
            order_id_fields = ['order_id', 'transaction_id', 'mapping_key', 'zomato_mapping_key', 'pos_transaction_id']
            for field in order_id_fields:
                if field in df.columns:
                    side1_orders = df[field].nunique()
                    break
            
            # Update progress
            await ExcelGeneration.update_status(
                None,
                generation_id,
                ExcelGenerationStatus.PROCESSING,
                progress=80,
                message="Generating Excel file with two sheets..."
            )
            
            # Generate filename
            filename = f"summary_{report_name}_{start_date_dt.strftime('%Y-%m-%d')}_to_{end_date_dt.strftime('%Y-%m-%d')}_{generation_id}.xlsx"
            filepath = os.path.join(reports_dir, filename)
            
            # Create Excel file with two sheets
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                # Sheet 1: Main Report Data
                df.to_excel(writer, sheet_name='Report', index=False)
                
                # Sheet 2: Summary Information
                summary_data = {
                    'Field': [
                        'Start Date',
                        'End Date',
                        'Total Orders (Side 1)',
                        'Total Orders (Side 2)',
                        'Reconciled Count',
                        'Unreconciled Count'
                    ],
                    'Value': [
                        start_date_dt.strftime('%Y-%m-%d'),
                        end_date_dt.strftime('%Y-%m-%d'),
                        side1_orders,
                        side2_orders,
                        reconciled_count,
                        unreconciled_count
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            logger.info(f"[Summary Report Generation {generation_id}] Generated Excel file: {filename} with {len(df)} row(s)")
            
            # Update final status
            await ExcelGeneration.update_status(
                None,
                generation_id,
                ExcelGenerationStatus.COMPLETED,
                progress=100,
                message="Summary report generation completed successfully",
                filename=filename
            )
            
            logger.info(f"[Summary Report Generation {generation_id}] Generation completed successfully")
        
        # Run async function
        loop.run_until_complete(process_in_async())
        
    except Exception as e:
        logger.error(f"[Summary Report Generation {generation_id}] Error: {e}", exc_info=True)
        try:
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            from app.models.main.excel_generation import ExcelGeneration, ExcelGenerationStatus
            
            async def update_error():
                await ExcelGeneration.update_status(
                    None,
                    generation_id,
                    ExcelGenerationStatus.FAILED,
                    message="Error generating summary report",
                    error=str(e)[:500]
                )
            
            loop.run_until_complete(update_error())
        except Exception as update_error:
            logger.error(f"[Summary Report Generation {generation_id}] Failed to update error status: {update_error}")
    
    finally:
        # Clean up event loop
        try:
            loop.close()
        except:
            pass
        
        logger.info(f"[Summary Report Generation {generation_id}] Process finished")


if __name__ == "__main__":
    # This allows the script to be run directly for testing
    # In production, it's called via multiprocessing.Process
    import json
    if len(sys.argv) > 1:
        generation_id = sys.argv[1]
        params = json.loads(sys.argv[2])
        # Determine which function to call based on params
        if "store_codes" in params:
            run_summary_sheet_generation(generation_id, params)
        elif params.get("report_type") == "summary" or "report_name" in params and "columns" not in params:
            run_summary_report_excel_generation(generation_id, params)
        else:
            run_report_excel_generation(generation_id, params)
    else:
        logger.error("No arguments provided")

