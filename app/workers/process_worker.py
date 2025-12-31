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
        else:
            run_report_excel_generation(generation_id, params)
    else:
        logger.error("No arguments provided")

