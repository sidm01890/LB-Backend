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


def run_summary_sheet_generation(generation_id: int, params: dict):
    """
    Run summary sheet generation in a completely separate process.
    This function is called by multiprocessing.Process and runs independently.
    Similar to Node.js worker.fork() - completely isolated execution.
    
    This process has its own:
    - Memory space (not shared with main process)
    - CPU time slice (doesn't block main process)
    - Database connections (separate connection pool)
    - Event loop (separate async context)
    """
    try:
        import asyncio
        
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
                
                # Create engines for THIS process (separate connection pool)
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
            
            # Update status to processing - use module reference directly
            async with db_module.main_session_factory() as db:
                await ExcelGeneration.update_status(
                    db,
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
            
            # Update progress before starting heavy work
            # Use module reference directly (already imported above)
            async with db_module.main_session_factory() as db:
                await ExcelGeneration.update_status(
                    db,
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
            
            # Update progress before heavy work starts
            async with db_module.main_session_factory() as db:
                await ExcelGeneration.update_status(
                    db,
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
                # Update error status before re-raising
                async with db_module.main_session_factory() as db:
                    await ExcelGeneration.update_status(
                        db,
                        generation_id,
                        ExcelGenerationStatus.FAILED,
                        message=f"Error during Excel generation: {str(excel_error)[:200]}",
                        error=str(excel_error)[:500]
                    )
                raise
            
            logger.info(f"[Process {generation_id}] Excel generation completed, updating status...")
            
            # Update progress to 90% before finalizing
            async with db_module.main_session_factory() as db:
                await ExcelGeneration.update_status(
                    db,
                    generation_id,
                    ExcelGenerationStatus.PROCESSING,
                    progress=90,
                    message="Excel file generated, finalizing..."
                )
            
            # Update status to completed
            # Use module reference directly (already imported above)
            async with db_module.main_session_factory() as db:
                await ExcelGeneration.update_status(
                    db,
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
            import app.config.database as db_module
            from app.models.main.excel_generation import ExcelGeneration, ExcelGenerationStatus
            
            # Try to update error status
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            async def update_error():
                await db_module.create_engines()
                # Access from module directly AFTER create_engines
                if db_module.main_session_factory is None:
                    raise RuntimeError("main_session_factory is None in error handler")
                async with db_module.main_session_factory() as db:
                    await ExcelGeneration.update_status(
                        db,
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


if __name__ == "__main__":
    # This allows the script to be run directly for testing
    # In production, it's called via multiprocessing.Process
    import json
    if len(sys.argv) > 1:
        generation_id = int(sys.argv[1])
        params = json.loads(sys.argv[2])
        run_summary_sheet_generation(generation_id, params)
    else:
        logger.error("No arguments provided")

