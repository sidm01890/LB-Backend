"""
Application startup script
"""

import uvicorn
from app.main import app
from app.config.database import create_engines, test_connections
import asyncio
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def startup():
    """Application startup tasks"""
    try:
        # Create database engines
        await create_engines()
        
        # Test database connections
        await test_connections()
        
        logger.info("Application startup completed successfully")
        
    except Exception as e:
        logger.error(f"Application startup failed: {e}")
        raise


if __name__ == "__main__":
    # Run startup tasks
    asyncio.run(startup())
    
    # Start the application
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8034,
        reload=True,
        log_level="info"
    )
