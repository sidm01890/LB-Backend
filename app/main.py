"""
FastAPI Reconcii Admin Backend Application
Main application entry point this file is used to start the application.
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
import asyncio
import logging
from app.config.database import create_engines, test_connections, close_connections
from app.config.executor import create_task_executor, shutdown_task_executor
from app.config.settings import settings, validate_environment
from app.workers.tasks import run_scheduled_tasks
from app.workers.formula_watcher import start_formula_watcher, stop_formula_watcher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Validate environment on startup
try:
    validate_environment()
except ValueError as e:
    logger.error(f"Environment validation failed: {e}")
    exit(1)

# Import models to ensure they are registered with SQLAlchemy
# This must be done after database configuration is set up
from app.models.sso import (
    UserDetails, Organization, Tool, Module, Group, Permission, 
    AuditLog, Upload, OrganizationTool, GroupModuleMapping, UserModuleMapping,
    # Sheet Data Models (now in SSO for compatibility)
    ZomatoPosVs3poData, Zomato3poVsPosData, Zomato3poVsPosRefundData,
    OrdersNotInPosData, OrdersNotIn3poData,
    # Reconciliation Models (now in SSO for compatibility)
    ZomatoVsPosSummary, ThreepoDashboard, Store, Trm
)
from app.models.main import (
    # Main database models
    Orders, UploadRecord
)

# Import routes
from app.routes import auth, users, organizations, tools, modules, groups, permissions, audit_log, reconciliation, uploader, sheet_data

# Create FastAPI application
app = FastAPI(
    title="Reconcii Admin API",
    description="API documentation for Reconcii Admin Backend",
    version="1.0.0",
    docs_url="/api-docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"message": "Internal server error", "detail": str(exc)}
    )

# Startup event
@app.on_event("startup")
async def startup_event():
    """Application startup tasks"""
    try:
        logger.info("🚀 Starting Reconcii Admin API...")
        
        # Create database engines
        logger.info("📊 Initializing database connections...")
        await create_engines()
        
        # Test database connections
        logger.info("🔍 Testing database connections...")
        await test_connections()
        
        # Initialize task executor for parallel processing
        logger.info("⚙️ Initializing task executor for parallel processing...")
        create_task_executor(max_workers=settings.task_executor_workers)

        # Start formula watcher scheduler
        await start_formula_watcher()
        
        logger.info("✅ Database connections established successfully")
        logger.info("✅ Task executor initialized for parallel processing")
        logger.info("✅ Application startup completed")
        logger.info(f"🌐 API Documentation available at: http://localhost:{settings.port}/api-docs")
        
    except Exception as e:
        logger.error(f"❌ Application startup failed: {e}")
        logger.error("Application will not start due to startup errors")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown tasks"""
    try:
        logger.info("🛑 Shutting down application...")
        
        # Shutdown task executor
        await shutdown_task_executor()

        # Stop formula watcher
        await stop_formula_watcher()
        
        # Close database connections
        await close_connections()
        
        logger.info("✅ Application shutdown completed")
        
    except Exception as e:
        logger.error(f"❌ Error during shutdown: {e}")

# Health check endpoint
@app.get("/health")
async def health_check():
    """Enhanced health check endpoint"""
    return {
        "status": "healthy", 
        "message": "Reconcii Admin API is running",
        "version": "1.0.0",
        "environment": settings.environment,
        "timestamp": asyncio.get_event_loop().time()
    }

# Include routers
app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])
app.include_router(users.router, prefix="/api/user", tags=["Users"])
app.include_router(organizations.router, prefix="/api/organization", tags=["Organizations"])
app.include_router(tools.router, prefix="/api/tool", tags=["Tools"])
app.include_router(modules.router, prefix="/api/module", tags=["Modules"])
app.include_router(groups.router, prefix="/api/group", tags=["Groups"])
app.include_router(permissions.router, prefix="/api/permission", tags=["Permissions"])
app.include_router(audit_log.router, prefix="/api/audit_log", tags=["Audit Logs"])
app.include_router(reconciliation.router, prefix="/api/reconciliation", tags=["Reconciliation"])
app.include_router(uploader.router, prefix="/api/uploader", tags=["File Upload"])
app.include_router(sheet_data.router, prefix="/api/sheet-data", tags=["Sheet Data"])

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug
    )
