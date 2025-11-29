"""
Database configuration and connection management
"""

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import text
from app.config.settings import settings, get_database_urls
import logging

# Note: Model imports are handled in main.py to avoid circular imports

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create declarative base
Base = declarative_base()

# Database engines
sso_engine = None
main_engine = None
sso_session_factory = None
main_session_factory = None


async def create_engines():
    """Create database engines"""
    global sso_engine, main_engine, sso_session_factory, main_session_factory
    
    try:
        sso_url, main_url = get_database_urls()
        
        # SSO Database Engine
        sso_engine = create_async_engine(
            sso_url,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_timeout=settings.db_pool_timeout,
            pool_recycle=settings.db_pool_recycle,
            echo=settings.debug,
        )
        
        # Main Database Engine
        main_engine = create_async_engine(
            main_url,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_timeout=settings.db_pool_timeout,
            pool_recycle=settings.db_pool_recycle,
            echo=settings.debug,
        )
        
        # Create session factories
        sso_session_factory = async_sessionmaker(
            sso_engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        
        main_session_factory = async_sessionmaker(
            main_engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        
        logger.info("Database engines created successfully")
        
    except Exception as e:
        logger.error(f"Failed to create database engines: {e}")
        raise


async def test_connections():
    """Test database connections"""
    try:
        # Test SSO database connection
        async with sso_engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        logger.info("SSO database connection successful")
        
        # Test Main database connection
        async with main_engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        logger.info("Main database connection successful")
        
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        raise


async def close_connections():
    """Close database connections"""
    global sso_engine, main_engine
    
    if sso_engine:
        await sso_engine.dispose()
        logger.info("SSO database engine disposed")
    
    if main_engine:
        await main_engine.dispose()
        logger.info("Main database engine disposed")


# Dependency to get SSO database session
async def get_sso_db():
    """Get SSO database session"""
    if not sso_session_factory:
        await create_engines()
    
    async with sso_session_factory() as session:
        try:
            yield session
        finally:
            await session.close()


# Dependency to get Main database session
async def get_main_db():
    """Get Main database session"""
    if not main_session_factory:
        await create_engines()
    
    async with main_session_factory() as session:
        try:
            yield session
        finally:
            await session.close()
