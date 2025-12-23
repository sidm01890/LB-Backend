"""
Application settings and configuration
"""

from pydantic_settings import BaseSettings
from typing import Optional
import os
import logging
from urllib.parse import quote_plus


class Settings(BaseSettings):
    """Application settings"""
    
    # Application
    app_name: str = "Reconcii Admin API"
    debug: bool = True  # Default to True for development
    environment: str = "development"  # Default to development
    
    # Server
    host: str = "0.0.0.0"
    port: int = 8034
    
    # JWT Configuration
    jwt_secret: str = "your-default-jwt-secret-key-for-development"
    jwt_algorithm: str = "HS256"
    jwt_expires_in: str = "24h"
    
    # Encryption
    secret_key: str = "your-secret-key"
    iv: str = "your-iv-key"
    
    # Database - SSO (Authentication)
    sso_db_host: str = "localhost"
    sso_db_user: str = "root"
    sso_db_password: str = "NewStrongPassword123!"
    sso_db_name: str = "devyani_sso"
    sso_db_port: int = 3306
    
    # Database - Main (Application Data)
    main_db_host: str = "localhost"
    main_db_user: str = "root"
    main_db_password: str = "NewStrongPassword123!"
    main_db_name: str = "devyani"
    main_db_port: int = 3306
    
    # Production Database (AWS RDS)
    production_sso_db_host: str = "coreco-mysql.cpzxmgfkrh6g.ap-south-1.rds.amazonaws.com"
    production_sso_db_user: str = "admin"
    production_sso_db_password: str = "One4the$#"
    production_sso_db_name: str = "devyani_sso"
    
    production_main_db_host: str = "coreco-mysql.cpzxmgfkrh6g.ap-south-1.rds.amazonaws.com"
    production_main_db_user: str = "admin"
    production_main_db_password: str = "One4the$#"
    production_main_db_name: str = "devyani"
    
    # Database Pool Settings
    db_pool_size: int = 5
    db_max_overflow: int = 10
    db_pool_timeout: int = 30
    db_pool_recycle: int = 3600
    
    # Organization & Tool IDs
    organization_id: int = 1
    tool_id: int = 1
    
    # Email Configuration
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_username: Optional[str] = None
    smtp_password: Optional[str] = None
    
    # Redis Configuration (for background tasks)
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    
    # MongoDB Configuration (Local Development)
    mongo_host: str = "localhost"
    mongo_port: int = 27017
    mongo_database: str = "devyani_mongo"
    mongo_username: Optional[str] = None
    mongo_password: Optional[str] = None
    mongo_auth_source: str = "admin"  # Authentication database
    
    # Production MongoDB Configuration (if using MongoDB Atlas or remote)
    production_mongo_host: Optional[str] = None
    production_mongo_port: int = 27017
    production_mongo_database: Optional[str] = None
    production_mongo_username: Optional[str] = None
    production_mongo_password: Optional[str] = None
    production_mongo_auth_source: str = "admin"
    
    # MongoDB Connection Pool Settings
    mongo_max_pool_size: int = 50
    mongo_min_pool_size: int = 10
    mongo_max_idle_time_ms: int = 45000
    mongo_server_selection_timeout_ms: int = 5000
    
    # Task Executor Configuration (for parallel processing)
    task_executor_workers: int = 10  # Number of worker threads for background tasks
    
    # CORS Configuration
    cors_origins: str = "*"  # Comma-separated list of allowed origins, or "*" for all
    
    class Config:
        # pydantic_settings reads from environment variables first, then from env_file
        # For staging, environment variables should be set in Docker Compose
        # This allows .env for local dev, and env vars for Docker deployments
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        # Allow .env file to be optional - use defaults if not present
        env_file_required = False
        # Allow extra fields in .env that aren't defined in Settings (e.g., FORMULA_WATCH_INTERVAL_SECONDS)
        extra = "ignore"


# Create settings instance
# Note: pydantic_settings automatically reads from environment variables first,
# so Docker environment variables will override .env file values
settings = Settings()

# Log environment configuration for debugging
logger = logging.getLogger(__name__)
logger.info(f"🌍 Environment: {settings.environment}")
logger.info(f"🔧 MongoDB Config - Host: {settings.mongo_host}, Port: {settings.mongo_port}, DB: {settings.mongo_database}")
if settings.mongo_username:
    logger.info(f"👤 MongoDB Username: {settings.mongo_username}, Auth Source: {settings.mongo_auth_source}")

# Database URLs
def get_database_urls():
    """Get database URLs based on environment"""
    # Use AWS RDS for both production and staging environments
    if settings.environment == "production":
        # Production: Use production_* settings (hardcoded AWS RDS)
        sso_user = quote_plus(settings.production_sso_db_user)
        sso_pass = quote_plus(settings.production_sso_db_password)
        main_user = quote_plus(settings.production_main_db_user)
        main_pass = quote_plus(settings.production_main_db_password)
        sso_url = (
            f"mysql+aiomysql://{sso_user}:{sso_pass}"
            f"@{settings.production_sso_db_host}:{settings.sso_db_port}/{settings.production_sso_db_name}"
        )
        main_url = (
            f"mysql+aiomysql://{main_user}:{main_pass}"
            f"@{settings.production_main_db_host}:{settings.main_db_port}/{settings.production_main_db_name}"
        )
    elif settings.environment == "staging":
        # Staging: Use sso_db_* settings from .env.staging file (contains AWS RDS credentials)
        # The .env.staging file should have SSO_DB_HOST, SSO_DB_USER, etc. set to AWS RDS values
        sso_user = quote_plus(settings.sso_db_user)
        sso_pass = quote_plus(settings.sso_db_password)
        main_user = quote_plus(settings.main_db_user)
        main_pass = quote_plus(settings.main_db_password)
        sso_url = (
            f"mysql+aiomysql://{sso_user}:{sso_pass}"
            f"@{settings.sso_db_host}:{settings.sso_db_port}/{settings.sso_db_name}"
        )
        main_url = (
            f"mysql+aiomysql://{main_user}:{main_pass}"
            f"@{settings.main_db_host}:{settings.main_db_port}/{settings.main_db_name}"
        )
    else:
        # Development environment - use localhost defaults
        sso_user = quote_plus(settings.sso_db_user)
        sso_pass = quote_plus(settings.sso_db_password)
        main_user = quote_plus(settings.main_db_user)
        main_pass = quote_plus(settings.main_db_password)
        sso_url = (
            f"mysql+aiomysql://{sso_user}:{sso_pass}"
            f"@{settings.sso_db_host}:{settings.sso_db_port}/{settings.sso_db_name}"
        )
        main_url = (
            f"mysql+aiomysql://{main_user}:{main_pass}"
            f"@{settings.main_db_host}:{settings.main_db_port}/{settings.main_db_name}"
        )
    
    return sso_url, main_url


def get_mongodb_connection_string() -> str:
    """Get MongoDB connection string based on environment"""
    if settings.environment == "production" and settings.production_mongo_host:
        # Production: Use production MongoDB settings
        if settings.production_mongo_username and settings.production_mongo_password:
            username = quote_plus(settings.production_mongo_username)
            password = quote_plus(settings.production_mongo_password)
            connection_string = (
                f"mongodb://{username}:{password}"
                f"@{settings.production_mongo_host}:{settings.production_mongo_port}/"
                f"{settings.production_mongo_database}?authSource={settings.production_mongo_auth_source}"
            )
        else:
            connection_string = (
                f"mongodb://{settings.production_mongo_host}:{settings.production_mongo_port}/"
                f"{settings.production_mongo_database}"
            )
    else:
        # Development/Staging: Use local MongoDB
        if settings.mongo_username and settings.mongo_password:
            username = quote_plus(settings.mongo_username)
            password = quote_plus(settings.mongo_password)
            connection_string = (
                f"mongodb://{username}:{password}"
                f"@{settings.mongo_host}:{settings.mongo_port}/"
                f"{settings.mongo_database}?authSource={settings.mongo_auth_source}"
            )
        else:
            # Local MongoDB without authentication (default for development)
            connection_string = (
                f"mongodb://{settings.mongo_host}:{settings.mongo_port}/"
                f"{settings.mongo_database}"
            )
    
    return connection_string


def get_mongodb_database_name() -> str:
    """Get MongoDB database name based on environment"""
    if settings.environment == "production" and settings.production_mongo_database:
        return settings.production_mongo_database
    return settings.mongo_database


def validate_environment() -> None:
    """
    Validate required environment variables and configuration.
    This should be called during application startup.
    """
    logger = logging.getLogger(__name__)
    
    # Validate JWT configuration
    if not settings.jwt_algorithm:
        logger.error("JWT_ALGORITHM is not configured")
        raise ValueError("JWT_ALGORITHM is required")
    
    logger.info("✅ Environment variables validated successfully")
    logger.info(f"🌍 Environment: {settings.environment}")
    logger.info(f"🔧 Debug mode: {settings.debug}")
