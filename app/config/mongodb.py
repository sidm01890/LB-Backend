"""
MongoDB configuration and connection management
"""

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from app.config.settings import (
    settings,
    get_mongodb_connection_string,
    get_mongodb_database_name
)
import logging
from typing import Optional, Dict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global MongoDB client (shared across all databases)
_mongo_client: Optional[MongoClient] = None
# Per-database cache (key: database_name, value: Database instance)
_mongo_databases: Dict[str, Database] = {}


def create_mongodb_client() -> MongoClient:
    """Create and return MongoDB client"""
    global _mongo_client
    
    if _mongo_client is not None:
        return _mongo_client
    
    try:
        connection_string = get_mongodb_connection_string()
        
        # Log connection details (without password)
        logger.info(f"🔌 Connecting to MongoDB: {settings.mongo_host}:{settings.mongo_port}")
        if settings.mongo_username:
            logger.info(f"👤 Username: {settings.mongo_username}")
            logger.info(f"🔐 Auth Source: {settings.mongo_auth_source}")
        else:
            logger.info("🔓 No authentication configured")
        
        # Log connection string (masked password)
        masked_conn_str = connection_string
        if settings.mongo_password:
            masked_conn_str = connection_string.replace(settings.mongo_password, "***")
        logger.debug(f"🔗 Connection string: {masked_conn_str}")
        
        # Create MongoDB client with connection pool settings
        # Note: Connection string doesn't include database name - we'll select database per request
        _mongo_client = MongoClient(
            connection_string,
            maxPoolSize=settings.mongo_max_pool_size,
            minPoolSize=settings.mongo_min_pool_size,
            maxIdleTimeMS=settings.mongo_max_idle_time_ms,
            serverSelectionTimeoutMS=settings.mongo_server_selection_timeout_ms
        )
        
        # Test connection
        _mongo_client.admin.command('ping')
        logger.info("✅ MongoDB connection successful")
        
        return _mongo_client
        
    except ConnectionFailure as e:
        logger.error(f"❌ MongoDB connection failed: {e}")
        logger.error(f"   Host: {settings.mongo_host}:{settings.mongo_port}")
        if settings.mongo_username:
            logger.error(f"   Username: {settings.mongo_username}")
        raise
    except ServerSelectionTimeoutError as e:
        logger.error(f"❌ MongoDB server selection timeout: {e}")
        logger.error(f"   Host: {settings.mongo_host}:{settings.mongo_port}")
        logger.error(f"   This usually means the MongoDB server is not reachable or not running")
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected MongoDB error: {e}")
        logger.error(f"   Error type: {type(e).__name__}")
        raise


def get_mongodb_database() -> Database:
    """Get MongoDB database instance based on user context (token-based)"""
    global _mongo_databases
    
    try:
        client = create_mongodb_client()
        database_name = get_mongodb_database_name()
        
        # Cache per database name (supports multiple databases per MongoDB instance)
        if database_name not in _mongo_databases:
            _mongo_databases[database_name] = client[database_name]
            logger.info(f"✅ MongoDB database '{database_name}' cached successfully")
        else:
            logger.debug(f"📊 Using cached MongoDB database '{database_name}'")
        
        return _mongo_databases[database_name]
        
    except Exception as e:
        logger.error(f"❌ Error accessing MongoDB database: {e}")
        raise


def get_mongodb_collection(collection_name: str):
    """Get a MongoDB collection by name"""
    try:
        db = get_mongodb_database()
        return db[collection_name]
    except Exception as e:
        logger.error(f"❌ Error accessing MongoDB collection '{collection_name}': {e}")
        raise


def test_mongodb_connection() -> bool:
    """Test MongoDB connection"""
    try:
        client = create_mongodb_client()
        client.admin.command('ping')
        logger.info("✅ MongoDB connection test successful")
        return True
    except ConnectionFailure as e:
        logger.error(f"❌ MongoDB connection test failed - Connection Failure: {e}")
        logger.error(f"   Check if MongoDB is running and accessible at {settings.mongo_host}:{settings.mongo_port}")
        return False
    except ServerSelectionTimeoutError as e:
        logger.error(f"❌ MongoDB connection test failed - Server Selection Timeout: {e}")
        logger.error(f"   MongoDB server at {settings.mongo_host}:{settings.mongo_port} is not reachable")
        logger.error(f"   Check network connectivity and firewall rules")
        return False
    except Exception as e:
        logger.error(f"❌ MongoDB connection test failed: {e}")
        logger.error(f"   Error type: {type(e).__name__}")
        logger.error(f"   Connection details: {settings.mongo_host}:{settings.mongo_port}, DB: {get_mongodb_database_name()}")
        return False


def close_mongodb_connection():
    """Close MongoDB connection"""
    global _mongo_client, _mongo_databases
    
    if _mongo_client:
        try:
            _mongo_client.close()
            logger.info("✅ MongoDB connection closed")
        except Exception as e:
            logger.error(f"❌ Error closing MongoDB connection: {e}")
        finally:
            _mongo_client = None
            _mongo_databases.clear()

