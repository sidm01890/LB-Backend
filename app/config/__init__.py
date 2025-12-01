# Configuration module
from app.config.settings import settings

# MongoDB imports - optional, only if pymongo is installed
try:
    from app.config.mongodb import (
        create_mongodb_client,
        get_mongodb_database,
        get_mongodb_collection,
        test_mongodb_connection,
        close_mongodb_connection
    )
    MONGODB_AVAILABLE = True
except ImportError:
    # MongoDB not available - define dummy functions
    MONGODB_AVAILABLE = False
    
    def create_mongodb_client():
        raise ImportError("pymongo is not installed. Install it with: pip install pymongo")
    
    def get_mongodb_database():
        raise ImportError("pymongo is not installed. Install it with: pip install pymongo")
    
    def get_mongodb_collection(collection_name: str):
        raise ImportError("pymongo is not installed. Install it with: pip install pymongo")
    
    def test_mongodb_connection():
        return False
    
    def close_mongodb_connection():
        pass

__all__ = [
    'settings',
    'MONGODB_AVAILABLE',
    'create_mongodb_client',
    'get_mongodb_database',
    'get_mongodb_collection',
    'test_mongodb_connection',
    'close_mongodb_connection'
]
