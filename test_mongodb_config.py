#!/usr/bin/env python3
"""
Test script to verify MongoDB configuration and connection
Run this from the Backend directory: python test_mongodb_config.py
"""

import sys
import os
import asyncio

# Add Backend directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.config.mongodb import (
    create_mongodb_client,
    get_mongodb_database,
    get_mongodb_collection,
    test_mongodb_connection,
    close_mongodb_connection
)
from app.config.settings import (
    settings,
    get_mongodb_connection_string,
    get_mongodb_database_name
)


def test_configuration():
    """Test MongoDB configuration"""
    print("=" * 60)
    print("MongoDB Configuration Test")
    print("=" * 60)
    print(f"Environment: {settings.environment}")
    print(f"Host: {settings.mongo_host}")
    print(f"Port: {settings.mongo_port}")
    print(f"Database: {get_mongodb_database_name()}")
    print(f"Connection String: {get_mongodb_connection_string()}")
    print()


def test_connection():
    """Test MongoDB connection"""
    print("=" * 60)
    print("Testing MongoDB Connection")
    print("=" * 60)
    
    try:
        # Test connection
        result = test_mongodb_connection()
        if result:
            print("✅ Connection test successful!")
        else:
            print("❌ Connection test failed!")
            return False
        
        # Get database
        db = get_mongodb_database()
        print(f"✅ Database '{db.name}' accessed successfully")
        
        # List collections
        collections = db.list_collection_names()
        print(f"📁 Collections in database: {len(collections)}")
        if collections:
            for coll in collections[:10]:  # Show first 10
                count = db[coll].count_documents({})
                print(f"   - {coll}: {count} documents")
        else:
            print("   (No collections found - database is empty)")
        
        # Test write/read
        test_collection = get_mongodb_collection("test_connection")
        test_doc = {
            "test": True,
            "message": "Configuration test successful",
            "timestamp": "2024-01-01"
        }
        result = test_collection.insert_one(test_doc)
        print(f"✅ Test write successful! Document ID: {result.inserted_id}")
        
        # Read it back
        retrieved = test_collection.find_one({"_id": result.inserted_id})
        if retrieved:
            print("✅ Test read successful!")
        
        # Clean up
        test_collection.delete_one({"_id": result.inserted_id})
        print("✅ Cleanup successful!")
        
        print()
        print("=" * 60)
        print("All tests passed! MongoDB is configured correctly.")
        print("=" * 60)
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        close_mongodb_connection()


if __name__ == "__main__":
    test_configuration()
    success = test_connection()
    sys.exit(0 if success else 1)

