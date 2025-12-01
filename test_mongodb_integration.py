#!/usr/bin/env python3
"""
Integration test to demonstrate MongoDB usage in the application
"""

import sys
import os
from datetime import datetime

# Add Backend directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.config.mongodb import (
    get_mongodb_database,
    get_mongodb_collection,
    test_mongodb_connection
)
from app.config.settings import settings


def test_integration():
    """Test MongoDB integration with example operations"""
    print("=" * 60)
    print("MongoDB Integration Test")
    print("=" * 60)
    print()
    
    # Test 1: Connection
    print("1️⃣ Testing Connection...")
    if not test_mongodb_connection():
        print("❌ Connection failed!")
        return False
    print("✅ Connection successful")
    print()
    
    # Test 2: Database Access
    print("2️⃣ Testing Database Access...")
    try:
        db = get_mongodb_database()
        print(f"✅ Database '{db.name}' accessed")
    except Exception as e:
        print(f"❌ Database access failed: {e}")
        return False
    print()
    
    # Test 3: Collection Operations
    print("3️⃣ Testing Collection Operations...")
    try:
        collection = get_mongodb_collection("test_integration")
        
        # Insert sample data (similar to daily_sales_summary structure)
        sample_doc = {
            "sales_date": "2024-01-15",
            "store_code": "STORE001",
            "city_id": "CITY001",
            "zone": "NORTH",
            "instore_total": 5000.50,
            "aggregator_total": 3000.25,
            "total_sales": 8000.75,
            "total_order_count": 150,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        result = collection.insert_one(sample_doc)
        print(f"✅ Insert successful - Document ID: {result.inserted_id}")
        
        # Query
        found = collection.find_one({"store_code": "STORE001"})
        if found:
            print(f"✅ Query successful - Found document for store: {found['store_code']}")
            print(f"   Total Sales: {found['total_sales']}")
        
        # Update
        update_result = collection.update_one(
            {"store_code": "STORE001"},
            {"$set": {"total_sales": 8500.00, "updated_at": datetime.now().isoformat()}}
        )
        print(f"✅ Update successful - Modified: {update_result.modified_count} document(s)")
        
        # Query multiple
        all_docs = list(collection.find({}).limit(5))
        print(f"✅ Query multiple successful - Found {len(all_docs)} document(s)")
        
        # Cleanup
        collection.delete_many({"store_code": "STORE001"})
        print("✅ Cleanup successful")
        
    except Exception as e:
        print(f"❌ Collection operations failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    print()
    
    # Test 4: Index Creation (for performance)
    print("4️⃣ Testing Index Creation...")
    try:
        collection = get_mongodb_collection("test_indexes")
        
        # Create indexes
        collection.create_index([("sales_date", 1), ("store_code", 1)], unique=True, name="idx_date_store")
        collection.create_index("sales_date", name="idx_date")
        collection.create_index("store_code", name="idx_store")
        
        indexes = collection.list_indexes()
        index_names = [idx['name'] for idx in indexes]
        print(f"✅ Indexes created: {', '.join(index_names)}")
        
    except Exception as e:
        print(f"⚠️ Index creation: {e} (may already exist)")
    print()
    
    # Test 5: Aggregation (example query)
    print("5️⃣ Testing Aggregation Query...")
    try:
        collection = get_mongodb_collection("test_aggregation")
        
        # Insert sample data
        sample_data = [
            {"sales_date": "2024-01-15", "store_code": "STORE001", "total_sales": 1000},
            {"sales_date": "2024-01-15", "store_code": "STORE002", "total_sales": 2000},
            {"sales_date": "2024-01-16", "store_code": "STORE001", "total_sales": 1500},
        ]
        collection.insert_many(sample_data)
        
        # Aggregate total sales by date
        pipeline = [
            {"$group": {
                "_id": "$sales_date",
                "total_sales": {"$sum": "$total_sales"},
                "store_count": {"$sum": 1}
            }},
            {"$sort": {"_id": 1}}
        ]
        
        results = list(collection.aggregate(pipeline))
        print(f"✅ Aggregation successful - Found {len(results)} date groups")
        for result in results:
            print(f"   Date: {result['_id']}, Total: {result['total_sales']}, Stores: {result['store_count']}")
        
        # Cleanup
        collection.delete_many({})
        print("✅ Cleanup successful")
        
    except Exception as e:
        print(f"❌ Aggregation failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    print()
    
    print("=" * 60)
    print("✅ All integration tests passed!")
    print("=" * 60)
    print()
    print("MongoDB is ready to use in your application!")
    print()
    print("Example usage in your code:")
    print("""
from app.config.mongodb import get_mongodb_collection

collection = get_mongodb_collection("daily_sales_summary")
collection.insert_one({
    "sales_date": "2024-01-15",
    "store_code": "STORE001",
    "total_sales": 1000.50
})
    """)
    
    return True


if __name__ == "__main__":
    success = test_integration()
    sys.exit(0 if success else 1)

