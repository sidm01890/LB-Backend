# MongoDB Configuration Guide

## ✅ Configuration Complete

MongoDB has been configured in your Backend application with local development settings.

## Configuration Files

### 1. Settings (`app/config/settings.py`)
- Added MongoDB configuration fields
- Environment-aware connection string generation
- Development mode defaults to local MongoDB (localhost:27017)

### 2. MongoDB Module (`app/config/mongodb.py`)
- Connection management with connection pooling
- Database and collection access functions
- Connection testing utilities

### 3. Environment Variables (`env.example`)
- MongoDB configuration variables added
- Local development defaults included

## Usage

### Basic Usage

```python
from app.config.mongodb import (
    get_mongodb_database,
    get_mongodb_collection,
    test_mongodb_connection
)

# Test connection
if test_mongodb_connection():
    print("Connected!")

# Get database
db = get_mongodb_database()

# Get a collection
collection = get_mongodb_collection("daily_sales_summary")

# Insert a document
collection.insert_one({
    "sales_date": "2024-01-01",
    "store_code": "STORE001",
    "total_sales": 1000.50
})

# Query documents
for doc in collection.find({"store_code": "STORE001"}):
    print(doc)
```

### In API Routes

```python
from fastapi import APIRouter, Depends
from app.config.mongodb import get_mongodb_collection

router = APIRouter()

@router.get("/api/sales")
async def get_sales():
    collection = get_mongodb_collection("daily_sales_summary")
    results = list(collection.find({}).limit(100))
    return {"data": results}
```

## Configuration Details

### Development Mode (Default)
- **Host**: `localhost`
- **Port**: `27017`
- **Database**: `devyani_mongo`
- **Authentication**: None (local MongoDB default)
- **Connection String**: `mongodb://localhost:27017/devyani_mongo`

### Production Mode
- Uses `PRODUCTION_MONGO_*` environment variables if set
- Falls back to development settings if not configured
- Supports authentication if credentials are provided

## Environment Variables

Add these to your `.env` file:

```bash
# MongoDB Configuration (Local Development)
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DATABASE=devyani_mongo
MONGO_USERNAME=
MONGO_PASSWORD=
MONGO_AUTH_SOURCE=admin

# MongoDB Connection Pool Settings
MONGO_MAX_POOL_SIZE=50
MONGO_MIN_POOL_SIZE=10
MONGO_MAX_IDLE_TIME_MS=45000
MONGO_SERVER_SELECTION_TIMEOUT_MS=5000
```

## Testing

Run the test script to verify configuration:

```bash
cd Backend
python test_mongodb_config.py
```

## Connection Pool Settings

- **maxPoolSize**: 50 (maximum connections)
- **minPoolSize**: 10 (minimum connections)
- **maxIdleTimeMS**: 45000 (45 seconds)
- **serverSelectionTimeoutMS**: 5000 (5 seconds)

## Next Steps

1. ✅ MongoDB is configured and tested
2. ⬜ Create collections as needed for your use case
3. ⬜ Implement MongoDB operations in your services
4. ⬜ Add MongoDB indexes for performance (if needed)

## Example: Creating a Collection for Daily Sales Summary

```python
from app.config.mongodb import get_mongodb_collection

# Get or create collection
collection = get_mongodb_collection("daily_sales_summary")

# Create index for faster queries
collection.create_index([("sales_date", 1), ("store_code", 1)], unique=True)
collection.create_index("sales_date")
collection.create_index("store_code")
```

## Notes

- MongoDB collections are created automatically when you first insert data
- No need to manually create collections
- Indexes can be added for better query performance
- Connection is managed globally and reused across requests

