# Connecting MongoDB Compass to Local MongoDB

## Quick Connection

### Connection String (for MongoDB Compass)

**Simple connection (no authentication):**
```
mongodb://localhost:27017/
```

**With database name:**
```
mongodb://localhost:27017/devyani_mongo
```

## Step-by-Step Instructions

### Option 1: Using Connection String (Recommended)

1. In MongoDB Compass, click the **"+ Add new connection"** button (green button at the bottom)
2. You'll see a connection string input field
3. Paste this connection string:
   ```
   mongodb://localhost:27017/
   ```
4. Click **"Connect"**

### Option 2: Using Connection Form

1. Click **"+ Add new connection"**
2. If you see a form instead of connection string:
   - **Hostname**: `localhost`
   - **Port**: `27017`
   - **Authentication**: Leave as "None" (default for local MongoDB)
   - **Default Database**: `devyani_mongo` (optional)
3. Click **"Connect"**

## Connection Details

Based on your configuration:

- **Host**: `localhost`
- **Port**: `27017`
- **Database**: `devyani_mongo`
- **Authentication**: None (local MongoDB default)
- **Connection String**: `mongodb://localhost:27017/`

## After Connecting

Once connected, you should see:
- Your databases listed in the left sidebar
- The `devyani_mongo` database (if you've created collections)
- Other default databases: `admin`, `config`, `local`

## Troubleshooting

### If connection fails:

1. **Check if MongoDB is running:**
   ```bash
   ps aux | grep mongod
   # or
   lsof -i :27017
   ```

2. **Start MongoDB if not running:**
   ```bash
   brew services start mongodb-community
   ```

3. **Check MongoDB logs:**
   ```bash
   tail -f /opt/homebrew/var/log/mongodb/mongo.log
   ```

4. **Try connecting with IP address instead:**
   ```
   mongodb://127.0.0.1:27017/
   ```

## Testing the Connection

After connecting in Compass, you can:
- Browse databases and collections
- View documents
- Run queries
- Create indexes
- Import/export data

## Your Application Configuration

Your application is configured to use:
- **Database**: `devyani_mongo`
- **Connection**: `mongodb://localhost:27017/devyani_mongo`

You can verify this matches what you see in Compass!

