# 🔧 MongoDB Compass Connection Guide for Azure Cosmos DB

## Problem
MongoDB Compass shows the error:
```
Server at poc-new-vida.mongo.cosmos.azure.com:10255 reports maximum wire version 6, but this version of the Node.js Driver requires at least 7 (MongoDB 4.0)
```

This happens because:
- Your Azure Cosmos DB runs MongoDB 3.6 (wire version 6)
- MongoDB Compass by default requires MongoDB 4.0+ (wire version 7)

## Solution

### Option 1: Use Older Version of MongoDB Compass (Recommended)

**Download MongoDB Compass 1.28.4 or earlier:**
- These versions support MongoDB 3.6
- Download from: https://www.mongodb.com/try/download/compass
- Look for "Previous Releases" section

### Option 2: Connection String with Compatibility Options

If you must use a newer Compass version, try these connection string modifications:

#### Standard Connection String:
```
mongodb://poc-new-vida:GNYYn0FNXwOgOxKQ8vWaQ5cgVUJVKN5up4aFzmYxRcXkTJaub57UeZxJwgwneeLiWyaZvs8IHeYjACDb1kDqwQ==@poc-new-vida.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@poc-new-vida@
```

#### With Compatibility Options:
```
mongodb://poc-new-vida:GNYYn0FNXwOgOxKQ8vWaQ5cgVUJVKN5up4aFzmYxRcXkTJaub57UeZxJwgwneeLiWyaZvs8IHeYjACDb1kDqwQ==@poc-new-vida.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@poc-new-vida@&serverSelectionTimeoutMS=30000&connectTimeoutMS=30000&socketTimeoutMS=30000
```

### Option 3: Use Alternative MongoDB GUI Tools

#### Studio 3T (Free Community Edition)
- Download: https://studio3t.com/download/
- Better compatibility with older MongoDB versions
- More features than Compass

#### MongoDB for VS Code Extension
- Install MongoDB extension in VS Code
- Often has better compatibility
- Good for development work

#### NoSQLBooster
- Download: https://nosqlbooster.com/
- Excellent compatibility with older MongoDB versions
- Free version available

## Step-by-Step Compass Connection (Older Version)

1. **Download Compass 1.28.4**:
   - Go to https://www.mongodb.com/try/download/compass
   - Click "Previous Releases"
   - Download version 1.28.4 or earlier

2. **Install and Launch Compass**

3. **Connection Settings**:
   - **Connection String**: 
     ```
     mongodb://poc-new-vida:GNYYn0FNXwOgOxKQ8vWaQ5cgVUJVKN5up4aFzmYxRcXkTJaub57UeZxJwgwneeLiWyaZvs8IHeYjACDb1kDqwQ==@poc-new-vida.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@poc-new-vida@
     ```

4. **Advanced Options** (if needed):
   - **Server Selection Timeout**: 30000
   - **Connect Timeout**: 30000
   - **Socket Timeout**: 30000
   - **Max Pool Size**: 50

## Alternative: Use Our Python Scripts Instead

Since you have the Python migration tools working, you can use them for database operations:

### View Data:
```bash
source venv/bin/activate
python -c "
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import os

load_dotenv()

async def view_data():
    client = AsyncIOMotorClient(os.getenv('COSMOS_DB_CONNECTION_STRING'))
    db = client['volvo-service-orders']
    collection = db['serviceorders']
    
    # Count documents
    count = await collection.count_documents({})
    print(f'Total documents: {count:,}')
    
    # Show sample documents
    async for doc in collection.find({}).limit(5):
        print(f'Sample document: {doc.get(\"order_id\", \"N/A\")}')
    
    client.close()

asyncio.run(view_data())
"
```

### Query Data:
```bash
source venv/bin/activate
python -c "
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import os

load_dotenv()

async def query_data():
    client = AsyncIOMotorClient(os.getenv('COSMOS_DB_CONNECTION_STRING'))
    db = client['volvo-service-orders']
    collection = db['serviceorders']
    
    # Query example
    query = {'status': 'completed'}
    count = await collection.count_documents(query)
    print(f'Completed orders: {count:,}')
    
    client.close()

asyncio.run(query_data())
"
```

## Recommended Approach

1. **For Development**: Use Studio 3T or NoSQLBooster (better compatibility)
2. **For Migration**: Use our Python scripts (already working)
3. **For Quick Queries**: Use the Python query scripts above
4. **For Compass**: Use version 1.28.4 or earlier

## Why This Happens

- **Azure Cosmos DB**: Runs MongoDB 3.6 for compatibility
- **Modern Tools**: Expect MongoDB 4.0+ for new features
- **Solution**: Use compatible tool versions or alternative tools

The Python migration tools we created are actually more powerful than Compass for your use case, as they're specifically designed for large-scale data operations!
