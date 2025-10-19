"""
MongoDB compatibility layer for older wire versions
Handles compatibility between newer drivers and older MongoDB versions
"""
import os
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, OperationFailure
import logging

logger = logging.getLogger(__name__)

class CompatibleMongoClient:
    """Compatible MongoDB client for older wire versions"""
    
    def __init__(self, connection_string: str, **kwargs):
        self.connection_string = connection_string
        self.kwargs = kwargs
        self._client = None
        
    def create_client(self, async_client: bool = False):
        """Create a compatible MongoDB client"""
        try:
            # Add ultra-optimized compatibility options for maximum performance
            compatibility_options = {
                'serverSelectionTimeoutMS': 10000,  # Faster server selection
                'connectTimeoutMS': 10000,        # Faster connection
                'socketTimeoutMS': 30000,         # Keep socket timeout reasonable
                'maxPoolSize': 100,               # Increased for high concurrency
                'minPoolSize': 20,                # Keep more connections warm
                'retryWrites': False,             # Disable retryWrites for older versions
                'w': 0,                          # Fire-and-forget writes for speed
                'j': False,                      # No journaling for speed
                'readPreference': 'primary',
                'maxIdleTimeMS': 300000,         # Keep connections alive longer
                'heartbeatFrequencyMS': 10000,   # Faster heartbeat
                'compressors': 'zstd',           # Compression for network efficiency
                'zlibCompressionLevel': 6,       # Balanced compression
            }
            
            # Merge with user-provided options
            compatibility_options.update(self.kwargs)
            
            if async_client:
                self._client = AsyncIOMotorClient(self.connection_string, **compatibility_options)
            else:
                self._client = MongoClient(self.connection_string, **compatibility_options)
                
            return self._client
            
        except Exception as e:
            logger.error(f"Failed to create compatible MongoDB client: {e}")
            raise

    async def test_connection_async(self):
        """Test async connection with compatibility handling"""
        try:
            if not self._client:
                self.create_client(async_client=True)
            
            # Use a simple ping command that works with older versions
            await self._client.admin.command('ping')
            logger.info("✅ Async MongoDB connection successful")
            return True
            
        except Exception as e:
            logger.error(f"❌ Async MongoDB connection failed: {e}")
            return False

    def test_connection_sync(self):
        """Test sync connection with compatibility handling"""
        try:
            if not self._client:
                self.create_client(async_client=False)
            
            # Use a simple ping command that works with older versions
            self._client.admin.command('ping')
            logger.info("✅ Sync MongoDB connection successful")
            return True
            
        except Exception as e:
            logger.error(f"❌ Sync MongoDB connection failed: {e}")
            return False

    def close(self):
        """Close the client connection"""
        if self._client:
            self._client.close()

def create_compatible_cosmos_client(connection_string: str, **kwargs):
    """Create a compatible Cosmos DB client"""
    return CompatibleMongoClient(connection_string, **kwargs)

def create_compatible_atlas_client(connection_string: str, **kwargs):
    """Create a compatible Atlas client"""
    return CompatibleMongoClient(connection_string, **kwargs)

async def test_database_connections():
    """Test both database connections with compatibility"""
    from dotenv import load_dotenv
    load_dotenv('.env_local')  # Try to load sensitive credentials first
    load_dotenv('config.env')  # Fallback to example values
    
    cosmos_conn_str = os.getenv('COSMOS_DB_CONNECTION_STRING')
    atlas_conn_str = os.getenv('MONGODB_ATLAS_CONNECTION_STRING')
    
    if not cosmos_conn_str or not atlas_conn_str:
        logger.error("Missing connection strings in environment variables")
        return False
    
    # Test Cosmos DB connection
    logger.info("Testing Cosmos DB connection...")
    cosmos_client = create_compatible_cosmos_client(cosmos_conn_str)
    cosmos_ok = await cosmos_client.test_connection_async()
    
    # Test Atlas connection
    logger.info("Testing Atlas connection...")
    atlas_client = create_compatible_atlas_client(atlas_conn_str)
    atlas_ok = await atlas_client.test_connection_async()
    
    # Cleanup
    cosmos_client.close()
    atlas_client.close()
    
    return cosmos_ok and atlas_ok

if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_database_connections())
