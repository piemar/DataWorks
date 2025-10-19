"""
Test script to verify database connections work with the compatibility layer
"""
import asyncio
import os
import logging
from dotenv import load_dotenv

from mongodb_compatibility import test_database_connections

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def main():
    """Test database connections"""
    logger.info("Testing database connections with compatibility layer...")
    
    # Load environment variables
    load_dotenv()
    
    # Check if .env file exists
    if not os.path.exists('.env'):
        logger.error("❌ .env file not found. Please copy config.env.example to .env and update with your connection strings.")
        return False
    
    # Test connections
    success = await test_database_connections()
    
    if success:
        logger.info("✅ All database connections successful!")
        logger.info("You can now run the data generator or migration scripts.")
    else:
        logger.error("❌ Database connection test failed.")
        logger.error("Please check your connection strings in the .env file.")
    
    return success

if __name__ == "__main__":
    asyncio.run(main())
