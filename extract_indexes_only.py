#!/usr/bin/env python3
"""
Script to extract indexes from Azure Cosmos DB and save them to a file
Use this to review indexes before creating them on MongoDB Atlas
"""

import asyncio
import json
import logging
import os
from typing import List, Dict, Any
from dotenv import load_dotenv
from mongodb_compatibility import create_compatible_cosmos_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IndexExtractor:
    def __init__(self):
        load_dotenv('.env_local')  # Try to load sensitive credentials first
        load_dotenv('config.env')  # Fallback to example values
        
        # Connection string
        self.cosmos_connection_string = os.getenv('COSMOS_DB_CONNECTION_STRING')
        
        # Database and collection names
        self.cosmos_db_name = os.getenv('COSMOS_DB_NAME', 'volvo-service-orders')
        self.cosmos_collection_name = os.getenv('COSMOS_DB_COLLECTION', 'serviceorders')
        
        # Client
        self.cosmos_client = None
        self.cosmos_collection = None
        
        # Extracted indexes
        self.extracted_indexes = []

    async def connect_to_cosmos(self):
        """Connect to Azure Cosmos DB"""
        try:
            logger.info("Connecting to Azure Cosmos DB...")
            cosmos_compatible = create_compatible_cosmos_client(self.cosmos_connection_string)
            self.cosmos_client = cosmos_compatible.create_client(async_client=True)
            await self.cosmos_client.admin.command('ping')
            
            cosmos_db = self.cosmos_client[self.cosmos_db_name]
            self.cosmos_collection = cosmos_db[self.cosmos_collection_name]
            logger.info("Connected to Cosmos DB successfully")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Cosmos DB: {e}")
            return False

    async def extract_indexes(self):
        """Extract all indexes from Cosmos DB collection"""
        try:
            logger.info("Extracting indexes from Cosmos DB...")
            
            # Get list of indexes
            indexes = await self.cosmos_collection.list_indexes().to_list(length=None)
            
            logger.info(f"Found {len(indexes)} indexes in Cosmos DB:")
            
            for index in indexes:
                index_info = {
                    'name': index.get('name', 'unknown'),
                    'key': index.get('key', {}),
                    'unique': index.get('unique', False),
                    'sparse': index.get('sparse', False),
                    'background': index.get('background', False),
                    'expireAfterSeconds': index.get('expireAfterSeconds'),
                    'partialFilterExpression': index.get('partialFilterExpression'),
                    'collation': index.get('collation'),
                    'weights': index.get('weights'),
                    'default_language': index.get('default_language'),
                    'language_override': index.get('language_override'),
                    'textIndexVersion': index.get('textIndexVersion'),
                    '2dsphereIndexVersion': index.get('2dsphereIndexVersion'),
                    'bits': index.get('bits'),
                    'min': index.get('min'),
                    'max': index.get('max'),
                    'bucketSize': index.get('bucketSize'),
                    'wildcardProjection': index.get('wildcardProjection')
                }
                
                self.extracted_indexes.append(index_info)
                
                # Log index details
                logger.info(f"  - {index_info['name']}: {index_info['key']}")
                if index_info['unique']:
                    logger.info(f"    (UNIQUE)")
                if index_info['sparse']:
                    logger.info(f"    (SPARSE)")
                if index_info['expireAfterSeconds']:
                    logger.info(f"    (TTL: {index_info['expireAfterSeconds']}s)")
                    
            logger.info(f"Successfully extracted {len(self.extracted_indexes)} indexes")
            return True
            
        except Exception as e:
            logger.error(f"Error extracting indexes: {e}")
            return False

    async def save_to_file(self, filename: str = "cosmos_indexes.json"):
        """Save extracted indexes to a JSON file"""
        try:
            with open(filename, 'w') as f:
                json.dump(self.extracted_indexes, f, indent=2, default=str)
            
            logger.info(f"Indexes saved to {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving indexes to file: {e}")
            return False

    async def cleanup(self):
        """Close database connection"""
        if self.cosmos_client:
            self.cosmos_client.close()
        logger.info("Database connection closed")

async def main():
    """Main function"""
    logger.info("Starting index extraction from Cosmos DB...")
    
    extractor = IndexExtractor()
    
    try:
        # Connect to Cosmos DB
        if not await extractor.connect_to_cosmos():
            logger.error("Failed to connect to Cosmos DB")
            return
        
        # Extract indexes
        if not await extractor.extract_indexes():
            logger.error("Failed to extract indexes")
            return
        
        # Save to file
        if not await extractor.save_to_file():
            logger.error("Failed to save indexes to file")
            return
        
        logger.info("✅ Index extraction completed successfully!")
        logger.info("📁 Check 'cosmos_indexes.json' for the extracted indexes")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
    finally:
        await extractor.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
