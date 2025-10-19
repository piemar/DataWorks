#!/usr/bin/env python3
"""
Script to extract indexes from Azure Cosmos DB and create them on MongoDB Atlas
Run this after migration is complete to recreate indexes on Atlas
"""

import asyncio
import logging
import os
from typing import List, Dict, Any
from dotenv import load_dotenv
from mongodb_compatibility import create_compatible_cosmos_client, create_compatible_atlas_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IndexManager:
    def __init__(self):
        load_dotenv('.env_local')  # Try to load sensitive credentials first
        load_dotenv('config.env')  # Fallback to example values
        
        # Connection strings
        self.cosmos_connection_string = os.getenv('COSMOS_DB_CONNECTION_STRING')
        self.atlas_connection_string = os.getenv('MONGODB_ATLAS_CONNECTION_STRING')
        
        # Database and collection names
        self.cosmos_db_name = os.getenv('COSMOS_DB_NAME', 'volvo-service-orders')
        self.cosmos_collection_name = os.getenv('COSMOS_DB_COLLECTION', 'serviceorders')
        self.atlas_db_name = os.getenv('MONGODB_ATLAS_DB_NAME', 'volvo-service-orders')
        self.atlas_collection_name = os.getenv('MONGODB_ATLAS_COLLECTION', 'serviceorders')
        
        # Clients
        self.cosmos_client = None
        self.atlas_client = None
        self.cosmos_collection = None
        self.atlas_collection = None
        
        # Extracted indexes
        self.extracted_indexes = []

    async def connect_to_databases(self):
        """Connect to both Cosmos DB and MongoDB Atlas"""
        try:
            # Connect to Cosmos DB
            logger.info("Connecting to Azure Cosmos DB...")
            cosmos_compatible = create_compatible_cosmos_client(self.cosmos_connection_string)
            self.cosmos_client = cosmos_compatible.create_client(async_client=True)
            await self.cosmos_client.admin.command('ping')
            
            cosmos_db = self.cosmos_client[self.cosmos_db_name]
            self.cosmos_collection = cosmos_db[self.cosmos_collection_name]
            logger.info("Connected to Cosmos DB successfully")
            
            # Connect to MongoDB Atlas
            logger.info("Connecting to MongoDB Atlas...")
            atlas_compatible = create_compatible_atlas_client(self.atlas_connection_string)
            self.atlas_client = atlas_compatible.create_client(async_client=True)
            await self.atlas_client.admin.command('ping')
            
            atlas_db = self.atlas_client[self.atlas_db_name]
            self.atlas_collection = atlas_db[self.atlas_collection_name]
            logger.info("Connected to MongoDB Atlas successfully")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to databases: {e}")
            return False

    async def extract_indexes_from_cosmos(self):
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

    async def create_indexes_on_atlas(self):
        """Create extracted indexes on MongoDB Atlas"""
        try:
            logger.info("Creating indexes on MongoDB Atlas...")
            
            created_count = 0
            failed_count = 0
            
            for index_info in self.extracted_indexes:
                try:
                    # Skip the default _id index
                    if index_info['name'] == '_id_':
                        logger.info(f"Skipping default _id index")
                        continue
                    
                    # Prepare index creation options
                    index_options = {}
                    
                    if index_info['unique']:
                        index_options['unique'] = True
                    if index_info['sparse']:
                        index_options['sparse'] = True
                    if index_info['background']:
                        index_options['background'] = True
                    if index_info['expireAfterSeconds']:
                        index_options['expireAfterSeconds'] = index_info['expireAfterSeconds']
                    if index_info['partialFilterExpression']:
                        index_options['partialFilterExpression'] = index_info['partialFilterExpression']
                    if index_info['collation']:
                        index_options['collation'] = index_info['collation']
                    if index_info['weights']:
                        index_options['weights'] = index_info['weights']
                    if index_info['default_language']:
                        index_options['default_language'] = index_info['default_language']
                    if index_info['language_override']:
                        index_options['language_override'] = index_info['language_override']
                    if index_info['textIndexVersion']:
                        index_options['textIndexVersion'] = index_info['textIndexVersion']
                    if index_info['2dsphereIndexVersion']:
                        index_options['2dsphereIndexVersion'] = index_info['2dsphereIndexVersion']
                    if index_info['bits']:
                        index_options['bits'] = index_info['bits']
                    if index_info['min']:
                        index_options['min'] = index_info['min']
                    if index_info['max']:
                        index_options['max'] = index_info['max']
                    if index_info['bucketSize']:
                        index_options['bucketSize'] = index_info['bucketSize']
                    if index_info['wildcardProjection']:
                        index_options['wildcardProjection'] = index_info['wildcardProjection']
                    
                    # Create the index
                    await self.atlas_collection.create_index(
                        list(index_info['key'].items()),
                        **index_options
                    )
                    
                    logger.info(f"✅ Created index: {index_info['name']} on {index_info['key']}")
                    created_count += 1
                    
                except Exception as e:
                    logger.warning(f"❌ Failed to create index {index_info['name']}: {e}")
                    failed_count += 1
            
            logger.info(f"Index creation completed: {created_count} created, {failed_count} failed")
            return True
            
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
            return False

    async def save_indexes_to_file(self, filename: str = "extracted_indexes.json"):
        """Save extracted indexes to a JSON file for reference"""
        try:
            import json
            
            with open(filename, 'w') as f:
                json.dump(self.extracted_indexes, f, indent=2, default=str)
            
            logger.info(f"Indexes saved to {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving indexes to file: {e}")
            return False

    async def cleanup(self):
        """Close database connections"""
        if self.cosmos_client:
            self.cosmos_client.close()
        if self.atlas_client:
            self.atlas_client.close()
        logger.info("Database connections closed")

async def main():
    """Main function"""
    logger.info("Starting index extraction and creation process...")
    
    index_manager = IndexManager()
    
    try:
        # Connect to databases
        if not await index_manager.connect_to_databases():
            logger.error("Failed to connect to databases")
            return
        
        # Extract indexes from Cosmos DB
        if not await index_manager.extract_indexes_from_cosmos():
            logger.error("Failed to extract indexes")
            return
        
        # Save indexes to file for reference
        await index_manager.save_indexes_to_file()
        
        # Create indexes on Atlas
        if not await index_manager.create_indexes_on_atlas():
            logger.error("Failed to create indexes on Atlas")
            return
        
        logger.info("✅ Index extraction and creation completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
    finally:
        await index_manager.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
