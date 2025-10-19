"""
Migration monitoring script to track progress and performance
"""
import asyncio
import os
import time
import json
from datetime import datetime, timedelta
from typing import Dict, Any
import logging

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MigrationMonitor:
    """Monitor migration progress and performance"""
    
    def __init__(self):
        self.cosmos_connection_string = os.getenv('COSMOS_DB_CONNECTION_STRING')
        self.atlas_connection_string = os.getenv('MONGODB_ATLAS_CONNECTION_STRING')
        self.cosmos_db_name = os.getenv('COSMOS_DB_NAME', 'volvo-service-orders')
        self.atlas_db_name = os.getenv('MONGODB_ATLAS_DB_NAME', 'volvo-service-orders')
        self.cosmos_collection_name = os.getenv('COSMOS_DB_COLLECTION', 'serviceorders')
        self.atlas_collection_name = os.getenv('MONGODB_ATLAS_COLLECTION', 'serviceorders')
        
        self.cosmos_client = None
        self.atlas_client = None
        self.checkpoint_file = 'migration_checkpoint.json'
        
        self.start_time = None
        self.last_source_count = 0
        self.last_target_count = 0

    async def connect_to_databases(self):
        """Connect to both databases"""
        try:
            # Connect to Cosmos DB
            self.cosmos_client = AsyncIOMotorClient(self.cosmos_connection_string)
            await self.cosmos_client.admin.command('ping')
            
            # Connect to Atlas
            self.atlas_client = AsyncIOMotorClient(self.atlas_connection_string)
            await self.atlas_client.admin.command('ping')
            
            logger.info("Connected to both databases")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to databases: {e}")
            return False

    async def get_document_counts(self) -> Dict[str, int]:
        """Get current document counts from both databases"""
        try:
            cosmos_db = self.cosmos_client[self.cosmos_db_name]
            cosmos_collection = cosmos_db[self.cosmos_collection_name]
            source_count = await cosmos_collection.count_documents({})
            
            atlas_db = self.atlas_client[self.atlas_db_name]
            atlas_collection = atlas_db[self.atlas_collection_name]
            target_count = await atlas_collection.count_documents({})
            
            return {
                'source': source_count,
                'target': target_count
            }
            
        except Exception as e:
            logger.error(f"Error getting document counts: {e}")
            return {'source': 0, 'target': 0}

    def load_checkpoint(self) -> Dict[str, Any]:
        """Load migration checkpoint"""
        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Error loading checkpoint: {e}")
        return {}

    def calculate_progress(self, source_count: int, target_count: int) -> Dict[str, Any]:
        """Calculate migration progress"""
        if source_count == 0:
            return {'percentage': 0, 'remaining': 0, 'rate': 0}
        
        percentage = (target_count / source_count) * 100
        remaining = source_count - target_count
        
        # Calculate rate if we have previous data
        rate = 0
        if self.start_time and self.last_target_count > 0:
            elapsed = time.time() - self.start_time
            if elapsed > 0:
                rate = (target_count - self.last_target_count) / elapsed
        
        return {
            'percentage': percentage,
            'remaining': remaining,
            'rate': rate
        }

    def estimate_completion_time(self, remaining: int, rate: float) -> str:
        """Estimate time to completion"""
        if rate <= 0:
            return "Unknown"
        
        seconds_remaining = remaining / rate
        eta = datetime.now() + timedelta(seconds=seconds_remaining)
        return eta.strftime("%Y-%m-%d %H:%M:%S")

    async def monitor_migration(self, interval: int = 30):
        """Monitor migration progress"""
        logger.info(f"Starting migration monitoring (checking every {interval} seconds)")
        
        if not await self.connect_to_databases():
            return
        
        self.start_time = time.time()
        checkpoint = self.load_checkpoint()
        
        logger.info("Migration Monitor Started")
        logger.info("=" * 60)
        
        try:
            while True:
                # Get current counts
                counts = await self.get_document_counts()
                source_count = counts['source']
                target_count = counts['target']
                
                # Calculate progress
                progress = self.calculate_progress(source_count, target_count)
                
                # Print status
                print(f"\n📊 Migration Status - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"Source (Cosmos DB): {source_count:,} documents")
                print(f"Target (Atlas):     {target_count:,} documents")
                print(f"Progress:           {progress['percentage']:.2f}%")
                print(f"Remaining:          {progress['remaining']:,} documents")
                print(f"Rate:               {progress['rate']:.0f} docs/sec")
                
                if progress['rate'] > 0:
                    eta = self.estimate_completion_time(progress['remaining'], progress['rate'])
                    print(f"ETA:                {eta}")
                
                # Check if migration is complete
                if target_count >= source_count:
                    print("\n🎉 Migration appears to be complete!")
                    break
                
                # Update previous counts for rate calculation
                self.last_source_count = source_count
                self.last_target_count = target_count
                
                # Wait for next check
                await asyncio.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        except Exception as e:
            logger.error(f"Error in monitoring: {e}")

    async def cleanup(self):
        """Clean up resources"""
        if self.cosmos_client:
            self.cosmos_client.close()
        if self.atlas_client:
            self.atlas_client.close()

async def main():
    """Main function"""
    monitor = MigrationMonitor()
    
    try:
        await monitor.monitor_migration(interval=30)
    except Exception as e:
        logger.error(f"Monitoring failed: {e}")
    finally:
        await monitor.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
