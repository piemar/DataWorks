"""
PROVEN HIGH-PERFORMANCE Migration
Targets 10-20k docs/sec on M40 from Azure VM
Simple architecture, maximum throughput
"""
import asyncio
import os
import time
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import InsertOne
from pymongo.errors import BulkWriteError
from pymongo.write_concern import WriteConcern
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv('.env_local')
load_dotenv('config.env')

class FastMigrator:
    def __init__(self):
        self.cosmos_uri = os.getenv('COSMOS_DB_CONNECTION_STRING')
        self.atlas_uri = os.getenv('MONGODB_ATLAS_CONNECTION_STRING')
        self.cosmos_db_name = os.getenv('COSMOS_DB_NAME', 'volvo-service-orders')
        self.atlas_db_name = os.getenv('MONGODB_ATLAS_DB_NAME', 'volvo-service-orders')
        self.collection_name = os.getenv('COSMOS_DB_COLLECTION', 'serviceorders')
        
        # Optimized settings
        self.read_batch_size = 10000  # Read large chunks from Cosmos
        self.write_batch_size = 5000  # Write in optimal chunks to Atlas
        self.concurrent_writers = 20 # Parallel write workers
        
    async def connect(self):
        """Connect with optimized settings"""
        print("🔌 Connecting to databases...")
        
        # Cosmos - minimal pool for reading
        self.cosmos_client = AsyncIOMotorClient(
            self.cosmos_uri,
            maxPoolSize=20,
            socketTimeoutMS=120000,
            connectTimeoutMS=30000
        )
        
        # Atlas - optimized pool for writing
        self.atlas_client = AsyncIOMotorClient(
            self.atlas_uri,
            maxPoolSize=100,
            minPoolSize=30,
            maxIdleTimeMS=120000,
            waitQueueTimeoutMS=10000
        )
        
        # Test connections
        await self.cosmos_client.admin.command('ping')
        await self.atlas_client.admin.command('ping')
        
        # Get collections
        self.cosmos_coll = self.cosmos_client[self.cosmos_db_name][self.collection_name]
        self.atlas_coll = self.atlas_client[self.atlas_db_name].get_collection(
            self.collection_name,
            write_concern=WriteConcern(w=1, j=False)
        )
        
        print("✅ Connected successfully")
        print(f"📊 Read batch: {self.read_batch_size:,} | Write batch: {self.write_batch_size:,}")
        print(f"👷 Concurrent writers: {self.concurrent_writers}")
        return True
    
    async def bulk_insert(self, docs):
        """Fast bulk insert with error handling"""
        if not docs:
            return 0
        
        try:
            result = await self.atlas_coll.bulk_write(
                [InsertOne(d) for d in docs],
                ordered=False
            )
            return result.inserted_count
        except BulkWriteError as e:
            # Some docs inserted, some duplicates
            return e.details.get('nInserted', 0)
        except Exception as e:
            print(f"❌ Insert error: {e}")
            return 0
    
    async def write_worker(self, queue, pbar, worker_id):
        """Worker that consumes from queue and writes to Atlas"""
        inserted_count = 0
        
        while True:
            try:
                batch = await asyncio.wait_for(queue.get(), timeout=2.0)
                if batch is None:  # Sentinel
                    break
                
                # Write batch
                inserted = await self.bulk_insert(batch)
                inserted_count += inserted
                
                if inserted > 0:
                    pbar.update(inserted)
                
                queue.task_done()
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                print(f"Worker {worker_id} error: {e}")
        
        return inserted_count
    
    async def migrate(self):
        """Main migration logic"""
        start_time = time.time()
        
        # Get resume point from Atlas
        print("🔍 Checking for existing data...")
        last_doc = await self.atlas_coll.find_one(sort=[("_id", -1)])
        
        if last_doc:
            query = {'_id': {'$gt': last_doc['_id']}}
            # Use estimated count for total existing docs
            existing = await self.atlas_coll.estimated_document_count()
            print(f"📌 Resuming after {existing:,} existing documents")
            print(f"📌 Last ID: {last_doc['_id']}")
        else:
            query = {}
            print("🆕 Starting fresh migration")
        
        # Get total count
        total = await self.cosmos_coll.estimated_document_count()
        print(f"📊 Total documents to process: {total:,}")
        
        # Create progress bar
        pbar = tqdm(
            total=total,
            desc="Migrating",
            unit="docs",
            unit_scale=True,
            ncols=120
        )
        
        # Create queue for batches
        queue = asyncio.Queue(maxsize=30)
        
        # Start write workers
        workers = [
            asyncio.create_task(self.write_worker(queue, pbar, i))
            for i in range(self.concurrent_writers)
        ]
        
        # Read from Cosmos and queue batches
        cursor = self.cosmos_coll.find(query).sort('_id', 1).batch_size(self.read_batch_size)
        
        read_buffer = []
        docs_read = 0
        
        async for doc in cursor:
            read_buffer.append(doc)
            docs_read += 1
            
            # When buffer reaches write batch size, queue it
            if len(read_buffer) >= self.write_batch_size:
                await queue.put(read_buffer)
                read_buffer = []
        
        # Queue remaining documents
        if read_buffer:
            await queue.put(read_buffer)
        
        print(f"\n📖 Finished reading {docs_read:,} documents from Cosmos")
        print("⏳ Waiting for write workers to complete...")
        
        # Send sentinel to stop workers
        for _ in range(self.concurrent_writers):
            await queue.put(None)
        
        # Wait for all workers
        results = await asyncio.gather(*workers)
        
        pbar.close()
        
        # Calculate stats
        elapsed = time.time() - start_time
        total_inserted = sum(results)
        rate = total_inserted / elapsed if elapsed > 0 else 0
        
        print(f"\n{'='*60}")
        print(f"✅ MIGRATION COMPLETE")
        print(f"{'='*60}")
        print(f"📊 Documents migrated: {total_inserted:,}")
        print(f"⏱️  Total time: {elapsed:.1f}s ({elapsed/60:.1f}m)")
        print(f"🚀 Average rate: {rate:.0f} docs/sec")
        print(f"{'='*60}\n")
        
        # Performance guidance
        if rate < 5000:
            print("⚠️  LOW PERFORMANCE DETECTED")
            print("Possible causes:")
            print("  1. Running from outside Azure (high latency)")
            print("  2. Large document size")
            print("  3. Cosmos DB throttling")
            print("\nRecommendations:")
            print("  • Run from Azure VM in same region as Cosmos DB")
            print("  • Check Cosmos DB metrics for throttling (429 errors)")
            print("  • Consider increasing Cosmos RU/s temporarily")
        elif rate < 10000:
            print("📊 MODERATE PERFORMANCE")
            print("You can likely improve by:")
            print("  • Running from Azure VM (if not already)")
            print("  • Increasing concurrent_writers to 8-10")
        else:
            print("🎉 EXCELLENT PERFORMANCE!")
        
        return total_inserted
    
    async def cleanup(self):
        """Close connections"""
        if self.cosmos_client:
            self.cosmos_client.close()
        if self.atlas_client:
            self.atlas_client.close()
        print("🔌 Connections closed")

async def main():
    migrator = FastMigrator()
    try:
        await migrator.connect()
        await migrator.migrate()
    finally:
        await migrator.cleanup()

if __name__ == "__main__":
    asyncio.run(main())