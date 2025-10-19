"""
Performance tuning utility for optimizing migration and data generation
"""
import asyncio
import time
import os
import json
from typing import Dict, List, Tuple
import logging
from datetime import datetime

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

# Load environment variables from .env_local (sensitive) or config.env (examples)
load_dotenv('.env_local')  # Try to load sensitive credentials first
load_dotenv('config.env')  # Fallback to example values

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PerformanceTuner:
    """Performance tuning utility for database operations"""
    
    def __init__(self):
        self.cosmos_connection_string = os.getenv('COSMOS_DB_CONNECTION_STRING')
        self.atlas_connection_string = os.getenv('MONGODB_ATLAS_CONNECTION_STRING')
        self.cosmos_db_name = os.getenv('COSMOS_DB_NAME', 'volvo-service-orders')
        self.atlas_db_name = os.getenv('MONGODB_ATLAS_DB_NAME', 'volvo-service-orders')
        self.cosmos_collection_name = os.getenv('COSMOS_DB_COLLECTION', 'serviceorders')
        self.atlas_collection_name = os.getenv('MONGODB_ATLAS_COLLECTION', 'serviceorders')
        
        self.cosmos_client = None
        self.atlas_client = None

    async def connect_to_databases(self):
        """Connect to both databases"""
        try:
            self.cosmos_client = AsyncIOMotorClient(self.cosmos_connection_string)
            await self.cosmos_client.admin.command('ping')
            
            self.atlas_client = AsyncIOMotorClient(self.atlas_connection_string)
            await self.atlas_client.admin.command('ping')
            
            logger.info("Connected to both databases for performance testing")
            return True
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            return False

    async def test_batch_sizes(self, test_documents: List[Dict]) -> Dict[int, float]:
        """Test different batch sizes to find optimal performance"""
        logger.info("Testing different batch sizes...")
        
        batch_sizes = [100, 500, 1000, 2000, 5000, 10000]
        results = {}
        
        cosmos_collection = self.cosmos_client[self.cosmos_db_name][self.cosmos_collection_name]
        
        for batch_size in batch_sizes:
            logger.info(f"Testing batch size: {batch_size}")
            
            # Prepare test batch
            test_batch = test_documents[:batch_size]
            
            # Test write performance
            start_time = time.time()
            try:
                await cosmos_collection.insert_many(test_batch, ordered=False)
                end_time = time.time()
                
                duration = end_time - start_time
                rate = batch_size / duration if duration > 0 else 0
                results[batch_size] = rate
                
                logger.info(f"Batch size {batch_size}: {rate:.0f} docs/sec")
                
                # Clean up test data
                await cosmos_collection.delete_many({"_id": {"$in": [doc["_id"] for doc in test_batch]}})
                
            except Exception as e:
                logger.error(f"Error testing batch size {batch_size}: {e}")
                results[batch_size] = 0
            
            # Small delay between tests
            await asyncio.sleep(1)
        
        return results

    async def test_concurrent_workers(self, test_documents: List[Dict], batch_size: int) -> Dict[int, float]:
        """Test different numbers of concurrent workers"""
        logger.info(f"Testing concurrent workers with batch size {batch_size}...")
        
        worker_counts = [1, 2, 4, 8, 16, 32]
        results = {}
        
        for worker_count in worker_counts:
            logger.info(f"Testing {worker_count} workers...")
            
            start_time = time.time()
            try:
                await self._test_concurrent_writes(test_documents, batch_size, worker_count)
                end_time = time.time()
                
                duration = end_time - start_time
                total_docs = len(test_documents)
                rate = total_docs / duration if duration > 0 else 0
                results[worker_count] = rate
                
                logger.info(f"{worker_count} workers: {rate:.0f} docs/sec")
                
            except Exception as e:
                logger.error(f"Error testing {worker_count} workers: {e}")
                results[worker_count] = 0
            
            await asyncio.sleep(2)
        
        return results

    async def _test_concurrent_writes(self, documents: List[Dict], batch_size: int, worker_count: int):
        """Test concurrent write operations"""
        cosmos_collection = self.cosmos_client[self.cosmos_db_name][self.cosmos_collection_name]
        
        # Split documents into batches
        batches = [documents[i:i + batch_size] for i in range(0, len(documents), batch_size)]
        
        # Create semaphore to limit concurrent operations
        semaphore = asyncio.Semaphore(worker_count)
        
        async def write_batch(batch):
            async with semaphore:
                try:
                    await cosmos_collection.insert_many(batch, ordered=False)
                    # Clean up
                    await cosmos_collection.delete_many({"_id": {"$in": [doc["_id"] for doc in batch]}})
                except Exception as e:
                    logger.error(f"Error in concurrent write: {e}")
        
        # Execute all batches concurrently
        await asyncio.gather(*[write_batch(batch) for batch in batches])

    async def test_connection_pool_sizes(self) -> Dict[int, float]:
        """Test different connection pool sizes"""
        logger.info("Testing connection pool sizes...")
        
        pool_sizes = [10, 20, 50, 100, 200]
        results = {}
        
        for pool_size in pool_sizes:
            logger.info(f"Testing pool size: {pool_size}")
            
            try:
                # Create client with specific pool size
                test_client = AsyncIOMotorClient(
                    self.cosmos_connection_string,
                    maxPoolSize=pool_size,
                    minPoolSize=pool_size // 2
                )
                
                # Test connection
                start_time = time.time()
                await test_client.admin.command('ping')
                end_time = time.time()
                
                connection_time = end_time - start_time
                results[pool_size] = connection_time
                
                logger.info(f"Pool size {pool_size}: {connection_time:.3f}s connection time")
                
                test_client.close()
                
            except Exception as e:
                logger.error(f"Error testing pool size {pool_size}: {e}")
                results[pool_size] = float('inf')
            
            await asyncio.sleep(1)
        
        return results

    async def benchmark_migration_performance(self) -> Dict[str, float]:
        """Benchmark migration performance between databases"""
        logger.info("Benchmarking migration performance...")
        
        cosmos_collection = self.cosmos_client[self.cosmos_db_name][self.cosmos_collection_name]
        atlas_collection = self.atlas_client[self.atlas_db_name][self.atlas_collection_name]
        
        # Get a sample of documents
        sample_size = 1000
        sample_docs = await cosmos_collection.find({}).limit(sample_size).to_list(length=sample_size)
        
        if not sample_docs:
            logger.error("No documents found for benchmarking")
            return {}
        
        # Test read performance from Cosmos DB
        start_time = time.time()
        docs = await cosmos_collection.find({}).limit(sample_size).to_list(length=sample_size)
        read_time = time.time() - start_time
        read_rate = len(docs) / read_time if read_time > 0 else 0
        
        # Test write performance to Atlas
        start_time = time.time()
        await atlas_collection.insert_many(sample_docs, ordered=False)
        write_time = time.time() - start_time
        write_rate = len(sample_docs) / write_time if write_time > 0 else 0
        
        # Clean up test data
        await atlas_collection.delete_many({"_id": {"$in": [doc["_id"] for doc in sample_docs]}})
        
        results = {
            'cosmos_read_rate': read_rate,
            'atlas_write_rate': write_rate,
            'combined_rate': min(read_rate, write_rate)  # Bottleneck rate
        }
        
        logger.info(f"Cosmos DB read rate: {read_rate:.0f} docs/sec")
        logger.info(f"Atlas write rate: {write_rate:.0f} docs/sec")
        logger.info(f"Combined migration rate: {results['combined_rate']:.0f} docs/sec")
        
        return results

    def generate_recommendations(self, batch_results: Dict, worker_results: Dict, 
                               pool_results: Dict, migration_results: Dict) -> Dict[str, any]:
        """Generate performance recommendations based on test results"""
        recommendations = {}
        
        # Find optimal batch size
        if batch_results:
            optimal_batch = max(batch_results.items(), key=lambda x: x[1])
            recommendations['optimal_batch_size'] = optimal_batch[0]
            recommendations['batch_performance'] = optimal_batch[1]
        
        # Find optimal worker count
        if worker_results:
            optimal_workers = max(worker_results.items(), key=lambda x: x[1])
            recommendations['optimal_workers'] = optimal_workers[0]
            recommendations['worker_performance'] = optimal_workers[1]
        
        # Find optimal pool size
        if pool_results:
            optimal_pool = min(pool_results.items(), key=lambda x: x[1])
            recommendations['optimal_pool_size'] = optimal_pool[0]
            recommendations['pool_performance'] = optimal_pool[1]
        
        # Migration performance
        if migration_results:
            recommendations['estimated_migration_rate'] = migration_results.get('combined_rate', 0)
            
            # Estimate time for 100GB (assuming ~2.5KB per document = 40M documents)
            docs_per_gb = 400000  # Approximate
            total_docs_100gb = docs_per_gb * 100
            estimated_hours = total_docs_100gb / (recommendations['estimated_migration_rate'] * 3600)
            recommendations['estimated_100gb_migration_hours'] = estimated_hours
        
        return recommendations

    async def run_full_performance_test(self):
        """Run complete performance test suite"""
        logger.info("Starting comprehensive performance test...")
        
        if not await self.connect_to_databases():
            return
        
        # Generate test documents
        from models import ServiceOrderGenerator
        generator = ServiceOrderGenerator()
        test_documents = [generator.generate_service_order().dict(by_alias=True) for _ in range(10000)]
        
        try:
            # Run all tests
            batch_results = await self.test_batch_sizes(test_documents[:5000])
            worker_results = await self.test_concurrent_workers(test_documents[:2000], 1000)
            pool_results = await self.test_connection_pool_sizes()
            migration_results = await self.benchmark_migration_performance()
            
            # Generate recommendations
            recommendations = self.generate_recommendations(
                batch_results, worker_results, pool_results, migration_results
            )
            
            # Save results
            results = {
                'timestamp': datetime.utcnow().isoformat(),
                'batch_size_results': batch_results,
                'worker_results': worker_results,
                'pool_results': pool_results,
                'migration_results': migration_results,
                'recommendations': recommendations
            }
            
            with open('performance_results.json', 'w') as f:
                json.dump(results, f, indent=2)
            
            # Print recommendations
            logger.info("\n" + "="*60)
            logger.info("PERFORMANCE RECOMMENDATIONS")
            logger.info("="*60)
            
            for key, value in recommendations.items():
                if isinstance(value, float):
                    logger.info(f"{key}: {value:.2f}")
                else:
                    logger.info(f"{key}: {value}")
            
            logger.info(f"\nResults saved to: performance_results.json")
            
        except Exception as e:
            logger.error(f"Error in performance test: {e}")
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Clean up resources"""
        if self.cosmos_client:
            self.cosmos_client.close()
        if self.atlas_client:
            self.atlas_client.close()

async def main():
    """Main function"""
    tuner = PerformanceTuner()
    await tuner.run_full_performance_test()

if __name__ == "__main__":
    asyncio.run(main())
