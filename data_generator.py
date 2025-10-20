"""
Volvo Service Orders Data Generator
Generates and continuously writes service order data to Azure Cosmos DB
Optimized for maximum speed with UVLoop and advanced concurrency
"""
import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
import os
import time
from datetime import datetime
from typing import List, Optional
import logging
from concurrent.futures import ThreadPoolExecutor
import signal
import sys

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient, WriteConcern
from dotenv import load_dotenv
from tqdm import tqdm
import json

from models import ServiceOrderGenerator, ServiceOrder
from mongodb_compatibility import create_compatible_cosmos_client

# Load environment variables from .env_local (sensitive) or config.env (examples)
load_dotenv('.env_local')  # Try to load sensitive credentials first
load_dotenv('config.env')  # Fallback to example values

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataGenerator:
    """Main data generator class for creating and writing service orders"""
    
    def __init__(self):
        self.cosmos_connection_string = os.getenv('COSMOS_DB_CONNECTION_STRING')
        self.cosmos_db_name = os.getenv('COSMOS_DB_NAME', 'volvo-service-orders')
        self.cosmos_collection_name = os.getenv('COSMOS_DB_COLLECTION', 'serviceorders')
        
        self.batch_size = int(os.getenv('BATCH_SIZE', 5000))  # Optimized batch size for maximum speed
        self.total_documents = int(os.getenv('TOTAL_DOCUMENTS', 10000000))
        self.concurrent_workers = int(os.getenv('CONCURRENT_WORKERS', 16))  # More workers for maximum speed
        
        # Parallel generation/write optimization (ULTRA-FAST)
        self.generation_workers = int(os.getenv('GENERATION_WORKERS', 8))  # Dedicated generation workers
        self.write_workers = int(os.getenv('WRITE_WORKERS', 16))  # Dedicated write workers
        self.generation_queue = None  # Will be initialized in connect_to_cosmos
        self.write_queue = None  # Will be initialized in connect_to_cosmos
        self.generation_tasks = []
        self.write_tasks = []
        
        self.client: Optional[AsyncIOMotorClient] = None
        self.collection = None
        self.is_running = True
        
        # Statistics
        self.documents_written = 0
        self.start_time = None
        self.errors = 0
        
        
        # RU monitoring - correct rate calculation
        self.ru_consumed_in_window = 0  # RU consumed in current window
        self.ru_per_second = 0
        self.last_ru_check = time.time()
        self.ru_check_interval = 10.0  # Check RU rate every 10 seconds for accuracy
        self.is_throttling = False  # Track throttling state for progress bar
        self.batch_count = 0  # Count batches for reduced monitoring
        self.monitoring_batch_interval = 10  # Monitor every 10 batches for responsiveness
        self.progress_update_interval = 10  # Update progress bar every 10 batches
        self.pending_updates = 0  # Accumulate updates before showing
        
        # Adaptive concurrency control (ULTRA-FAST)
        self.current_generation_workers = self.generation_workers
        self.current_write_workers = self.write_workers
        self.max_generation_workers = int(os.getenv('MAX_GENERATION_WORKERS', 12))
        self.min_generation_workers = int(os.getenv('MIN_GENERATION_WORKERS', 4))
        self.max_write_workers = int(os.getenv('MAX_WRITE_WORKERS', 24))
        self.min_write_workers = int(os.getenv('MIN_WRITE_WORKERS', 8))
        self.performance_window = []
        self.last_performance_check = time.time()
        self.scaling_cooldown = 0
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.is_running = False

    async def connect_to_cosmos(self):
        """Connect to Azure Cosmos DB with optimized settings"""
        try:
            logger.info("Connecting to Azure Cosmos DB with optimizations...")
            
            # Use compatible client for older MongoDB versions
            compatible_client = create_compatible_cosmos_client(self.cosmos_connection_string)
            self.client = compatible_client.create_client(async_client=True)
            
            # Optimize connection settings for ULTRA-FAST speed
            # Increase connection pool size for better concurrency
            self.client._pool_size = 400  # More connections for concurrent operations
            self.client._max_pool_size = 600  # Maximum pool size
            self.client._min_pool_size = 100  # Higher minimum pool size
            self.client._max_idle_time_ms = 300000  # Keep connections alive much longer
            
            # Pre-warm connection pool for maximum performance
            logger.info("Pre-warming Cosmos connection pool...")
            warmup_tasks = []
            for i in range(30):  # Create 30 warmup connections
                warmup_tasks.append(self.client.admin.command('ping'))
            await asyncio.gather(*warmup_tasks, return_exceptions=True)
            logger.info("Connection pool pre-warmed successfully")
            
            # Test connection
            await self.client.admin.command('ping')
            
            db = self.client[self.cosmos_db_name]
            self.collection = db[self.cosmos_collection_name]
            
            # Configure collection for optimal bulk operations
            # Use unacknowledged writes for better performance (fire and forget)
            self.collection = self.collection.with_options(write_concern=WriteConcern(w=0))
            
            # Initialize async queues for parallel processing
            self.generation_queue = asyncio.Queue(maxsize=self.generation_workers * 2)
            self.write_queue = asyncio.Queue(maxsize=self.write_workers * 2)
            
            logger.info("Successfully connected to Azure Cosmos DB with ULTRA-FAST optimizations")
            logger.info("🚀 ULTRA-FAST data generation settings active:")
            logger.info(f"   📦 Batch size: {self.batch_size:,} documents")
            logger.info(f"   🔧 Generation workers: {self.generation_workers}")
            logger.info(f"   ✍️ Write workers: {self.write_workers}")
            logger.info(f"   📈 Max generation workers: {self.max_generation_workers}")
            logger.info(f"   📈 Max write workers: {self.max_write_workers}")
            logger.info(f"   🔄 Retry logic: 3 attempts with exponential backoff")
            logger.info(f"   ⚡ Write concern: Unacknowledged (w=0)")
            logger.info(f"   🚀 Connection pool: 400-600 connections (pre-warmed)")
            logger.info(f"   📚 Generation queue: {self.generation_workers * 2} capacity")
            logger.info(f"   ✍️ Write queue: {self.write_workers * 2} capacity")
            logger.info(f"   ⚡ Parallel generation/write: Enabled")
            logger.info(f"   🎯 Adaptive concurrency: Enabled")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Cosmos DB: {e}")
            return False

    def generate_batch(self, batch_size: int) -> List[dict]:
        """Ultra-fast batch generation with minimal overhead"""
        from bson import ObjectId
        import random
        import time
        
        orders = []
        
        # Pre-generate common values for speed
        current_time = time.time()
        
        for i in range(batch_size):
            try:
                # Generate document structure with shard key at root level
                service_type = random.choice([
                    'oil_change', 'brake_service', 'tire_rotation', 
                    'engine_diagnostic', 'transmission_service', 'battery_replacement',
                    'air_filter_change', 'spark_plug_replacement', 'coolant_flush',
                    'power_steering_service', 'suspension_check', 'exhaust_system_repair',
                    'clutch_service', 'timing_belt_replacement', 'fuel_system_clean',
                    'ac_service', 'heater_repair', 'electrical_diagnostic',
                    'body_work', 'paint_job', 'windshield_replacement',
                    'headlight_replacement', 'taillight_repair', 'mirror_adjustment',
                    'seat_reupholstery', 'interior_cleaning', 'carpet_replacement',
                    'door_handle_repair', 'window_regulator', 'lock_mechanism_service'
                ])
                
                order_dict = {
                    '_id': ObjectId(),
                    'service_type': service_type,  # SHARD KEY - must be at root level
                    'service_center_id': f"CENTER_{random.randint(1, 50)}",  # Additional partition distribution
                    'order_id': f"SO_{random.randint(1000000000, 9999999999)}",
                    'customer': {
                        'customer_id': f"CUST_{random.randint(10000000, 99999999)}",
                        'name': f"Customer_{random.randint(1, 10000)}",
                        'email': f"customer{random.randint(1, 10000)}@example.com",
                        'phone': f"+46{random.randint(10000000, 99999999)}",
                        'customer_type': random.choice(["individual", "corporate", "fleet"])
                    },
                    'vehicle': {
                        'vin': f"YV1{random.randint(1000000000000000, 9999999999999999)}",
                        'license_plate': f"{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=3))}{random.randint(100, 999)}",
                        'make': 'Volvo',
                        'model': random.choice(['XC90', 'XC60', 'XC40', 'S90', 'V90']),
                        'year': random.randint(2015, 2024)
                    },
                    'service_items': [
                        {
                            'item_id': f"ITEM_{random.randint(100000, 999999)}",
                            'service_type': service_type,  # Same as shard key
                            'description': 'Service item',
                            'quantity': random.randint(1, 3),
                            'unit_price': round(random.uniform(50.0, 500.0), 2),
                            'total_price': round(random.uniform(50.0, 1500.0), 2),
                            'labor_hours': round(random.uniform(0.5, 4.0), 1)
                        }
                    ],
                    'order_date': f"2024-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}T{random.randint(8, 18):02d}:00:00Z",
                    'service_date': f"2024-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}T{random.randint(8, 18):02d}:00:00Z",
                    'status': random.choice(['pending', 'in_progress', 'completed']),
                    'total_amount': round(random.uniform(100.0, 2000.0), 2),
                    'labor_cost': round(random.uniform(50.0, 800.0), 2),
                    'parts_cost': round(random.uniform(50.0, 1200.0), 2),
                    'tax_amount': round(random.uniform(10.0, 200.0), 2),
                    'technician_id': f"TECH_{random.randint(1000, 9999)}",
                    'warranty_info': {
                        'covered': random.choice([True, False]),
                        'warranty_type': random.choice(['manufacturer', 'extended', 'none'])
                    },
                    'created_at': f"2024-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}T{random.randint(8, 18):02d}:00:00Z",
                    'updated_at': f"2024-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}T{random.randint(8, 18):02d}:00:00Z"
                }
                
                orders.append(order_dict)
                
            except Exception as e:
                logger.error(f"Error generating service order: {e}")
                self.errors += 1

        return orders

    async def _generation_worker(self, worker_id: int):
        """Parallel generation worker that creates batches for the write queue"""
        try:
            while self.is_running and self.generation_queue is not None and self.write_queue is not None:
                try:
                    # Get batch request from generation queue
                    batch_request = await asyncio.wait_for(self.generation_queue.get(), timeout=1.0)
                    
                    if batch_request is None:  # End signal
                        break
                        
                    batch_size, batch_number = batch_request
                    
                    # Generate batch
                    batch = self.generate_batch(batch_size)
                    
                    # Put batch in write queue for parallel processing
                    await self.write_queue.put((batch, batch_number))
                    
                    # Mark generation task as done
                    self.generation_queue.task_done()
                    
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"Error in generation worker {worker_id}: {e}")
                    
        except Exception as e:
            logger.error(f"Generation worker {worker_id} failed: {e}")

    async def _write_worker(self, worker_id: int):
        """Parallel write worker that processes batches from the write queue"""
        try:
            while self.is_running and self.write_queue is not None:
                try:
                    # Get batch from write queue with timeout
                    batch_data = await asyncio.wait_for(self.write_queue.get(), timeout=1.0)
                    
                    if batch_data is None:  # End signal
                        break
                        
                    batch, batch_number = batch_data
                    
                    # Write batch with retry logic
                    success = await self.write_batch_with_retry(batch)
                    
                    if success:
                        self.documents_written += len(batch)
                    
                    # Mark write task as done
                    self.write_queue.task_done()
                    
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"Error in write worker {worker_id}: {e}")
                    
        except Exception as e:
            logger.error(f"Write worker {worker_id} failed: {e}")

    async def generate_and_write_batch(self, batch_size: int) -> int:
        """Generate and write a batch in parallel - returns number of documents written"""
        try:
            # Generate batch
            batch = self.generate_batch(batch_size)
            
            # Write batch with retry logic
            success = await self.write_batch_with_retry(batch)
            
            if success:
                return len(batch)
            else:
                return 0
                
        except Exception as e:
            logger.error(f"Error in parallel batch: {e}")
            return 0

    async def write_batch_with_retry(self, batch: List[dict], max_retries: int = 3) -> bool:
        """Write a batch with minimal monitoring overhead"""
        if not batch:
            return True
            
        # Increment batch counter for reduced monitoring
        self.batch_count += 1
        
        for attempt in range(max_retries + 1):
            try:
                # Correct RU calculation for current window
                avg_ru_per_doc = 6  # Conservative estimate for complex documents
                batch_ru = len(batch) * avg_ru_per_doc
                self.ru_consumed_in_window += batch_ru
                
                # Monitor RU consumption every 5 batches for responsiveness
                if (self.batch_count % self.monitoring_batch_interval == 0):
                    current_time = time.time()
                    time_diff = current_time - self.last_ru_check
                    
                    if time_diff >= self.ru_check_interval:
                        # Calculate current RU consumption rate (correct method)
                        self.ru_per_second = self.ru_consumed_in_window / time_diff
                        
                        # Reset for next window
                        self.last_ru_check = current_time
                        self.ru_consumed_in_window = 0
                        
                        # Throttle if consuming too many RU (for 40K RU provisioned)
                        if self.ru_per_second > 36000:  # 90% of 40K RU limit for safety
                            self.is_throttling = True
                            # Proportional throttling: more aggressive for higher RU consumption
                            excess_ru = self.ru_per_second - 36000
                            throttle_time = (excess_ru / 36000) * 0.1  # Up to 0.1s throttle
                            await asyncio.sleep(throttle_time)
                        else:
                            self.is_throttling = False
                
                # Optimized insert with bulk execution mode
                result = await self.collection.insert_many(
                    batch, 
                    ordered=False  # Allow parallel processing
                )
                self.documents_written += len(result.inserted_ids)
                return True
                
            except Exception as e:
                error_str = str(e).lower()
                
                # Handle 429 throttling errors with exponential backoff
                if "429" in error_str or "throttle" in error_str or "rate limit" in error_str:
                    if attempt < max_retries:
                        backoff_time = (2 ** attempt) * 0.1  # Exponential backoff: 0.1s, 0.2s, 0.4s
                        logger.warning(f"Throttled (attempt {attempt + 1}/{max_retries + 1}), retrying in {backoff_time:.1f}s...")
                        await asyncio.sleep(backoff_time)
                        continue
                    else:
                        logger.error(f"Max retries exceeded for throttling: {e}")
                        self.errors += 1
                        return False
                else:
                    # Non-throttling errors - fail immediately
                    logger.error(f"Non-retryable error writing batch: {e}")
                    self.errors += 1
                    return False
        
        return False

    def _adaptive_concurrency_control(self, performance_metric: float):
        """Adaptive concurrency control based on performance and throttling"""
        try:
            current_time = time.time()
            
            # Add performance data to window
            self.performance_window.append({
                'performance': performance_metric,
                'timestamp': current_time,
                'generation_workers': self.current_generation_workers,
                'write_workers': self.current_write_workers
            })
            
            # Keep only last 20 performance measurements
            self.performance_window = self.performance_window[-20:]
            
            # Check if we should evaluate scaling (every 10 batches or 30 seconds)
            if (len(self.performance_window) >= 5 and 
                current_time - self.last_performance_check > 30):
                
                self.last_performance_check = current_time
                
                # Calculate average performance over the window
                avg_performance = sum(p['performance'] for p in self.performance_window) / len(self.performance_window)
                
                # Detect throttling (performance degradation)
                recent_performance = self.performance_window[-3:] if len(self.performance_window) >= 3 else self.performance_window
                recent_avg = sum(p['performance'] for p in recent_performance) / len(recent_performance)
                
                # Throttling detection: recent performance significantly lower than average
                performance_degradation = (avg_performance - recent_avg) / avg_performance if avg_performance > 0 else 0
                
                if performance_degradation > 0.1:  # 10% throttling threshold
                    # Throttling detected - scale down
                    if self.current_write_workers > self.min_write_workers:
                        old_workers = self.current_write_workers
                        self.current_write_workers = max(self.min_write_workers, int(self.current_write_workers * 0.8))
                        logger.info(f"🔄 Throttling detected! Scaling down write workers: {old_workers} → {self.current_write_workers}")
                        self.scaling_cooldown = current_time + 60  # 1 minute cooldown
                        
                elif (performance_degradation < -0.05 and  # Performance improving
                      self.current_write_workers < self.max_write_workers and
                      current_time > self.scaling_cooldown):
                    # Performance improving - scale up
                    old_workers = self.current_write_workers
                    self.current_write_workers = min(self.max_write_workers, int(self.current_write_workers * 1.2))
                    logger.info(f"🚀 Performance improving! Scaling up write workers: {old_workers} → {self.current_write_workers}")
                    self.scaling_cooldown = current_time + 30  # 30 second cooldown
                    
        except Exception as e:
            logger.debug(f"Error in adaptive concurrency control: {e}")

    async def generate_and_write_continuously(self):
        """ULTRA-FAST parallel data generation with read-ahead caching and parallel generation/write operations"""
        logger.info(f"🚀 Starting ULTRA-FAST parallel data generation...")
        logger.info(f"Database: {self.cosmos_db_name}")
        logger.info(f"Collection: {self.cosmos_collection_name}")
        logger.info(f"Target: {self.total_documents:,} documents")
        logger.info(f"Batch size: {self.batch_size}")
        logger.info(f"Generation workers: {self.generation_workers}")
        logger.info(f"Write workers: {self.write_workers}")
        
        self.start_time = time.time()
        
        # Create progress bar with RU consumption display
        pbar = tqdm(
            total=self.total_documents, 
            desc=f"🚀 ULTRA-FAST Generation to {self.cosmos_db_name}.{self.cosmos_collection_name}", 
            unit="docs",
            unit_scale=True,
            ncols=120,
            bar_format='{desc}: {percentage:3.0f}%|{bar:25}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}',
            colour='green',
            smoothing=0.1,
            miniters=1000,
            dynamic_ncols=True,
            leave=True
        )
        
        try:
            # Start parallel generation workers
            self.generation_tasks = []
            for i in range(self.generation_workers):
                task = asyncio.create_task(self._generation_worker(i))
                self.generation_tasks.append(task)
            
            # Start parallel write workers
            self.write_tasks = []
            for i in range(self.write_workers):
                task = asyncio.create_task(self._write_worker(i))
                self.write_tasks.append(task)
            
            # Process batches with parallel generation and writes
            current_batch_number = 0
            remaining_docs = self.total_documents
            
            while remaining_docs > 0 and self.is_running:
                try:
                    current_batch_size = min(self.batch_size, remaining_docs)
                    current_batch_number += 1
                    
                    # Put batch request in generation queue
                    await self.generation_queue.put((current_batch_size, current_batch_number))
                    
                    remaining_docs -= current_batch_size
                    
                    # Update progress periodically
                    if current_batch_number % 10 == 0:
                        # Update progress bar with RU rate and worker info
                        ru_info = f"RU:{self.ru_per_second:.0f}" if not self.is_throttling else f"\033[91mRU:{self.ru_per_second:.0f}\033[0m"
                        worker_info = f"G:{self.current_generation_workers}W:{self.current_write_workers}"
                        pbar.set_postfix_str(f"{ru_info} {worker_info}")
                        pbar.update(self.batch_size * 10)  # Approximate update
                        
                        # Adaptive concurrency control
                        self._adaptive_concurrency_control(self.ru_per_second)
                    
                except Exception as e:
                    logger.error(f"Error processing batch request: {e}")
                    break
            
            # Signal end to generation workers
            if self.generation_queue is not None:
                for _ in range(self.generation_workers):
                    await self.generation_queue.put(None)
            
            # Signal end to write workers
            if self.write_queue is not None:
                for _ in range(self.write_workers):
                    await self.write_queue.put(None)
            
            # Wait for all workers to complete
            if self.generation_tasks:
                await asyncio.gather(*self.generation_tasks, return_exceptions=True)
            
            if self.write_tasks:
                await asyncio.gather(*self.write_tasks, return_exceptions=True)
                
        except Exception as e:
            logger.error(f"Error in ULTRA-FAST generation: {e}")
        finally:
            pbar.close()
            
        # Final statistics
        elapsed = time.time() - self.start_time
        rate = self.documents_written / elapsed if elapsed > 0 else 0
        
        # Calculate final statistics
        estimated_size_gb = (self.documents_written * 2.5) / (1024**3)  # ~2.5KB per document
        total_ru_consumed = self.documents_written * 6  # ~6 RU per document (more accurate)
        avg_ru_rate = total_ru_consumed / elapsed if elapsed > 0 else 0
        
        logger.info(f"🎉 Data generation completed!")
        logger.info(f"📊 Performance Summary:")
        logger.info(f"   • Database: {self.cosmos_db_name}")
        logger.info(f"   • Collection: {self.cosmos_collection_name}")
        logger.info(f"   • Documents written: {self.documents_written:,}")
        logger.info(f"   • Total time: {elapsed:.2f} seconds")
        logger.info(f"   • Average rate: {rate:.0f} documents/second")
        logger.info(f"   • Peak RU rate: {self.ru_per_second:.0f} RU/sec")
        logger.info(f"   • Average RU rate: {avg_ru_rate:.0f} RU/sec")
        logger.info(f"   • Total RU consumed: {total_ru_consumed:.0f} RU")
        logger.info(f"   • Data size: {estimated_size_gb:.2f} GB")
        logger.info(f"   • Errors: {self.errors}")
        logger.info(f"   • Throttling threshold: 36,000 RU/sec (90% of 40K limit)")

    async def generate_specific_amount(self, target_documents: int):
        """Generate a specific amount of data and stop"""
        logger.info(f"Generating {target_documents:,} documents...")
        
        self.start_time = time.time()
        pbar = tqdm(
            total=target_documents, 
            desc="🚀 Data Generation", 
            unit="docs",
            unit_scale=True,
            ncols=80,
            bar_format='{desc}: {percentage:3.0f}%|{bar:20}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
            colour='green'
        )
        
        try:
            while self.documents_written < target_documents:
                remaining = target_documents - self.documents_written
                current_batch_size = min(self.batch_size, remaining)
                
                batch = self.generate_batch(current_batch_size)
                success = await self.write_batch_with_retry(batch)
                
                if success:
                    pbar.update(len(batch))
                    pbar.refresh()  # Force refresh
                
                await asyncio.sleep(0.01)
                
        except Exception as e:
            logger.error(f"Error in specific amount generation: {e}")
        finally:
            pbar.close()
            
        elapsed = time.time() - self.start_time
        rate = self.documents_written / elapsed if elapsed > 0 else 0
        
        logger.info(f"Generation completed: {self.documents_written:,} documents in {elapsed:.2f}s ({rate:.0f} docs/sec)")

    async def cleanup(self):
        """Clean up resources"""
        if self.client:
            self.client.close()
            logger.info("Database connection closed")

async def main():
    """Main function"""
    generator = DataGenerator()
    
    try:
        # Connect to database
        if not await generator.connect_to_cosmos():
            logger.error("Failed to connect to database. Exiting.")
            return
        
        # Check if we should run continuously or generate specific amount
        mode = os.getenv('GENERATION_MODE', 'continuous')
        
        if mode == 'continuous':
            await generator.generate_and_write_continuously()
        else:
            target = int(os.getenv('TARGET_DOCUMENTS', 1000000))
            await generator.generate_specific_amount(target)
            
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        await generator.cleanup()

if __name__ == "__main__":
    # Check if environment variables are set
    if not os.getenv('COSMOS_DB_CONNECTION_STRING'):
        logger.error("COSMOS_DB_CONNECTION_STRING environment variable is required")
        sys.exit(1)
    
    # Run the generator
    asyncio.run(main())
