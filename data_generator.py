"""
Volvo Service Orders Data Generator
Generates and continuously writes service order data to Azure Cosmos DB
Optimized for maximum speed with UVLoop and advanced concurrency
"""
import asyncio
import os
import time
from datetime import datetime
from typing import List, Optional
import logging

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass  # uvloop not available, using default event loop
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
    """
    High-performance data generator for Volvo service orders.
    
    This class generates realistic Volvo service order data and writes it to Azure Cosmos DB
    using parallel processing, batch aggregation, and optimized connection pooling for
    maximum throughput.
    
    Features:
    - Parallel write workers with batch aggregation
    - Bulk write operations for maximum efficiency
    - RU consumption monitoring and throttling
    - Adaptive concurrency control
    - Real-time progress tracking
    - Configurable via environment variables (GEN_ prefix)
    
    Environment Variables:
    - GEN_DB_CONNECTION_STRING: Cosmos DB connection string
    - GEN_DB_NAME: Database name (default: volvo-service-orders)
    - GEN_DB_COLLECTION: Collection name (default: serviceorders)
    - GEN_BATCH_SIZE: Batch size for generation (default: 5000)
    - GEN_TOTAL_DOCUMENTS: Total documents to generate (default: 10000000)
    - GEN_WORKERS: Number of generation workers (default: 8)
    - GEN_WRITE_WORKERS: Number of write workers (default: 16)
    - GEN_MAX_WORKERS: Maximum workers for adaptive scaling (default: 12)
    - GEN_MIN_WORKERS: Minimum workers for adaptive scaling (default: 4)
    - GEN_MAX_WRITE_WORKERS: Maximum write workers (default: 24)
    - GEN_MIN_WRITE_WORKERS: Minimum write workers (default: 8)
    - GEN_POOL_SIZE: Connection pool size (default: 400)
    - GEN_MAX_POOL_SIZE: Maximum pool size (default: 600)
    - GEN_MIN_POOL_SIZE: Minimum pool size (default: 100)
    - GEN_MAX_IDLE_TIME_MS: Max idle time in ms (default: 300000)
    - GEN_WARMUP_CONNECTIONS: Connections to pre-warm (default: 30)
    - GEN_QUEUE_MULTIPLIER: Generation queue multiplier (default: 10)
    - GEN_WRITE_QUEUE_MULTIPLIER: Write queue multiplier (default: 10)
    - GEN_BATCH_AGGREGATION_SIZE: Batches to aggregate (default: 5)
    - GEN_BATCH_AGGREGATION_TIMEOUT_MS: Aggregation timeout (default: 200)
    - GEN_RU_PER_DOCUMENT: RU per document (default: 6)
    - GEN_RU_CHECK_INTERVAL_SECONDS: RU check interval (default: 10)
    - GEN_MONITORING_BATCH_INTERVAL: Monitoring interval (default: 10)
    - GEN_PROGRESS_UPDATE_INTERVAL_BATCHES: Progress update interval (default: 10)
    - GEN_RU_THROTTLE_THRESHOLD: RU throttle threshold (default: 40000)
    - GEN_RU_THROTTLE_PERCENTAGE: RU throttle percentage (default: 90)
    - GEN_DISABLE_THROTTLING: Disable throttling (default: true)
    - GEN_MAX_RETRIES: Maximum retries (default: 3)
    - GEN_BACKOFF_BASE_SECONDS: Backoff base seconds (default: 0.1)
    - GEN_PERFORMANCE_WINDOW_SIZE: Performance window size (default: 20)
    - GEN_PERFORMANCE_CHECK_INTERVAL_SECONDS: Performance check interval (default: 30)
    - GEN_THROTTLING_THRESHOLD_PERCENTAGE: Throttling threshold (default: 10)
    - GEN_SCALING_COOLDOWN_SECONDS: Scaling cooldown (default: 60)
    - GEN_SCALE_UP_COOLDOWN_SECONDS: Scale up cooldown (default: 30)
    - GEN_PROGRESS_SMOOTHING: Progress bar smoothing (default: 0.1)
    - GEN_PROGRESS_MINITERS: Progress bar miniters (default: 1000)
    - GEN_DOCUMENT_SIZE_KB: Document size in KB (default: 2.5)
    - GEN_DOCUMENT_SIZE_MULTIPLIER: Document size multiplier (default: 1024)
    
    Example:
        generator = DataGenerator()
        await generator.connect_to_cosmos()
        await generator.generate_specific_amount(1000000)
        await generator.cleanup()
    """
    
    def __init__(self):
        # Database connection settings (using GEN_ prefix for all database settings)
        self.cosmos_connection_string = os.getenv('GEN_DB_CONNECTION_STRING')
        self.cosmos_db_name = os.getenv('GEN_DB_NAME', 'volvo-service-orders')
        self.cosmos_collection_name = os.getenv('GEN_DB_COLLECTION', 'serviceorders')
        
        # Basic generation settings (using new GEN_ prefix)
        self.batch_size = int(os.getenv('GEN_BATCH_SIZE', 5000))
        self.total_documents = int(os.getenv('GEN_TOTAL_DOCUMENTS', 10000000))
        self.concurrent_workers = int(os.getenv('GEN_WORKERS', 16))
        
        # Parallel generation/write optimization (using new GEN_ prefix)
        self.generation_workers = int(os.getenv('GEN_WORKERS', 8))
        self.write_workers = int(os.getenv('GEN_WRITE_WORKERS', 16))
        self.generation_queue = None  # Will be initialized in connect_to_cosmos
        self.write_queue = None  # Will be initialized in connect_to_cosmos
        self.generation_tasks = []
        self.write_tasks = []
        
        # Connection pool settings (using new GEN_ prefix)
        self.pool_size = int(os.getenv('GEN_POOL_SIZE', 400))
        self.max_pool_size = int(os.getenv('GEN_MAX_POOL_SIZE', 600))
        self.min_pool_size = int(os.getenv('GEN_MIN_POOL_SIZE', 100))
        self.max_idle_time_ms = int(os.getenv('GEN_MAX_IDLE_TIME_MS', 300000))
        self.warmup_connections = int(os.getenv('GEN_WARMUP_CONNECTIONS', 30))
        
        # Queue settings (using new GEN_ prefix)
        self.generation_queue_multiplier = int(os.getenv('GEN_QUEUE_MULTIPLIER', 10))
        self.write_queue_multiplier = int(os.getenv('GEN_WRITE_QUEUE_MULTIPLIER', 10))
        
        # Batch aggregation settings (CONFIGURABLE)
        self.batch_aggregation_size = int(os.getenv('GEN_BATCH_AGGREGATION_SIZE', 5))
        self.batch_aggregation_timeout_ms = int(os.getenv('GEN_BATCH_AGGREGATION_TIMEOUT_MS', 200))
        
        # RU monitoring settings (CONFIGURABLE)
        self.ru_per_document = int(os.getenv('GEN_RU_PER_DOCUMENT', 6))
        self.ru_check_interval = float(os.getenv('GEN_RU_CHECK_INTERVAL_SECONDS', 10.0))
        self.monitoring_batch_interval = int(os.getenv('GEN_MONITORING_BATCH_INTERVAL', 10))
        self.progress_update_interval = int(os.getenv('GEN_PROGRESS_UPDATE_INTERVAL_BATCHES', 10))
        self.ru_throttle_threshold = int(os.getenv('GEN_RU_THROTTLE_THRESHOLD', 40000))
        self.ru_throttle_percentage = int(os.getenv('GEN_RU_THROTTLE_PERCENTAGE', 90))
        
        # Retry logic settings (CONFIGURABLE)
        self.max_retries = int(os.getenv('GEN_MAX_RETRIES', 3))
        self.backoff_base_seconds = float(os.getenv('GEN_BACKOFF_BASE_SECONDS', 0.1))
        
        # Performance monitoring settings (CONFIGURABLE)
        self.performance_window_size = int(os.getenv('GEN_PERFORMANCE_WINDOW_SIZE', 20))
        self.performance_check_interval = int(os.getenv('GEN_PERFORMANCE_CHECK_INTERVAL_SECONDS', 30))
        self.throttling_threshold_percentage = float(os.getenv('GEN_THROTTLING_THRESHOLD_PERCENTAGE', 10.0))
        self.scaling_cooldown_seconds = int(os.getenv('GEN_SCALING_COOLDOWN_SECONDS', 60))
        self.scale_up_cooldown_seconds = int(os.getenv('GEN_SCALE_UP_COOLDOWN_SECONDS', 30))
        
        # Progress bar settings (CONFIGURABLE)
        self.progress_smoothing = float(os.getenv('GEN_PROGRESS_SMOOTHING', 0.1))
        self.progress_miniters = int(os.getenv('GEN_PROGRESS_MINITERS', 1000))
        
        # Document size estimation (CONFIGURABLE)
        self.document_size_kb = float(os.getenv('GEN_DOCUMENT_SIZE_KB', 2.5))
        self.document_size_multiplier = int(os.getenv('GEN_DOCUMENT_SIZE_MULTIPLIER', 1024))
        
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
        self.disable_throttling = os.getenv('GEN_DISABLE_THROTTLING', 'false').lower() == 'true'
        self.is_throttling = False  # Track throttling state for progress bar
        self.batch_count = 0  # Count batches for reduced monitoring
        self.pending_updates = 0  # Accumulate updates before showing
        
        # Adaptive concurrency control (using new GEN_ prefix)
        self.current_generation_workers = self.generation_workers
        self.current_write_workers = self.write_workers
        self.max_generation_workers = int(os.getenv('GEN_MAX_WORKERS', 12))
        self.min_generation_workers = int(os.getenv('GEN_MIN_WORKERS', 4))
        self.max_write_workers = int(os.getenv('GEN_MAX_WRITE_WORKERS', 24))
        self.min_write_workers = int(os.getenv('GEN_MIN_WRITE_WORKERS', 8))
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
            
            # Optimize connection settings for ULTRA-FAST speed (CONFIGURABLE)
            # Increase connection pool size for better concurrency
            self.client._pool_size = self.pool_size
            self.client._max_pool_size = self.max_pool_size
            self.client._min_pool_size = self.min_pool_size
            self.client._max_idle_time_ms = self.max_idle_time_ms
            
            # Pre-warm connection pool for maximum performance
            logger.info(f"Pre-warming Cosmos connection pool ({self.warmup_connections} connections)...")
            warmup_tasks = []
            for i in range(self.warmup_connections):
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
            
            # Initialize async queues for parallel processing (CONFIGURABLE)
            # Deep queues to keep workers saturated
            self.generation_queue = asyncio.Queue(maxsize=self.generation_workers * self.generation_queue_multiplier)
            self.write_queue = asyncio.Queue(maxsize=self.write_workers * self.write_queue_multiplier)
            
            logger.info("Successfully connected to Azure Cosmos DB with ULTRA-FAST optimizations")
            logger.info("🚀 ULTRA-FAST data generation settings active:")
            logger.info(f"   📦 Batch size: {self.batch_size:,} documents")
            logger.info(f"   🔧 Generation workers: {self.generation_workers}")
            logger.info(f"   ✍️ Write workers: {self.write_workers}")
            logger.info(f"   📈 Max generation workers: {self.max_generation_workers}")
            logger.info(f"   📈 Max write workers: {self.max_write_workers}")
            logger.info(f"   🔄 Retry logic: {self.max_retries} attempts with exponential backoff")
            logger.info(f"   ⚡ Write concern: Unacknowledged (w=0)")
            logger.info(f"   🚀 Connection pool: {self.pool_size}-{self.max_pool_size} connections (pre-warmed: {self.warmup_connections})")
            logger.info(f"   📚 Generation queue: {self.generation_workers * self.generation_queue_multiplier} capacity")
            logger.info(f"   ✍️ Write queue: {self.write_workers * self.write_queue_multiplier} capacity")
            logger.info(f"   ⚡ Parallel generation/write: Enabled")
            logger.info(f"   🎯 Adaptive concurrency: Enabled")
            logger.info(f"   📊 RU per document: {self.ru_per_document}")
            logger.info(f"   🚦 RU throttle threshold: {self.ru_throttle_threshold} ({self.ru_throttle_percentage}% of limit)")
            logger.info(f"   🔄 Batch aggregation: {self.batch_aggregation_size} batches or {self.batch_aggregation_timeout_ms}ms")
            
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

    async def _write_worker(self, worker_id: int, pbar=None):
        """ULTRA-FAST write worker with batch aggregation for maximum throughput (like Atlas migration)"""
        try:
            batch_buffer = []
            batch_count = 0
            last_process_time = time.time()
            
            while self.is_running and self.write_queue is not None:
                try:
                    # Collect batches with timeout to prevent getting stuck
                    try:
                        batch_data = await asyncio.wait_for(self.write_queue.get(), timeout=2.0)
                    except asyncio.TimeoutError:
                        # Process any buffered batches if timeout occurs
                        if batch_buffer:
                            await self.write_batch_aggregated(batch_buffer, worker_id, pbar)
                            batch_buffer = []
                            batch_count = 0
                        continue
                    
                    if batch_data is None:  # End signal
                        # Process remaining batches in buffer
                        if batch_buffer:
                            await self.write_batch_aggregated(batch_buffer, worker_id, pbar)
                        break
                        
                    batch, batch_number = batch_data
                    batch_buffer.append(batch)
                    batch_count += 1
                    
                    # Process aggregated batches when buffer is full OR after timeout
                    current_time = time.time()
                    should_process = (
                        len(batch_buffer) >= self.batch_aggregation_size or  # Buffer full
                        (batch_buffer and current_time - last_process_time > self.batch_aggregation_timeout_ms / 1000.0)  # Timeout
                    )
                    
                    if should_process:
                        await self.write_batch_aggregated(batch_buffer, worker_id, pbar)
                        batch_buffer = []
                        batch_count = 0
                        last_process_time = current_time
                    
                    # Mark task as done
                    self.write_queue.task_done()
                    
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
                # Correct RU calculation for current window (CONFIGURABLE)
                batch_ru = len(batch) * self.ru_per_document
                self.ru_consumed_in_window += batch_ru
                
                # Monitor RU consumption every N batches for responsiveness (CONFIGURABLE)
                if (self.batch_count % self.monitoring_batch_interval == 0):
                    current_time = time.time()
                    time_diff = current_time - self.last_ru_check
                    
                    if time_diff >= self.ru_check_interval:
                        # Calculate current RU consumption rate (correct method)
                        self.ru_per_second = self.ru_consumed_in_window / time_diff
                        
                        # Reset for next window
                        self.last_ru_check = current_time
                        self.ru_consumed_in_window = 0
                        
                        # Throttle if consuming too many RU (CONFIGURABLE)
                        if not self.disable_throttling:
                            if self.ru_per_second > self.ru_throttle_threshold:
                                self.is_throttling = True
                                # Proportional throttling: more aggressive for higher RU consumption
                                excess_ru = self.ru_per_second - self.ru_throttle_threshold
                                throttle_time = (excess_ru / self.ru_throttle_threshold) * 0.1  # Up to 0.1s throttle
                                await asyncio.sleep(throttle_time)
                            else:
                                self.is_throttling = False
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
                
                # Handle 429 throttling errors with exponential backoff (CONFIGURABLE)
                if "429" in error_str or "throttle" in error_str or "rate limit" in error_str:
                    if attempt < max_retries:
                        backoff_time = (2 ** attempt) * self.backoff_base_seconds  # Configurable exponential backoff
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

    async def write_batch_aggregated(self, batches: List[List[dict]], worker_id: int, pbar=None) -> bool:
        """ULTRA-FAST aggregated batch write with bulk operations (like Atlas migration)"""
        if not batches:
            return True
            
        try:
            # Flatten batches
            all_docs = []
            for batch in batches:
                all_docs.extend(batch)
            
            if not all_docs:
                return True
            
            logger.debug(f"Worker {worker_id}: Processing aggregated batch of {len(all_docs)} documents")
            
            # ULTRA-FAST bulk write with maximum parallelization
            async def _bulk_write_aggregated():
                try:
                    from pymongo import InsertOne
                    
                    # Convert all documents to bulk operations
                    bulk_operations = [InsertOne(doc) for doc in all_docs]
                    
                    # Execute bulk write with optimized settings
                    result = await self.collection.bulk_write(
                        bulk_operations,
                        ordered=False,  # Allow parallel processing
                        bypass_document_validation=False
                    )
                    
                    # Update counters
                    self.documents_written += len(all_docs)
                    
                    # Update progress bar
                    if pbar:
                        pbar.update(len(all_docs))
                        pbar.refresh()
                    
                    logger.debug(f"Worker {worker_id}: Successfully wrote {len(all_docs)} documents")
                    return True
                    
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Bulk write failed: {e}")
                    # Fallback to individual batch writes
                    return await self._fallback_batch_writes(all_docs, worker_id, pbar)
            
            return await _bulk_write_aggregated()
            
        except Exception as e:
            logger.error(f"Worker {worker_id}: Error in write_batch_aggregated: {e}")
            return False
    
    async def _fallback_batch_writes(self, all_docs: List[dict], worker_id: int, pbar=None) -> bool:
        """Fallback to individual batch writes if bulk write fails"""
        try:
            # Split into smaller chunks and write individually
            chunk_size = 1000
            for i in range(0, len(all_docs), chunk_size):
                chunk = all_docs[i:i + chunk_size]
                success = await self.write_batch_with_retry(chunk)
                if success and pbar:
                    pbar.update(len(chunk))
                    pbar.refresh()
            return True
        except Exception as e:
            logger.error(f"Worker {worker_id}: Fallback writes failed: {e}")
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
            
            # Keep only last N performance measurements (CONFIGURABLE)
            self.performance_window = self.performance_window[-self.performance_window_size:]
            
            # Check if we should evaluate scaling (CONFIGURABLE)
            if (len(self.performance_window) >= 5 and 
                current_time - self.last_performance_check > self.performance_check_interval):
                
                self.last_performance_check = current_time
                
                # Calculate average performance over the window
                avg_performance = sum(p['performance'] for p in self.performance_window) / len(self.performance_window)
                
                # Detect throttling (performance degradation)
                recent_performance = self.performance_window[-3:] if len(self.performance_window) >= 3 else self.performance_window
                recent_avg = sum(p['performance'] for p in recent_performance) / len(recent_performance)
                
                # Throttling detection: recent performance significantly lower than average (CONFIGURABLE)
                performance_degradation = (avg_performance - recent_avg) / avg_performance if avg_performance > 0 else 0
                
                if performance_degradation > (self.throttling_threshold_percentage / 100.0):
                    # Throttling detected - scale down
                    if self.current_write_workers > self.min_write_workers:
                        old_workers = self.current_write_workers
                        self.current_write_workers = max(self.min_write_workers, int(self.current_write_workers * 0.8))
                        logger.info(f"🔄 Throttling detected! Scaling down write workers: {old_workers} → {self.current_write_workers}")
                        self.scaling_cooldown = current_time + self.scaling_cooldown_seconds
                        
                elif (performance_degradation < -0.05 and  # Performance improving
                      self.current_write_workers < self.max_write_workers and
                      current_time > self.scaling_cooldown):
                    # Performance improving - scale up
                    old_workers = self.current_write_workers
                    self.current_write_workers = min(self.max_write_workers, int(self.current_write_workers * 1.2))
                    logger.info(f"🚀 Performance improving! Scaling up write workers: {old_workers} → {self.current_write_workers}")
                    self.scaling_cooldown = current_time + self.scale_up_cooldown_seconds
                    
        except Exception as e:
            logger.debug(f"Error in adaptive concurrency control: {e}")

    async def generate_specific_amount(self, target_documents: int):
        """Generate a specific amount of data using parallel write workers (like Atlas migration)"""
        logger.info(f"🚀 Starting ULTRA-FAST parallel data generation...")
        logger.info(f"Database: {self.cosmos_db_name}")
        logger.info(f"Collection: {self.cosmos_collection_name}")
        logger.info(f"Target: {target_documents:,} documents")
        logger.info(f"Batch size: {self.batch_size}")
        logger.info(f"Write workers: {self.write_workers}")
        
        self.start_time = time.time()
        self.total_documents = target_documents
        
        # Create progress bar
        pbar = tqdm(
            total=target_documents, 
            desc=f"🚀 ULTRA-FAST Generation to {self.cosmos_db_name}.{self.cosmos_collection_name}", 
            unit="docs",
            unit_scale=True,
            ncols=120,
            bar_format='{desc}: {percentage:3.0f}%|{bar:25}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}',
            colour='green',
            smoothing=self.progress_smoothing,
            miniters=self.progress_miniters,
            dynamic_ncols=True,
            leave=True
        )
        
        try:
            # Start parallel write workers
            self.write_tasks = []
            for i in range(self.write_workers):
                task = asyncio.create_task(self._write_worker(i, pbar))
                self.write_tasks.append(task)
            
            # Generate and queue batches
            current_batch_number = 0
            remaining_docs = target_documents
            
            while remaining_docs > 0 and self.is_running:
                try:
                    current_batch_size = min(self.batch_size, remaining_docs)
                    current_batch_number += 1
                    
                    # Generate batch
                    batch = self.generate_batch(current_batch_size)
                    
                    # Put batch in write queue for parallel processing
                    await self.write_queue.put((batch, current_batch_number))
                    
                    remaining_docs -= current_batch_size
                    
                    # Update progress periodically
                    if current_batch_number % self.progress_update_interval == 0:
                        ru_info = f"RU:{self.ru_per_second:.0f}"
                        worker_info = f"W:{self.write_workers}"
                        pbar.set_postfix_str(f"{ru_info} {worker_info}")
                        pbar.refresh()
                        
                except Exception as e:
                    logger.error(f"Error processing batch request: {e}")
                    break
            
            # Signal end to write workers
            if self.write_queue is not None:
                for _ in range(self.write_workers):
                    await self.write_queue.put(None)
            
            # Wait for all write workers to complete
            if self.write_tasks:
                await asyncio.gather(*self.write_tasks, return_exceptions=True)
                
        except Exception as e:
            logger.error(f"Error in ULTRA-FAST generation: {e}")
        finally:
            pbar.close()
            
        # Final statistics
        elapsed = time.time() - self.start_time
        rate = self.documents_written / elapsed if elapsed > 0 else 0
        
        logger.info(f"🎉 Data generation completed!")
        logger.info(f"📊 Performance Summary:")
        logger.info(f"   • Database: {self.cosmos_db_name}")
        logger.info(f"   • Collection: {self.cosmos_collection_name}")
        logger.info(f"   • Documents written: {self.documents_written:,}")
        logger.info(f"   • Total time: {elapsed:.2f} seconds")
        logger.info(f"   • Average rate: {rate:.0f} documents/second")
        logger.info(f"   • Peak RU rate: {self.ru_per_second:.0f} RU/sec")
        logger.info(f"   • Errors: {self.errors}")

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
        
        # Generate specific amount of documents (user must specify GEN_TOTAL_DOCUMENTS)
        target = int(os.getenv('GEN_TOTAL_DOCUMENTS', 1000000))
        logger.info(f"Generating {target:,} documents...")
        await generator.generate_specific_amount(target)
            
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        await generator.cleanup()

if __name__ == "__main__":
    # Validate required environment variables (using GEN_ prefix)
    connection_string = os.getenv('GEN_DB_CONNECTION_STRING')
    db_name = os.getenv('GEN_DB_NAME')
    collection_name = os.getenv('GEN_DB_COLLECTION')
    
    if not connection_string:
        logger.error("Missing required environment variable: GEN_DB_CONNECTION_STRING")
        sys.exit(1)
    if not db_name:
        logger.error("Missing required environment variable: GEN_DB_NAME")
        sys.exit(1)
    if not collection_name:
        logger.error("Missing required environment variable: GEN_DB_COLLECTION")
        sys.exit(1)
    
    # Validate numeric environment variables (GEN_ prefix only)
    numeric_vars = [
        'GEN_BATCH_SIZE', 'GEN_TOTAL_DOCUMENTS', 'GEN_WORKERS', 'GEN_WRITE_WORKERS',
        'GEN_MAX_WORKERS', 'GEN_MIN_WORKERS', 'GEN_MAX_WRITE_WORKERS', 'GEN_MIN_WRITE_WORKERS',
        'GEN_POOL_SIZE', 'GEN_MAX_POOL_SIZE', 'GEN_MIN_POOL_SIZE', 'GEN_MAX_IDLE_TIME_MS',
        'GEN_WARMUP_CONNECTIONS', 'GEN_QUEUE_MULTIPLIER', 'GEN_WRITE_QUEUE_MULTIPLIER',
        'GEN_BATCH_AGGREGATION_SIZE', 'GEN_BATCH_AGGREGATION_TIMEOUT_MS', 'GEN_RU_PER_DOCUMENT',
        'GEN_RU_CHECK_INTERVAL_SECONDS', 'GEN_MONITORING_BATCH_INTERVAL', 'GEN_PROGRESS_UPDATE_INTERVAL_BATCHES',
        'GEN_RU_THROTTLE_THRESHOLD', 'GEN_RU_THROTTLE_PERCENTAGE', 'GEN_MAX_RETRIES',
        'GEN_PERFORMANCE_WINDOW_SIZE', 'GEN_PERFORMANCE_CHECK_INTERVAL_SECONDS',
        'GEN_THROTTLING_THRESHOLD_PERCENTAGE', 'GEN_SCALING_COOLDOWN_SECONDS',
        'GEN_SCALE_UP_COOLDOWN_SECONDS', 'GEN_PROGRESS_MINITERS', 'GEN_DOCUMENT_SIZE_MULTIPLIER'
    ]
    
    for var in numeric_vars:
        value = os.getenv(var)
        if value and not value.replace('.', '').replace('-', '').isdigit():
            logger.error(f"Invalid numeric value for {var}: {value}")
            sys.exit(1)
    
    logger.info("✅ All environment variables validated successfully")
    
    # Run the generator
    asyncio.run(main())
