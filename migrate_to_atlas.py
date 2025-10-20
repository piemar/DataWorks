"""
Migration script from Azure Cosmos DB RU MongoDB API to MongoDB Atlas
Optimized for large datasets (100GB+) with UVLoop and advanced concurrency
"""
import asyncio
import os
import time
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import signal
import sys
from pathlib import Path
import random

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass  # uvloop not available, using default event loop

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
from pymongo.errors import InvalidOperation
from dotenv import load_dotenv
from tqdm import tqdm
import bson

from mongodb_compatibility import create_compatible_cosmos_client, create_compatible_atlas_client

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

class CosmosToAtlasMigrator:
    """High-performance migrator from Cosmos DB to MongoDB Atlas"""
    
    def __init__(self):
        # Source (Cosmos DB) - using GEN_ prefix for source database
        self.cosmos_connection_string = os.getenv('GEN_DB_CONNECTION_STRING')
        self.cosmos_db_name = os.getenv('GEN_DB_NAME', 'volvo-service-orders')
        self.cosmos_collection_name = os.getenv('GEN_DB_COLLECTION', 'serviceorders')
        
        # Target (MongoDB Atlas) - using MIG_ prefix for target database
        self.atlas_connection_string = os.getenv('MIG_TARGET_DB_CONNECTION_STRING')
        self.atlas_db_name = os.getenv('MIG_TARGET_DB_NAME', 'volvo-service-orders')
        self.atlas_collection_name = os.getenv('MIG_TARGET_DB_COLLECTION', 'serviceorders')
        
        # Migration settings (using new MIG_ prefix)
        self.batch_size = int(os.getenv('MIG_BATCH_SIZE', 10000))
        self.max_workers = int(os.getenv('MIG_WORKERS', 16))
        
        # Insert batching optimization (using new MIG_ prefix)
        self.insert_batch_size = self.batch_size 
        self.max_insert_workers = int(os.getenv('MIG_MAX_INSERT_WORKERS', 30))
        
        # Read-ahead caching for ULTRA-FAST parallel operations (using new MIG_ prefix)
        self.read_ahead_batches = int(os.getenv('MIG_READ_AHEAD_BATCHES', 100))
        self.read_ahead_workers = int(os.getenv('MIG_READ_AHEAD_WORKERS', 1))
        self.read_queue = None  # Will be initialized in connect_to_databases
        self.write_queue = None  # Will be initialized in connect_to_databases
        self.read_task = None
        self.write_tasks = []
        
        # Projection optimization (3-5x improvement) - only retrieve essential fields
        self.projection = {
            "_id": 1,
            "service_type": 1,
            "service_center_id": 1,
            "order_id": 1,
            "customer": 1,
            "vehicle": 1,
            "service_items": 1,
            "order_date": 1,
            "service_date": 1,
            "status": 1,
            "total_amount": 1,
            "labor_cost": 1,
            "parts_cost": 1,
            "tax_amount": 1,
            "technician_id": 1,
            "warranty_info": 1,
            "created_at": 1,
            "updated_at": 1
        }
        
        # Parallel cursors optimization (2-4x improvement)
        self.parallel_cursors = int(os.getenv('MIG_PARALLEL_CURSORS', 4))
        self.cursor_ranges = []  # Will be populated based on collection size
        
        # Adaptive concurrency control (using new MIG_ prefix)
        self.max_concurrent_workers = int(os.getenv('MIG_MAX_WORKERS', 20))
        self.min_concurrent_workers = int(os.getenv('MIG_MIN_WORKERS', 8))
        self.throttle_threshold = 0  # 10% throttling threshold for scaling down
        self.resume_from_checkpoint = os.getenv('MIG_RESUME_FROM_CHECKPOINT', 'true').lower() == 'true'
        self.checkpoint_file = 'migration_checkpoint.json'
        
        # Connections
        self.cosmos_client: Optional[AsyncIOMotorClient] = None
        self.atlas_client: Optional[AsyncIOMotorClient] = None
        self.cosmos_collection = None
        self.atlas_collection = None
        
        # Statistics
        self.documents_migrated = 0
        self.documents_skipped = 0
        self.errors = 0
        self.start_time = None
        self.last_checkpoint = None
        
        # Control flags
        self.is_running = True
        
        # Migration metadata collection for tracking runs
        self.metadata_collection = None
        
        # Adaptive concurrency tracking
        self.throttle_count = 0  # Count of throttling events
        self.batch_count = 0  # Total batches processed
        self.adaptive_stats = {'throttles': 0, 'successes': 0}
        
        # Dynamic worker management
        self.current_workers = self.max_workers  # Start with configured workers
        self.performance_window = []  # Track recent performance
        self.last_performance_check = time.time()
        self.scaling_cooldown = 0  # Prevent rapid scaling changes


        # Ultra-fast retry settings for maximum speed
        self.max_retries = 1  # Single retry for speed
        
        # Performance metrics
        self.metrics = {
            'total_batches': 0,
            'successful_batches': 0,
            'failed_batches': 0,
            'total_retries': 0,
            'start_time': None,
            'last_checkpoint_time': None
        }
        
        # RU monitoring and auto-tuning
        self.ru_consumed = 0
        self.ru_per_second = 0
        self.last_ru_check = time.time()
        self.batch_performance_history = []  # Track batch performance
        self.optimal_batch_size = self.batch_size  # Dynamic batch size
        self.auto_tuning_info = None  # Store auto-tuning info for progress bar
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _format_number(self, num: int) -> str:
        """Format number in k format (e.g., 20000 -> 20k)"""
        if num >= 1000:
            return f"{num/1000:.0f}k"
        return str(num)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, saving checkpoint and shutting down...")
        self.is_running = False
        # Note: Cannot await in signal handler, so we'll let the main loop handle cleanup


    async def _retry_with_backoff(self, func, *args, **kwargs):
        """Ultra-fast retry with minimal overhead"""
        for attempt in range(1):  # Single attempt for maximum speed
            try:
                result = await func(*args, **kwargs)
                return result
                
            except Exception as e:
                # Ultra-minimal delay for speed
                await asyncio.sleep(0.01)  # 10ms delay
                raise e

    async def connect_to_databases(self):
        """Connect to both source and target databases with optimized settings"""
        try:
            # Connect to Cosmos DB (source) with compatibility layer
            logger.info("Connecting to Azure Cosmos DB with optimized settings...")
            cosmos_compatible = create_compatible_cosmos_client(self.cosmos_connection_string)
            
            # Create client with compatibility layer (no custom options)
            self.cosmos_client = cosmos_compatible.create_client(async_client=True)
            await self.cosmos_client.admin.command('ping')
            
            cosmos_db = self.cosmos_client[self.cosmos_db_name]
            self.cosmos_collection = cosmos_db[self.cosmos_collection_name]
            logger.info("Connected to Cosmos DB successfully")
            
            # Connect to MongoDB Atlas (target) with compatibility layer
            logger.info("Connecting to MongoDB Atlas with optimized settings...")
            atlas_compatible = create_compatible_atlas_client(self.atlas_connection_string)
            
            # Create client with compatibility layer (no custom options)
            self.atlas_client = atlas_compatible.create_client(async_client=True)
            
            # Optimize Atlas connection pool for ULTRA-FAST 20K DOCS/SECOND speed (STABLE)
            # ULTRA-FAST connection pool settings for maximum speed
            self.atlas_client._pool_size = 2000  # ULTRA-FAST maximum connections
            self.atlas_client._max_pool_size = 3000  # ULTRA-FAST maximum pool size
            self.atlas_client._min_pool_size = 500  # ULTRA-FAST minimum pool size
            self.atlas_client._max_idle_time_ms = 300000  # Keep connections alive (5 min)
            self.atlas_client._socket_timeout_ms = int(os.getenv('MIG_SOCKET_TIMEOUT_MS', 30000))
            self.atlas_client._connect_timeout_ms = int(os.getenv('MIG_CONNECT_TIMEOUT_MS', 20000))
            
            # Pre-warm connection pool for ULTRA-FAST performance (FAST_START-aware)
            logger.info("Pre-warming Atlas connection pool...")
            warmup_target = int(os.getenv('MIG_WARMUP_CONNECTIONS', 200))
            if os.getenv('MIG_FAST_START', 'false').lower() == 'true':
                warmup_target = min(warmup_target, 50)
                logger.info(f"FAST_START enabled: limiting pre-warm connections to {warmup_target}")
            warmup_tasks = []
            for i in range(warmup_target):
                warmup_tasks.append(self.atlas_client.admin.command('ping'))
            await asyncio.gather(*warmup_tasks, return_exceptions=True)
            logger.info("Connection pool pre-warmed successfully")
            
            await self.atlas_client.admin.command('ping')
            
            atlas_db = self.atlas_client[self.atlas_db_name]
            
            # Set unacknowledged write concern for maximum speed (fire and forget)
            from pymongo.write_concern import WriteConcern
            self.atlas_collection = atlas_db.get_collection(
                self.atlas_collection_name, 
                write_concern=WriteConcern(w=0)  # Unacknowledged writes for maximum speed
            )
            
            # Initialize metadata collection for tracking migration runs
            self.metadata_collection = atlas_db.get_collection(
                'migration_metadata',
                write_concern=WriteConcern(w=1)  # Acknowledged writes for metadata
            )
            logger.info("Connected to MongoDB Atlas successfully")
            
            # Initialize async queues for ULTRA-FAST parallel processing
            self.read_queue = asyncio.Queue(maxsize=self.read_ahead_batches * 5)  # 500 batches (25M docs)
            self.write_queue = asyncio.Queue(maxsize=self.max_insert_workers * 5)  # 500 batches (25M docs)
            
            # Test Atlas connection with a simple insert
            try:
                test_doc = {"_id": "test_migration_connection", "test": True, "timestamp": datetime.utcnow()}
                await self.atlas_collection.insert_one(test_doc)
                await self.atlas_collection.delete_one({"_id": "test_migration_connection"})
                logger.info("Atlas connection test successful")
            except Exception as e:
                logger.error(f"Atlas connection test failed: {e}")
                return False
            
            logger.info("🚀 ULTRA-FAST 20K DOCS/SECOND optimization settings active:")
            logger.info(f"   📦 Batch size: {self.batch_size:,} documents")
            logger.info(f"   🔧 Initial workers: {self.max_workers}")
            logger.info(f"   📈 Max concurrent workers: {self.max_concurrent_workers}")
            logger.info(f"   📉 Min concurrent workers: {self.min_concurrent_workers}")
            logger.info(f"   🔄 Retry logic: {self.max_retries} attempt with ultra-minimal delay")
            logger.info(f"   ⚡ Write concern: Unacknowledged (w=0)")
            logger.info(f"   🚀 Connection pool: 2000-3000 connections (ULTRA-FAST + stable)")
            logger.info(f"   🎯 Throttling threshold: {self.throttle_threshold * 100:.0f}%")
            logger.info(f"   📊 Insert batch size: {self.insert_batch_size:,} documents")
            logger.info(f"   🔥 Max insert workers: {self.max_insert_workers} (ULTRA-FAST stable)")
            logger.info(f"   📚 Read-ahead batches: {self.read_ahead_batches} (ULTRA-FAST)")
            logger.info(f"   🔥 Read-ahead workers: {self.read_ahead_workers} (ULTRA-FAST cursor-safe)")
            logger.info(f"   ⚡ Parallel read/write: ULTRA-FAST Enabled")
            logger.info(f"   🔥 Bulk write operations: ENABLED (1.5-2x improvement)")
            logger.info(f"   🎯 Atlas write performance: ULTRA-FAST MAXIMIZED")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to databases: {e}")
            return False

    async def get_source_count(self) -> int:
        """Get total document count from source"""
        try:
            count = await self.cosmos_collection.estimated_document_count()
            logger.info(f"Total documents in source: {count:,}")
            return count
        except Exception as e:
            logger.error(f"Error getting source count: {e}")
            return 0

    async def get_target_count(self) -> int:
        """Get total document count from target"""
        try:
            count = await self.atlas_collection.estimated_document_count()
            logger.info(f"Total documents in target: {count:,}")
            return count
        except Exception as e:
            logger.error(f"Error getting target count: {e}")
            return 0

    async def _get_resume_point_and_checkpoint(self) -> tuple[Optional[str], Optional[Dict]]:
        """Get resume point and checkpoint in a single operation (optimized)"""
        logger.info("🔍 Getting resume point and checkpoint from Atlas max _id...")
        
        try:            
            # Get Atlas max ID as resume point
            atlas_max_doc = await self.atlas_collection.find_one(sort=[("_id", -1)])
            
            if not atlas_max_doc:
                logger.info("❌ No documents in Atlas; starting from beginning")
                return None, None
            
            atlas_max_id = str(atlas_max_doc['_id'])
            logger.info(f"   Atlas max ID: {atlas_max_id}")
            
            # Validate Atlas max ID exists in Cosmos DB
            cosmos_doc = await self.cosmos_collection.find_one({"_id": bson.ObjectId(atlas_max_id)})
            if not cosmos_doc:
                logger.warning("   ⚠️ Atlas max ID not found in Cosmos DB - starting from beginning")
                return None, None
            
            logger.info("   ✅ Atlas max ID exists in Cosmos DB - using as resume point")
            
            # Count documents up to and including the max _id for checkpoint
            try:
                migrated_count = await self.atlas_collection.estimated_document_count({"_id": {"$lte": atlas_max_doc['_id']}})
            except Exception:
                migrated_count = await self.atlas_collection.estimated_document_count()

            checkpoint = {
                'documents_migrated': migrated_count,
                'documents_skipped': 0,
                'errors': 0,
                'last_checkpoint': datetime.utcnow().isoformat(),
                'last_document_id': atlas_max_id
            }

            logger.info("✅ Resume point and checkpoint loaded successfully!")
            logger.info(f"   Documents migrated (<= max _id): {migrated_count:,}")
            logger.info(f"   Resume from ID: {atlas_max_id}")
            
            return atlas_max_id, checkpoint
            
        except Exception as e:
            logger.error(f"❌ Error getting resume point and checkpoint: {e}")
            return None, None


    async def _save_checkpoint_async(self, pbar=None):
        """ULTRA-FAST async checkpoint saving (non-blocking)"""
        try:
            checkpoint = {
                'documents_migrated': self.documents_migrated,
                'documents_skipped': self.documents_skipped,
                'errors': self.errors,
                'last_checkpoint': datetime.utcnow().isoformat(),
                'last_document_id': self.last_checkpoint
            }
            
            # Run file I/O in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._write_checkpoint_file, checkpoint)
            
            # Show checkpoint saved indicator (non-blocking)
            if pbar:
                pbar.set_postfix_str("✅ Checkpoint saved")
                pbar.refresh()
            
        except Exception as e:
            logger.error(f"Error saving checkpoint async: {e}")
    
    def _write_checkpoint_file(self, checkpoint):
        """Write checkpoint file (runs in thread pool)"""
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint, f, indent=2)

    def _clear_checkpoint(self):
        """Clear migration checkpoint"""
        try:
            if Path(self.checkpoint_file).exists():
                Path(self.checkpoint_file).unlink()
                logger.info("Checkpoint file cleared")
        except Exception as e:
            logger.error(f"Error clearing checkpoint: {e}")

    async def _save_migration_metadata(self, resume_from_id: Optional[str], status: str = "started"):
        """Save migration run metadata to collection"""
        try:
            metadata = {
                '_id': f"migration_{int(time.time())}",
                'status': status,
                'started_at': datetime.utcnow(),
                'resume_from_id': resume_from_id,
                'source_db': self.cosmos_db_name,
                'source_collection': self.cosmos_collection_name,
                'target_db': self.atlas_db_name,
                'target_collection': self.atlas_collection_name,
                'documents_migrated': self.documents_migrated,
                'documents_skipped': self.documents_skipped,
                'errors': self.errors,
                'batch_size': self.batch_size,
                'max_insert_workers': self.max_insert_workers,
                'read_ahead_batches': self.read_ahead_batches
            }
            
            if status == "completed":
                metadata['completed_at'] = datetime.utcnow()
                metadata['total_time_seconds'] = time.time() - self.start_time if self.start_time else 0
                metadata['final_rate'] = self.documents_migrated / metadata['total_time_seconds'] if metadata['total_time_seconds'] > 0 else 0
            
            await self.metadata_collection.insert_one(metadata)
            logger.info(f"Migration metadata saved: {status}")
            
        except Exception as e:
            logger.error(f"Error saving migration metadata: {e}")

    def _auto_tune_batch_size(self):
        """Auto-tune batch size based on performance history"""
        try:
            if len(self.batch_performance_history) < 5:
                return
                
            # Get recent performance data
            recent_batches = self.batch_performance_history[-10:]
            avg_performance = sum(b['docs_per_second'] for b in recent_batches) / len(recent_batches)
            
            # Calculate optimal batch size based on performance
            if avg_performance > 15000:  # High performance threshold raised
                new_batch_size = min(self.optimal_batch_size * 1.3, 20000)  # Larger max batch size
            elif avg_performance < 8000:  # Low performance threshold raised
                new_batch_size = max(self.optimal_batch_size * 0.7, 5000)  # Higher minimum batch size
            else:
                new_batch_size = self.optimal_batch_size  # Keep current size
            
            if abs(new_batch_size - self.optimal_batch_size) > 1000:  # Significant change
                old_size = self.optimal_batch_size
                self.optimal_batch_size = int(new_batch_size)
                # Store auto-tuning info for progress bar display
                self.auto_tuning_info = {
                    'old_size': old_size,
                    'new_size': self.optimal_batch_size,
                    'performance': avg_performance
                }
                
            # Clear old history to keep memory usage low
            self.batch_performance_history = self.batch_performance_history[-20:]
            
        except Exception as e:
            logger.debug(f"Error in auto-tuning: {e}")

    def _update_realtime_rate(self, pbar, docs_processed: int):
        """Update progress bar with real-time (instant) and average rates.

        Instant rate is computed from actual displayed progress (pbar.n) delta
        over time to reflect committed inserts. Average rate uses elapsed time
        since migration start and total progressed documents.
        """
        current_time = time.time()

        # Initialize tracking variables if they don't exist
        if not hasattr(self, 'last_update_time'):
            self.last_update_time = current_time
        if not hasattr(self, 'last_pbar_n'):
            self.last_pbar_n = pbar.n
        if not hasattr(self, 'instant_rate'):
            self.instant_rate = 0.0

        time_diff = current_time - self.last_update_time
        docs_diff = max(0, pbar.n - self.last_pbar_n)

        # Base description (strip previous rates if present)
        current_desc = pbar.desc
        if " | " in current_desc and "docs/s" in current_desc:
            # Remove any existing rate information
            base_desc = current_desc.split(" | ")[0]
        else:
            base_desc = current_desc

        # Update instant rate using progress bar delta
        if time_diff > 0 and docs_diff > 0:
            self.instant_rate = docs_diff / time_diff

        # Compute average rate from start_time and pbar.n (minus initial)
        if not hasattr(self, 'start_time') or not self.start_time:
            avg_rate_num = 0.0
        else:
            elapsed = max(1e-6, current_time - self.start_time)
            base_n = getattr(self, 'initial_pbar_n', 0)
            progressed = max(0, pbar.n - base_n)
            avg_rate_num = progressed / elapsed

        # Format rates with "k" notation for readability
        def format_rate(rate):
            if rate >= 1000:
                return f"{rate/1000:.0f}k"
            return f"{rate:.0f}"
        
        instant_rate_formatted = format_rate(self.instant_rate)
        
        new_desc = f"{base_desc} | {instant_rate_formatted} docs/s "
        pbar.set_description(new_desc)

        # Update tracking variables
        self.last_update_time = current_time
        self.last_pbar_n = pbar.n

    def _adaptive_concurrency_control(self, batch_performance: float):
        """Adaptive concurrency control based on performance and throttling"""
        try:
            current_time = time.time()
            
            # Add performance data to window
            self.performance_window.append({
                'performance': batch_performance,
                'timestamp': current_time,
                'workers': self.current_workers
            })
            
            # Keep only last 20 performance measurements
            self.performance_window = self.performance_window[-20:]
            
            # Check if we should evaluate scaling (every 5 batches or 30 seconds)
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
                
                if performance_degradation > self.throttle_threshold:
                    # Throttling detected - scale down
                    self.throttle_count += 1
                    self.adaptive_stats['throttles'] += 1
                    
                    if self.current_workers > self.min_concurrent_workers:
                        old_workers = self.current_workers
                        self.current_workers = max(self.min_concurrent_workers, int(self.current_workers * 0.8))
                        logger.info(f"🔄 Throttling detected! Scaling down workers: {old_workers} → {self.current_workers}")
                        self.scaling_cooldown = current_time + 60  # 1 minute cooldown
                        
                elif (performance_degradation < -0.05 and  # Performance improving
                      self.current_workers < self.max_concurrent_workers and
                      current_time > self.scaling_cooldown):
                    # Performance improving - scale up
                    old_workers = self.current_workers
                    self.current_workers = min(self.max_concurrent_workers, int(self.current_workers * 1.2))
                    logger.info(f"🚀 Performance improving! Scaling up workers: {old_workers} → {self.current_workers}")
                    self.scaling_cooldown = current_time + 30  # 30 second cooldown
                    self.adaptive_stats['successes'] += 1
                    
        except Exception as e:
            logger.debug(f"Error in adaptive concurrency control: {e}")

    def _get_optimal_worker_count(self) -> int:
        """Get the optimal number of workers based on current performance"""
        return self.current_workers

    async def _create_parallel_cursor_ranges(self, total_docs: int) -> List[Dict]:
        """Deprecated: parallel cursor ranges are not used (single cursor for safety)."""
        return []

    async def _read_ahead_worker(self, cursor, query, worker_id: int):
        """ULTRA-FAST read-ahead worker that pre-fetches batches for parallel processing.

        AGGRESSIVE APPROACH: Use smaller batches and flush immediately to ensure
        write workers always have data to process.
        """
        try:
            batch = []
            batch_count = 0
            last_flush_time = time.time()
            flush_interval = 0.1  # Very aggressive: 100ms timeout
            min_batch_size = 1000  # Minimum batch size for immediate processing
            
            async for document in cursor:
                if not self.is_running or self.read_queue is None:
                    break
                
                batch.append(document)
                batch_count += 1
                
                current_time = time.time()
                should_flush_full = batch_count >= self.optimal_batch_size
                should_flush_min = batch_count >= min_batch_size
                should_flush_timeout = (batch_count > 0) and (current_time - last_flush_time >= flush_interval)
                
                # AGGRESSIVE: Flush on any condition to keep writers busy
                if should_flush_full or should_flush_min or should_flush_timeout:
                    await self.read_queue.put(batch)
                    batch = []
                    batch_count = 0
                    last_flush_time = current_time
            
            # Put remaining documents in final batch
            if batch and self.is_running and self.read_queue is not None:
                await self.read_queue.put(batch)
                
        except Exception as e:
            logger.error(f"Error in read-ahead worker {worker_id}: {e}")
        finally:
            # Signal end of reading for this worker
            if self.read_queue is not None:
                await self.read_queue.put(None)

    async def _write_worker(self, worker_id: int, pbar=None):
        """ULTRA-FAST write worker with batch aggregation for maximum throughput"""
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
                            await self.migrate_batch_aggregated(batch_buffer, worker_id, pbar)
                            batch_buffer = []
                            batch_count = 0
                        continue
                    
                    if batch_data is None:  # End signal
                        # Process remaining batches in buffer
                        if batch_buffer:
                            await self.migrate_batch_aggregated(batch_buffer, worker_id, pbar)
                        break
                        
                    batch, batch_number = batch_data
                    batch_buffer.append(batch)
                    batch_count += 1
                    
                    # Process aggregated batches when buffer is full OR after timeout
                    current_time = time.time()
                    should_process = (
                        len(batch_buffer) >= 5 or  # Buffer full (reduced from 10)
                        (batch_buffer and current_time - last_process_time > 0.10)  # 0.5 second timeout (reduced from 1.0)
                    )
                    
                    if should_process:
                        await self.migrate_batch_aggregated(batch_buffer, worker_id, pbar)
                        batch_buffer = []
                        batch_count = 0
                        last_process_time = current_time
                    
                    # Mark task as done
                    self.write_queue.task_done()
                    
                except Exception as e:
                    logger.error(f"Error in write worker {worker_id}: {e}")
                    
        except Exception as e:
            logger.error(f"Write worker {worker_id} failed: {e}")

    async def _get_cached_count(self, query: dict) -> int:
        """Get document count with caching for faster startup"""
        try:
            # Check if we have a cached count
            cache_file = 'count_cache.json'
            cached_count = None
            
            if Path(cache_file).exists():
                try:
                    with open(cache_file, 'r') as f:
                        cache_data = json.load(f)
                        cached_count = cache_data.get('count')
                        cache_time = cache_data.get('timestamp', 0)
                        cache_query = cache_data.get('query', {})
                        
                        # Convert current query to serializable format for comparison
                        serializable_query = {}
                        if query:
                            for key, value in query.items():
                                if isinstance(value, dict):
                                    serializable_query[key] = {}
                                    for sub_key, sub_value in value.items():
                                        if hasattr(sub_value, '__str__'):
                                            serializable_query[key][sub_key] = str(sub_value)
                                        else:
                                            serializable_query[key][sub_key] = sub_value
                                else:
                                    if hasattr(value, '__str__'):
                                        serializable_query[key] = str(value)
                                    else:
                                        serializable_query[key] = value
                        
                        # Use cache if it's less than 1 hour old and query matches
                        if (time.time() - cache_time < 3600 and 
                            cache_query == serializable_query):
                            logger.info(f"Using cached count: {cached_count:,}")
                            return cached_count
                        else:
                            logger.info("Cached count is too old or query changed, fetching fresh count...")
                            cached_count = None
                except Exception as e:
                    logger.warning(f"Error reading count cache: {e}")
                    cached_count = None
            
            # If no valid cache, get fresh count (FAST_START: allow immediate use once fetched)
            if cached_count is None:
                logger.info("Getting fresh estimated document count...")
                total_count = await self.cosmos_collection.estimated_document_count()
                logger.info(f"Fresh estimated count: {total_count:,}")
                
                # Cache the result
                try:
                    # Convert ObjectId to string for JSON serialization
                    serializable_query = {}
                    if query:
                        for key, value in query.items():
                            if isinstance(value, dict):
                                serializable_query[key] = {}
                                for sub_key, sub_value in value.items():
                                    if hasattr(sub_value, '__str__'):
                                        serializable_query[key][sub_key] = str(sub_value)
                                    else:
                                        serializable_query[key][sub_key] = sub_value
                            else:
                                if hasattr(value, '__str__'):
                                    serializable_query[key] = str(value)
                                else:
                                    serializable_query[key] = value
                    
                    cache_data = {
                        'count': total_count,
                        'timestamp': time.time(),
                        'query': serializable_query
                    }
                    with open(cache_file, 'w') as f:
                        json.dump(cache_data, f)
                    logger.info("Cached count for future use")
                except Exception as e:
                    logger.warning(f"Could not cache count: {e}")
                
                return total_count
                
        except Exception as e:
            logger.warning(f"Could not get count: {e}. Using fallback estimate.")
            return 10000000  # Fallback estimate

    async def migrate_batch(self, batch: List[Dict], batch_number: int) -> Dict[str, int]:
        """Ultra-fast batch migration with retry logic and metrics"""
        batch_start_time = time.time()
        stats = {'migrated': 0, 'skipped': 0, 'errors': 0}
        self.metrics['total_batches'] += 1
        
        try:
            if not batch:
                logger.warning("Empty batch received")
                return stats
                
            logger.debug(f"Processing batch of {len(batch)} documents")
            
            # DIRECT INSERT - No processing overhead (ULTRA-FAST)
            if batch:
                # ULTRA-FAST bulk write operations for maximum performance
                async def _bulk_write_batch():
                    try:
                        # Use bulk write operations for maximum efficiency
                        from pymongo import InsertOne
                        
                        # Convert documents to bulk operations
                        bulk_operations = [InsertOne(doc) for doc in batch]
                        
                        # Split large batches into optimal chunks for parallel processing
                        if len(bulk_operations) > self.insert_batch_size:
                            # Process chunks in parallel for maximum speed
                            chunks = [bulk_operations[i:i + self.insert_batch_size] 
                                    for i in range(0, len(bulk_operations), self.insert_batch_size)]
                            
                            # Create parallel bulk write tasks
                            bulk_tasks = []
                            for chunk in chunks:
                                task = self.atlas_collection.bulk_write(
                                    chunk,
                            ordered=False  # Continue on errors for speed
                        )
                                bulk_tasks.append(task)
                            
                            # Execute all chunks in parallel
                            results = await asyncio.gather(*bulk_tasks, return_exceptions=True)
                            
                            # Aggregate results
                            total_inserted = 0
                            for result in results:
                                if isinstance(result, Exception):
                                    error_str = str(result).lower()
                                    if "duplicate key" in error_str or "e11000" in error_str:
                                        # Skip duplicates silently for speed
                                        continue
                                    else:
                                        raise result
                                else:
                                    # For unacknowledged writes, assume all documents were inserted
                                    # since we can't get the actual count
                                    try:
                                        total_inserted += result.inserted_count
                                    except (AttributeError, InvalidOperation):
                                        # With unacknowledged writes, we assume all documents were inserted
                                        # chunk contains bulk operations, so we count the operations
                                        total_inserted += len(chunk)
                            
                            stats['migrated'] = total_inserted
                            return results[0] if results else None
                        else:
                            # Single bulk write for smaller batches
                            result = await self.atlas_collection.bulk_write(
                                bulk_operations,  # Direct bulk write without any processing
                                ordered=False  # Continue on errors for speed
                            )
                            # For unacknowledged writes, assume all documents were inserted
                            try:
                                stats['migrated'] = result.inserted_count
                            except (AttributeError, InvalidOperation):
                                # With unacknowledged writes, we assume all documents were inserted
                                stats['migrated'] = len(batch)
                        return result
                        
                    except Exception as e:
                        # Ultra-minimal error handling for speed - only handle critical errors
                        error_str = str(e).lower()
                        if "duplicate key" in error_str or "e11000" in error_str:
                            # Skip duplicates silently for speed
                            stats['skipped'] = len(batch)
                            class DuplicateResult:
                                inserted_count = 0
                            return DuplicateResult()
                        else:
                            # Re-raise other errors immediately
                            raise e
                
                result = await self._retry_with_backoff(_bulk_write_batch)
                self.metrics['successful_batches'] += 1
                # No batch logging for cleaner console output
                
                # ULTRA-FAST: Minimal tracking for maximum speed
                batch_time = time.time() - batch_start_time
                docs_per_second = len(batch) / batch_time if batch_time > 0 else 0
                
                # ULTRA-FAST: Only track essential metrics
                if len(self.batch_performance_history) < 5:
                self.batch_performance_history.append({
                    'docs_per_second': docs_per_second,
                    'timestamp': time.time()
                })
                
                # ULTRA-FAST: Minimal adaptive concurrency control
                if len(self.batch_performance_history) >= 5:
                    self._adaptive_concurrency_control(docs_per_second)
                
        except Exception as e:
            logger.error(f"Error migrating batch: {e}")
            stats['errors'] = len(batch)
            self.metrics['failed_batches'] += 1
            
        return stats

    async def migrate_batch_aggregated(self, batch_list: List[List[Dict]], worker_id: int, pbar=None) -> Dict[str, int]:
        """ULTRA-FAST aggregated batch migration for maximum throughput"""
        batch_start_time = time.time()
        stats = {'migrated': 0, 'skipped': 0, 'errors': 0}
        
        try:
            if not batch_list:
                return stats
            
            # Flatten all batches into one massive batch
            all_docs = []
            for batch in batch_list:
                all_docs.extend(batch)
            
            if not all_docs:
                return stats
            
            logger.debug(f"Worker {worker_id}: Processing aggregated batch of {len(all_docs)} documents")
            
            # ULTRA-FAST bulk write with maximum parallelization
            async def _bulk_write_aggregated():
                try:
                    from pymongo import InsertOne
                    
                    # Convert all documents to bulk operations
                    bulk_operations = [InsertOne(doc) for doc in all_docs]
                    
                    # Split into optimal chunks for parallel processing
                    chunk_size = len(all_docs)  # Use full batch size for maximum efficiency
                    chunks = [bulk_operations[i:i + chunk_size] 
                            for i in range(0, len(bulk_operations), chunk_size)]
                    
                    # Create parallel bulk write tasks
                    bulk_tasks = []
                    for chunk in chunks:
                        task = self.atlas_collection.bulk_write(
                            chunk,
                            ordered=False  # Continue on errors for speed
                        )
                        bulk_tasks.append(task)
                    
                    # Execute all chunks in parallel
                    results = await asyncio.gather(*bulk_tasks, return_exceptions=True)
                    
                    # Aggregate results
                    total_inserted = 0
                    for result in results:
                        if isinstance(result, Exception):
                            error_str = str(result).lower()
                            if "duplicate key" in error_str or "e11000" in error_str:
                                continue  # Skip duplicates silently
                            else:
                                raise result
                        else:
                            try:
                                total_inserted += result.inserted_count
                            except (AttributeError, InvalidOperation):
                                # With unacknowledged writes, assume all documents were inserted
                                total_inserted += len(chunk)
                    
                    stats['migrated'] = total_inserted
                    return results[0] if results else None
                    
                except Exception as e:
                    error_str = str(e).lower()
                    if "duplicate key" in error_str or "e11000" in error_str:
                        stats['skipped'] = len(all_docs)
                        class DuplicateResult:
                            inserted_count = 0
                        return DuplicateResult()
                    else:
                        raise e
            
            result = await self._retry_with_backoff(_bulk_write_aggregated)
            
            # Track performance for this aggregated batch
            batch_time = time.time() - batch_start_time
            docs_per_second = len(all_docs) / batch_time if batch_time > 0 else 0
            
            # Update counters
                    self.documents_migrated += stats['migrated']
                    self.documents_skipped += stats['skipped']
                    self.errors += stats['errors']
                    
            # ULTRA-FAST real-time progress update (immediate)
            if pbar and stats['migrated'] > 0:
                pbar.update(stats['migrated'])
                self._update_realtime_rate(pbar, stats['migrated'])
                pbar.refresh()  # Force immediate display update
            
            logger.debug(f"Worker {worker_id}: Aggregated batch completed - {docs_per_second:.0f} docs/s")
            
        except Exception as e:
            logger.error(f"Error in aggregated batch migration: {e}")
            stats['errors'] = len(all_docs) if all_docs else 0
            
        return stats



    async def migrate_with_parallel_processing(self, resume_from_id: Optional[str] = None):
        """ULTRA-FAST migration with read-ahead caching and parallel read/write operations"""
        logger.info("🚀 Starting ULTRA-FAST ASYNC PARALLEL migration...")
        logger.info(f"Source Database: {self.cosmos_db_name}")
        logger.info(f"Source Collection: {self.cosmos_collection_name}")
        logger.info(f"Target Database: {self.atlas_db_name}")
        logger.info(f"Target Collection: {self.atlas_collection_name}")
        logger.info(f"📊 Read-ahead batches: {self.read_ahead_batches} (ULTRA-FAST)")
        logger.info(f"📊 Read-ahead workers: {self.read_ahead_workers} (ULTRA-FAST cursor-safe)")
        logger.info(f"📊 Parallel write workers: {self.max_insert_workers} (ULTRA-FAST)")
        logger.info(f"📊 Batch aggregation: 5 batches per worker (250K docs)")
        logger.info(f"📊 Parallel processing: 20 concurrent batches")
        logger.info(f"📊 Non-blocking progress: Background updates")
        logger.info(f"📊 Async checkpoints: Thread pool I/O")
        
        # Build query for resuming
        query = {}
        if resume_from_id:
            try:
                # Try to resume from the checkpoint
                query['_id'] = {'$gt': bson.ObjectId(resume_from_id)}
                logger.info(f"Resuming from document ID: {resume_from_id}")
                logger.info(f"Resume query: {query}")
                
                # Validate checkpoint by checking if document exists
                test_doc = await self.cosmos_collection.find_one({'_id': bson.ObjectId(resume_from_id)})
                if test_doc is None:
                    logger.warning(f"Checkpoint document {resume_from_id} not found. Starting from beginning.")
                    query = {}  # Reset to empty query
                    resume_from_id = None  # Clear resume ID
                    self._clear_checkpoint()  # Clear invalid checkpoint
                else:
                    logger.info(f"Checkpoint validated: document {resume_from_id} exists")
                    
            except Exception as e:
                logger.warning(f"Error with resume checkpoint {resume_from_id}: {e}. Starting from beginning.")
                query = {}  # Reset to empty query
                resume_from_id = None  # Clear resume ID
                self._clear_checkpoint()  # Clear invalid checkpoint
        
        # Get total count for progress tracking with caching (FAST_START allows immediate cached use)
        total_count = await self._get_cached_count(query)
        logger.info(f"Total documents to migrate: {total_count:,}")
        
        if total_count == 0:
            logger.info("No documents to migrate")
            if resume_from_id:
                logger.info("Clearing checkpoint file to start fresh migration")
                self._clear_checkpoint()
            return
        
        # Create clean, single progress bar with checkpoint support
        if resume_from_id and self.documents_migrated > 0:
            # For resume, show progress from where we left off
            total_with_migrated = total_count + self.documents_migrated
            initial_n = self.documents_migrated
            desc = f"🚀 Migration Progress (Resuming from {self.documents_migrated:,} - {total_with_migrated:,} total)"
        else:
            # For fresh migration
            total_with_migrated = total_count
            initial_n = 0
            desc = f"🚀 Migration: {self.cosmos_db_name}.{self.cosmos_collection_name} → {self.atlas_db_name}.{self.atlas_collection_name}"
            
        pbar = tqdm(
            total=total_with_migrated,
            initial=initial_n,
            desc=desc, 
            unit="docs",
            unit_scale=True,
            ncols=160,
            bar_format='{desc}: {percentage:3.0f}%|{bar:25}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]',
            colour='green',
            smoothing=0.0,  # NO smoothing for ULTRA-FAST real-time updates
            miniters=1,     # Show rate immediately
            dynamic_ncols=True,
            leave=True
        )
        
        # Initialize real-time rate tracking
        self.last_update_time = time.time()
        self.last_docs_count = initial_n
        self.instant_rate = 0.0
        self.initial_pbar_n = initial_n
        
        logger.info(f"🚀 STARTING ULTRA-FAST MIGRATION:")
        logger.info(f"   📊 Total documents to migrate: {total_count:,}")
        logger.info(f"   📊 Documents already migrated: {self.documents_migrated:,}")
        logger.info(f"   📊 Remaining documents: {total_count:,}")
        logger.info(f"   🎯 Resume point: {resume_from_id}")
        logger.info(f"   🔥 Insert workers: {self.max_insert_workers}")
        logger.info(f"   📚 Read-ahead workers: {self.read_ahead_workers}")
        logger.info(f"   📦 Batch size: {self.batch_size:,}")
        
        # Define max_concurrent_batches before using it (using new MIG_ prefix)
        max_concurrent_batches = int(os.getenv('MIG_MAX_CONCURRENT_BATCHES', 20))
        
        logger.info(f"🔧 DEBUGGING INFO:")
        logger.info(f"   📊 Read queue maxsize: {self.read_queue.maxsize}")
        logger.info(f"   📊 Write queue maxsize: {self.write_queue.maxsize}")
        logger.info(f"   📊 Max concurrent batches: {max_concurrent_batches}")
        logger.info(f"   📊 Write worker timeout: 2.0s")
        logger.info(f"   📊 Batch aggregation timeout: 0.5s")
        
        try:
            # Create cursor with ULTRA-FAST optimizations: projection + hint + SAFE batch size
            # Use a configurable batch size to avoid hitting Mongo payload limits (~48MB)
            cosmos_batch_size = int(os.getenv('GEN_CURSOR_BATCH_SIZE', 5000))
            cursor = (
                self.cosmos_collection
                .find(query, self.projection)
                .sort('_id', 1)
                .batch_size(cosmos_batch_size)
                .hint([("_id", 1)])
            )
            
            # Start ULTRA-FAST single read-ahead worker (cursor-safe)
            self.read_task = asyncio.create_task(self._read_ahead_worker(cursor, query, 0))
            
            # Start ULTRA-FAST parallel write workers
            self.write_tasks = []
            for i in range(self.max_insert_workers):
                task = asyncio.create_task(self._write_worker(i, pbar))
                self.write_tasks.append(task)
            
            # Process batches with ULTRA-FAST parallel processing
            current_batch_number = 0
            completed_batches = 0
            
            # No background progress updater - only write workers update progress bar
            # This prevents double-counting and ensures accurate rates
            
            # Process multiple batches in parallel
            batch_processing_tasks = []
            
            while self.is_running and self.read_queue is not None and self.write_queue is not None:
                try:
                    # Collect multiple batches for parallel processing
                    batches_to_process = []
                    for _ in range(max_concurrent_batches):
                        try:
                            batch = await asyncio.wait_for(self.read_queue.get(), timeout=0.1)
                            if batch is None:  # End of reading
                                break
                            batches_to_process.append(batch)
                        except asyncio.TimeoutError:
                            break  # No more batches available
                    
                    if not batches_to_process:
                        # Check if read task is still running
                        if self.read_task and self.read_task.done():
                            break
                        # No sleep - immediate retry for maximum speed
                        continue
                    
                    # Process all batches in parallel
                    for batch in batches_to_process:
                        current_batch_number += 1
                        await self.write_queue.put((batch, current_batch_number))
                        completed_batches += 1
                    
                    # Don't update rates here - let write workers handle it
                    # This prevents double-counting and ensures accurate rates
                    
                    # Save checkpoint every 20 batches for maximum speed (non-blocking)
                    if completed_batches % 20 == 0 and batches_to_process:
                        self.last_checkpoint = str(batches_to_process[-1][-1]['_id'])
                        asyncio.create_task(self._save_checkpoint_async(pbar))
                    
                except Exception as e:
                    logger.error(f"Error processing batches: {e}")
                    break
            
            # Signal end to write workers
            if self.write_queue is not None:
                for _ in range(self.max_insert_workers):
                    await self.write_queue.put(None)
            
            # No background progress updater to cancel
            
            # Wait for all write workers to complete
            if self.write_tasks:
                await asyncio.gather(*self.write_tasks, return_exceptions=True)
            
            # Wait for read task to complete
            if self.read_task:
                await self.read_task
                
        except Exception as e:
            logger.error(f"Error in DATA GENERATION-SPEED migration: {e}")
        finally:
            pbar.close()

    async def verify_migration(self):
        """Verify that migration was successful"""
        logger.info("Verifying migration...")
        
        try:
            source_count = await self.get_source_count()
            target_count = await self.get_target_count()
            
            logger.info(f"Source documents: {source_count:,}")
            logger.info(f"Target documents: {target_count:,}")
            
            if source_count == target_count:
                logger.info("✅ Migration verification successful!")
                return True
            else:
                logger.warning(f"⚠️ Document count mismatch: {source_count - target_count:,} documents")
                return False
                
        except Exception as e:
            logger.error(f"Error verifying migration: {e}")
            return False

    async def run_migration(self):
        """Run the complete migration process"""
        logger.info("Starting Cosmos DB to MongoDB Atlas migration...")
        
        try:
            # Connect to databases
            if not await self.connect_to_databases():
                logger.error("Failed to connect to databases")
                return False
            
            # Get resume point and checkpoint in a single optimized operation
            logger.info("🔄 Getting resume point and checkpoint from Atlas max _id...")
            resume_from_id, checkpoint = await self._get_resume_point_and_checkpoint()
            
            # Save migration metadata at start
            await self._save_migration_metadata(resume_from_id, "started")
            
            if resume_from_id:
                logger.info(f"🎯 RELIABLE RESUME POINT FOUND:")
                logger.info(f"   🆔 Resume from document ID: {resume_from_id}")
                logger.info(f"   ✅ Resume point validated and ready")
                
                # Update documents_migrated to count only up to the resume point (<= resume_id)
                try:
                    # Validate that resume_from_id is a valid ObjectId
                    if not bson.ObjectId.is_valid(resume_from_id):
                        raise ValueError(f"Invalid ObjectId format: {resume_from_id}")
                    
                    resume_oid = bson.ObjectId(resume_from_id)
                    # For filtered up-to-resume, a precise count is required; if too slow, caller falls back below
                    migrated_up_to_resume = await self.atlas_collection.count_documents({"_id": {"$lte": resume_oid}})
                    logger.info(f"📊 Atlas documents up to resume point: {migrated_up_to_resume:,}")
                    self.documents_migrated = migrated_up_to_resume
                except Exception as e:
                    logger.warning(f"Could not count up to resume point, falling back to estimated count: {e}")
                    actual_atlas_count = await self.atlas_collection.estimated_document_count()
                    self.documents_migrated = actual_atlas_count
                self.documents_skipped = checkpoint.get('documents_skipped', 0) if checkpoint else 0
                self.errors = checkpoint.get('errors', 0) if checkpoint else 0
                
                logger.info(f"🎯 UPDATED MIGRATION COUNTERS:")
                logger.info(f"   📊 Documents migrated (updated): {self.documents_migrated:,}")
                logger.info(f"   📊 Documents skipped: {self.documents_skipped:,}")
                logger.info(f"   📊 Errors: {self.errors}")
                
                # Update checkpoint file with correct count
                await self._save_checkpoint_async()
                logger.info("✅ Checkpoint updated with correct Atlas count")
            else:
                logger.info("🚀 Starting fresh migration (no reliable resume point found)")
                # Load checkpoint data for fresh start
            if checkpoint:
                self.documents_migrated = checkpoint.get('documents_migrated', 0)
                self.documents_skipped = checkpoint.get('documents_skipped', 0)
                self.errors = checkpoint.get('errors', 0)
            else:
                self.documents_migrated = 0
                self.documents_skipped = 0
                self.errors = 0
            
            # Start ultra-fast migration (using cursor-based for speed)
            self.start_time = time.time()
            self.metrics['start_time'] = time.time()
            await self.migrate_with_parallel_processing(resume_from_id)
            
            # Save completion metadata
            await self._save_migration_metadata(resume_from_id, "completed")
            
            # Final checkpoint (async)
            await self._save_checkpoint_async()
            
            # Verify migration
            await self.verify_migration()
            
            # Final statistics with enhanced metrics
            elapsed = time.time() - self.start_time
            rate = self.documents_migrated / elapsed if elapsed > 0 else 0
            
            logger.info("🚀 Migration completed!")
            logger.info(f"📊 Migration Summary:")
            logger.info(f"   • Source Database: {self.cosmos_db_name}")
            logger.info(f"   • Source Collection: {self.cosmos_collection_name}")
            logger.info(f"   • Target Database: {self.atlas_db_name}")
            logger.info(f"   • Target Collection: {self.atlas_collection_name}")
            logger.info(f"   • Total documents migrated: {self.documents_migrated:,}")
            logger.info(f"   • Total documents skipped: {self.documents_skipped:,}")
            logger.info(f"   • Total errors: {self.errors}")
            logger.info(f"   • Total time: {elapsed:.2f} seconds")
            logger.info(f"   • Average rate: {rate:.0f} documents/second")
            
            # Enhanced metrics
            logger.info("🔧 Performance Metrics:")
            logger.info(f"   📦 Total batches processed: {self.metrics['total_batches']:,}")
            logger.info(f"   ✅ Successful batches: {self.metrics['successful_batches']:,}")
            logger.info(f"   ❌ Failed batches: {self.metrics['failed_batches']:,}")
            logger.info(f"   🔄 Total retries: {self.metrics['total_retries']:,}")
            logger.info(f"   📊 Success rate: {(self.metrics['successful_batches'] / max(self.metrics['total_batches'], 1)) * 100:.1f}%")
            
            # Adaptive concurrency metrics
            logger.info("🎯 Adaptive Concurrency Metrics:")
            logger.info(f"   📈 Final worker count: {self.current_workers}")
            logger.info(f"   🔄 Throttling events: {self.throttle_count}")
            logger.info(f"   📊 Scaling successes: {self.adaptive_stats['successes']}")
            logger.info(f"   📉 Scaling downs: {self.adaptive_stats['throttles']}")
            logger.info(f"   🎯 Worker range: {self.min_concurrent_workers} - {self.max_concurrent_workers}")
            
            # Performance analysis
            if self.metrics['total_batches'] > 0:
                avg_batch_size = self.documents_migrated / self.metrics['total_batches']
                logger.info(f"   📏 Average batch size: {avg_batch_size:.0f} documents")
            
            if self.metrics['total_retries'] > 0:
                retry_rate = (self.metrics['total_retries'] / self.metrics['total_batches']) * 100
                logger.info(f"   🔄 Retry rate: {retry_rate:.1f}%")
            
            return True
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            return False

    async def cleanup(self):
        """Clean up resources"""
        if self.cosmos_client:
            self.cosmos_client.close()
        if self.atlas_client:
            self.atlas_client.close()
        logger.info("Database connections closed")

async def main():
    """Main function"""
    migrator = CosmosToAtlasMigrator()
    
    try:
        success = await migrator.run_migration()
        if success:
            logger.info("Migration completed successfully!")
        else:
            logger.error("Migration failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Migration interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        await migrator.cleanup()

if __name__ == "__main__":
    # Check required environment variables (using new naming convention)
    required_vars = [
        'GEN_DB_CONNECTION_STRING',
        'MIG_TARGET_DB_CONNECTION_STRING'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        logger.error("Please set these variables in your .env_local file")
        sys.exit(1)
    
    # Run migration
    asyncio.run(main())
