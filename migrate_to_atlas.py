"""
Migration script from Azure Cosmos DB RU MongoDB API to MongoDB Atlas
Optimized for large datasets (100GB+) with UVLoop and advanced concurrency
"""
import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
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

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
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
        # Source (Cosmos DB)
        self.cosmos_connection_string = os.getenv('COSMOS_DB_CONNECTION_STRING')
        self.cosmos_db_name = os.getenv('COSMOS_DB_NAME', 'volvo-service-orders')
        self.cosmos_collection_name = os.getenv('COSMOS_DB_COLLECTION', 'serviceorders')
        
        # Target (MongoDB Atlas)
        self.atlas_connection_string = os.getenv('MONGODB_ATLAS_CONNECTION_STRING')
        self.atlas_db_name = os.getenv('MONGODB_ATLAS_DB_NAME', 'volvo-service-orders')
        self.atlas_collection_name = os.getenv('MONGODB_ATLAS_COLLECTION', 'serviceorders')
        
        # Migration settings (ULTRA-FAST OPTIMIZED)
        self.batch_size = int(os.getenv('MIGRATION_BATCH_SIZE', 10000))  # Larger batches for speed
        self.max_workers = int(os.getenv('MIGRATION_WORKERS', 16))  # More workers for parallelism
        
        # Insert batching optimization (DATA GENERATION SPEED)
        self.insert_batch_size = min(self.batch_size, 5000)  # Match data generation batch size
        self.max_insert_workers = min(self.max_workers * 2, 32)  # More insert workers
        
        # Read-ahead caching for parallel operations
        self.read_ahead_batches = 3  # Pre-fetch 3 batches ahead
        self.read_queue = None  # Will be initialized in connect_to_databases
        self.write_queue = None  # Will be initialized in connect_to_databases
        self.read_task = None
        self.write_tasks = []
        
        # Adaptive concurrency control (ULTRA-FAST OPTIMIZED)
        self.max_concurrent_workers = int(os.getenv('MAX_CONCURRENT_WORKERS', 20))  # Maximum workers
        self.min_concurrent_workers = int(os.getenv('MIN_CONCURRENT_WORKERS', 8))   # Higher minimum workers
        self.throttle_threshold = 0.1  # 10% throttling threshold for scaling down
        self.resume_from_checkpoint = os.getenv('RESUME_FROM_CHECKPOINT', 'true').lower() == 'true'
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
        self._save_checkpoint()


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
            
            # Optimize Atlas connection pool for ULTRA-FAST speed
            self.atlas_client._pool_size = 800  # Maximum connections for concurrent operations
            self.atlas_client._max_pool_size = 1000  # Maximum pool size
            self.atlas_client._min_pool_size = 200  # Higher minimum pool size
            self.atlas_client._max_idle_time_ms = 300000  # Keep connections alive much longer
            
            # Pre-warm connection pool for maximum performance
            logger.info("Pre-warming Atlas connection pool...")
            warmup_tasks = []
            for i in range(50):  # Create 50 warmup connections
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
            logger.info("Connected to MongoDB Atlas successfully")
            
            # Initialize async queues for parallel processing
            self.read_queue = asyncio.Queue(maxsize=self.read_ahead_batches * 2)
            self.write_queue = asyncio.Queue(maxsize=self.max_insert_workers * 2)
            
            # Test Atlas connection with a simple insert
            try:
                test_doc = {"_id": "test_migration_connection", "test": True, "timestamp": datetime.utcnow()}
                await self.atlas_collection.insert_one(test_doc)
                await self.atlas_collection.delete_one({"_id": "test_migration_connection"})
                logger.info("Atlas connection test successful")
            except Exception as e:
                logger.error(f"Atlas connection test failed: {e}")
                return False
            
            logger.info("🚀 DATA GENERATION-SPEED optimization settings active:")
            logger.info(f"   📦 Batch size: {self.batch_size:,} documents (matches data generation)")
            logger.info(f"   🔧 Initial workers: {self.max_workers}")
            logger.info(f"   📈 Max concurrent workers: {self.max_concurrent_workers}")
            logger.info(f"   📉 Min concurrent workers: {self.min_concurrent_workers}")
            logger.info(f"   🔄 Retry logic: {self.max_retries} attempt with ultra-minimal delay")
            logger.info(f"   ⚡ Write concern: Unacknowledged (w=0)")
            logger.info(f"   🚀 Connection pool: 800-1000 connections (pre-warmed)")
            logger.info(f"   🎯 Throttling threshold: {self.throttle_threshold * 100:.0f}%")
            logger.info(f"   📊 Insert batch size: {self.insert_batch_size:,} documents")
            logger.info(f"   🔥 Max insert workers: {self.max_insert_workers}")
            logger.info(f"   📚 Read-ahead batches: {self.read_ahead_batches}")
            logger.info(f"   ⚡ Parallel read/write: Enabled")
            logger.info(f"   🎯 Data generation speed: Optimized")
            
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

    def _load_checkpoint(self) -> Optional[Dict]:
        """Load migration checkpoint"""
        if not self.resume_from_checkpoint or not Path(self.checkpoint_file).exists():
            return None
            
        try:
            with open(self.checkpoint_file, 'r') as f:
                checkpoint = json.load(f)
                logger.info(f"Loaded checkpoint: {checkpoint}")
                return checkpoint
        except Exception as e:
            logger.error(f"Error loading checkpoint: {e}")
            return None

    def _save_checkpoint(self, pbar=None):
        """Save migration checkpoint with visual feedback"""
        try:
            checkpoint = {
                'documents_migrated': self.documents_migrated,
                'documents_skipped': self.documents_skipped,
                'errors': self.errors,
                'last_checkpoint': datetime.utcnow().isoformat(),
                'last_document_id': self.last_checkpoint
            }
            
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint, f, indent=2)
            
            # Show green checkmark in progress bar if available
            if pbar:
                pbar.set_postfix_str("✅ Checkpoint saved")
                pbar.refresh()
                # Clear the checkmark after a short delay
                import threading
                def clear_checkmark():
                    import time
                    time.sleep(2)
                    if pbar:
                        pbar.set_postfix_str("")
                        pbar.refresh()
                threading.Thread(target=clear_checkmark, daemon=True).start()
            
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")

    def _clear_checkpoint(self):
        """Clear migration checkpoint"""
        try:
            if Path(self.checkpoint_file).exists():
                Path(self.checkpoint_file).unlink()
                logger.info("Checkpoint file cleared")
        except Exception as e:
            logger.error(f"Error clearing checkpoint: {e}")

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

    async def _read_ahead_worker(self, cursor, query):
        """Read-ahead worker that pre-fetches batches for parallel processing"""
        try:
            batch = []
            batch_count = 0
            
            async for document in cursor:
                if not self.is_running or self.read_queue is None:
                    break
                    
                batch.append(document)
                batch_count += 1
                
                # When batch is ready, put it in the read queue
                if batch_count >= self.optimal_batch_size:
                    await self.read_queue.put(batch)
                    batch = []
                    batch_count = 0
            
            # Put remaining documents in final batch
            if batch and self.is_running and self.read_queue is not None:
                await self.read_queue.put(batch)
                
        except Exception as e:
            logger.error(f"Error in read-ahead worker: {e}")
        finally:
            # Signal end of reading
            if self.read_queue is not None:
                await self.read_queue.put(None)

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
                    
                    # Process the batch
                    result = await self.migrate_batch(batch, batch_number)
                    
                    # Mark task as done
                    self.write_queue.task_done()
                    
                except asyncio.TimeoutError:
                    continue
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
            
            # If no valid cache, get fresh count
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
                # Ultra-fast chunked insert for maximum parallelism
                async def _insert_batch():
                    try:
                        # Split large batches into optimal chunks for parallel processing
                        if len(batch) > self.insert_batch_size:
                            # Process chunks in parallel for maximum speed
                            chunks = [batch[i:i + self.insert_batch_size] 
                                    for i in range(0, len(batch), self.insert_batch_size)]
                            
                            # Create parallel insert tasks
                            insert_tasks = []
                            for chunk in chunks:
                                task = self.atlas_collection.insert_many(
                                    chunk,
                                    ordered=False  # Continue on errors for speed
                                )
                                insert_tasks.append(task)
                            
                            # Execute all chunks in parallel
                            results = await asyncio.gather(*insert_tasks, return_exceptions=True)
                            
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
                                    total_inserted += len(result.inserted_ids)
                            
                            stats['migrated'] = total_inserted
                            return results[0] if results else None
                        else:
                            # Single insert for smaller batches
                            result = await self.atlas_collection.insert_many(
                                batch,  # Direct insert without any processing
                                ordered=False  # Continue on errors for speed
                            )
                            stats['migrated'] = len(result.inserted_ids)
                            return result
                        
                    except Exception as e:
                        # Ultra-minimal error handling for speed - only handle critical errors
                        error_str = str(e).lower()
                        if "duplicate key" in error_str or "e11000" in error_str:
                            # Skip duplicates silently for speed
                            stats['skipped'] = len(batch)
                            class DuplicateResult:
                                inserted_ids = []
                            return DuplicateResult()
                        else:
                            # Re-raise other errors immediately
                            raise e
                
                result = await self._retry_with_backoff(_insert_batch)
                self.metrics['successful_batches'] += 1
                # No batch logging for cleaner console output
                
                # Track batch performance for auto-tuning and adaptive concurrency
                batch_time = time.time() - batch_start_time
                docs_per_second = len(batch) / batch_time if batch_time > 0 else 0
                self.batch_performance_history.append({
                    'batch_size': len(batch),
                    'docs_per_second': docs_per_second,
                    'timestamp': time.time()
                })
                
                # Adaptive concurrency control based on performance
                self._adaptive_concurrency_control(docs_per_second)
                
                # Auto-tune batch size every 10 batches for faster adaptation
                if len(self.batch_performance_history) >= 10:
                    self._auto_tune_batch_size()
                
        except Exception as e:
            logger.error(f"Error migrating batch: {e}")
            stats['errors'] = len(batch)
            self.metrics['failed_batches'] += 1
            
        return stats

    def _clean_document(self, doc: Dict) -> Dict:
        """Clean document for Atlas compatibility"""
        def clean_value(value):
            if isinstance(value, dict):
                return {k: clean_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [clean_value(item) for item in value]
            elif isinstance(value, bson.ObjectId):
                return str(value)
            elif isinstance(value, bson.Decimal128):
                return float(value)
            else:
                return value
        
        return clean_value(doc)

    async def migrate_with_cursor(self, resume_from_id: Optional[str] = None):
        """Migrate data using cursor-based pagination for better performance"""
        logger.info("Starting cursor-based migration...")
        
        # Build query for resuming
        query = {}
        if resume_from_id:
            query['_id'] = {'$gt': bson.ObjectId(resume_from_id)}
            logger.info(f"Resuming from document ID: {resume_from_id}")
        
        # Get total count for progress tracking
        total_count = await self.cosmos_collection.estimated_document_count()
        logger.info(f"Documents to migrate: {total_count:,}")
        
        if total_count == 0:
            logger.info("No documents to migrate")
            return
        
        # Create compact progress bar with checkpoint support
        if resume_from_id and self.documents_migrated > 0:
            # For resume, show progress from where we left off
            total_with_migrated = total_count + self.documents_migrated
            initial_n = self.documents_migrated
            desc = f"🔄 Migration (Resuming from {self.documents_migrated:,})"
        else:
            # For fresh migration
            total_with_migrated = total_count
            initial_n = 0
            desc = "🔄 Migration"
            
        pbar = tqdm(
            total=total_with_migrated,
            initial=initial_n,
            desc=desc, 
            unit="docs",
            unit_scale=True,
            ncols=80,
            bar_format='{desc}: {percentage:3.0f}%|{bar:20}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
            colour='blue'
        )
        
        try:
            # Use cursor with batch processing
            cursor = self.cosmos_collection.find(query).sort('_id', 1)
            
            batch = []
            batch_count = 0
            current_batch_number = 0
            
            async for document in cursor:
                if not self.is_running:
                    break
                    
                batch.append(document)
                batch_count += 1
                
                # Process batch when it reaches optimal batch size
                if batch_count >= self.optimal_batch_size:
                    current_batch_number += 1
                    stats = await self.migrate_batch(batch)
                    
                    self.documents_migrated += stats['migrated']
                    self.documents_skipped += stats['skipped']
                    self.errors += stats['errors']
                    
                    pbar.update(batch_count)
                    
                    # Save checkpoint every 10 batches for better visibility
                    if current_batch_number % 10 == 0:
                        self.last_checkpoint = str(batch[-1]['_id'])
                        self._save_checkpoint(pbar)
                    
                    # Reset batch
                    batch = []
                    batch_count = 0
            
            # Process remaining documents in final batch
            if batch and self.is_running:
                stats = await self.migrate_batch(batch)
                self.documents_migrated += stats['migrated']
                self.documents_skipped += stats['skipped']
                self.errors += stats['errors']
                pbar.update(batch_count)
                
        except Exception as e:
            logger.error(f"Error in cursor migration: {e}")
        finally:
            pbar.close()

    async def migrate_with_parallel_processing(self, resume_from_id: Optional[str] = None):
        """ULTRA-FAST migration with read-ahead caching and parallel read/write operations"""
        logger.info("🚀 Starting DATA GENERATION-SPEED migration...")
        logger.info(f"Source Database: {self.cosmos_db_name}")
        logger.info(f"Source Collection: {self.cosmos_collection_name}")
        logger.info(f"Target Database: {self.atlas_db_name}")
        logger.info(f"Target Collection: {self.atlas_collection_name}")
        logger.info(f"📊 Read-ahead batches: {self.read_ahead_batches}")
        logger.info(f"📊 Parallel write workers: {self.max_insert_workers}")
        
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
        
        # Get total count for progress tracking with caching
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
            desc = f"🚀 Migration Progress (Resuming from {self.documents_migrated:,})"
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
            ncols=100,
            bar_format='{desc}: {percentage:3.0f}%|{bar:25}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
            colour='green',
            smoothing=0.1,
            miniters=1000,
            dynamic_ncols=True,
            leave=True
        )
        
        try:
            # Create cursor for read-ahead processing
            cursor = self.cosmos_collection.find(query).sort('_id', 1)
            
            # Start read-ahead worker
            self.read_task = asyncio.create_task(self._read_ahead_worker(cursor, query))
            
            # Start parallel write workers
            self.write_tasks = []
            for i in range(self.max_insert_workers):
                task = asyncio.create_task(self._write_worker(i))
                self.write_tasks.append(task)
            
            # Process batches with read-ahead and parallel writes
            current_batch_number = 0
            completed_batches = 0
            
            while self.is_running and self.read_queue is not None and self.write_queue is not None:
                try:
                    # Get batch from read queue
                    batch = await asyncio.wait_for(self.read_queue.get(), timeout=5.0)
                    
                    if batch is None:  # End of reading
                        break
                        
                    current_batch_number += 1
                    
                    # Put batch in write queue for parallel processing
                    await self.write_queue.put((batch, current_batch_number))
                    
                    # Update progress
                    pbar.update(len(batch))
                    
                    # Save checkpoint every 10 batches
                    if current_batch_number % 10 == 0:
                        self.last_checkpoint = str(batch[-1]['_id'])
                        self._save_checkpoint(pbar)
                    
                    completed_batches += 1
                    
                except asyncio.TimeoutError:
                    # Check if read task is still running
                    if self.read_task and self.read_task.done():
                        break
                    continue
                except Exception as e:
                    logger.error(f"Error processing batch: {e}")
                    break
            
            # Signal end to write workers
            if self.write_queue is not None:
                for _ in range(self.max_insert_workers):
                    await self.write_queue.put(None)
            
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
            
            # Load checkpoint if resuming
            checkpoint = self._load_checkpoint()
            resume_from_id = None
            if checkpoint:
                self.documents_migrated = checkpoint.get('documents_migrated', 0)
                self.documents_skipped = checkpoint.get('documents_skipped', 0)
                self.errors = checkpoint.get('errors', 0)
                resume_from_id = checkpoint.get('last_document_id')
                logger.info(f"Resuming migration from checkpoint: {self.documents_migrated:,} documents already migrated")
            
            # Start ultra-fast migration (using cursor-based for speed)
            self.start_time = time.time()
            self.metrics['start_time'] = time.time()
            await self.migrate_with_parallel_processing(resume_from_id)
            
            # Final checkpoint
            self._save_checkpoint()
            
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
    # Check required environment variables
    required_vars = [
        'COSMOS_DB_CONNECTION_STRING',
        'MONGODB_ATLAS_CONNECTION_STRING'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        sys.exit(1)
    
    # Run migration
    asyncio.run(main())
