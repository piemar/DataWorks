"""
Migration Framework
Enterprise-ready data migration with parallel processing, checkpointing, and monitoring
"""
import asyncio
import logging
import time
import sys
import bson
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from enum import Enum
from collections import deque
from datetime import datetime

from tqdm import tqdm
from pymongo.errors import InvalidOperation

from ..core.database import BaseDatabaseClient, DatabaseConfig, DatabaseType, create_database_client
from ..config.manager import FrameworkConfig, MigrationConfig
from ..monitoring.metrics import MetricsCollector, OperationType

logger = logging.getLogger(__name__)

@dataclass
class MigrationBatch:
    """A batch of documents to migrate"""
    documents: List[Dict[str, Any]]
    batch_number: int
    source_cursor_position: Optional[str] = None
    metadata: Dict[str, Any] = None

@dataclass
class Checkpoint:
    """Migration checkpoint"""
    documents_migrated: int
    documents_skipped: int
    errors: int
    last_checkpoint: str
    last_document_id: Optional[str] = None
    cursor_position: Optional[str] = None
    metadata: Dict[str, Any] = None

class BaseMigrationStrategy(ABC):
    """
    Abstract base class for migration strategies
    
    Provides common functionality for:
    - Document reading
    - Batch processing
    - Checkpointing
    - Error handling
    """
    
    def __init__(self, source_client: BaseDatabaseClient, target_client: BaseDatabaseClient):
        self.source_client = source_client
        self.target_client = target_client
        self.documents_read = 0
        self.documents_migrated = 0
        self.documents_skipped = 0
        self.errors = 0
        self.start_time = None
    
    @abstractmethod
    async def read_batch(self, batch_size: int, resume_from: Optional[str] = None) -> MigrationBatch:
        """Read a batch of documents - must be implemented by subclasses"""
        pass
    
    @abstractmethod
    async def get_resume_point(self) -> Optional[str]:
        """Get resume point for migration - must be implemented by subclasses"""
        pass
    
    def get_migration_stats(self) -> Dict[str, Any]:
        """Get migration statistics"""
        elapsed = time.time() - self.start_time if self.start_time else 0
        rate = self.documents_migrated / elapsed if elapsed > 0 else 0
        
        return {
            "documents_read": self.documents_read,
            "documents_migrated": self.documents_migrated,
            "documents_skipped": self.documents_skipped,
            "errors": self.errors,
            "elapsed_time": elapsed,
            "migration_rate": rate
        }

class MigrationEngine:
    """
    Enterprise migration engine with:
    - Multiple migration strategies
    - Parallel processing
    - Checkpointing
    - Real-time monitoring
    - Error recovery
    """
    
    def __init__(self, config: FrameworkConfig):
        self.config = config
        self.source_client: Optional[BaseDatabaseClient] = None
        self.target_client: Optional[BaseDatabaseClient] = None
        self.metrics_collector = MetricsCollector()
        self.migration_strategy: Optional[BaseMigrationStrategy] = None
        self.is_running = False
        self.write_queue = None
        self.write_tasks = []
        
        # Performance tracking
        self.last_update_time = 0
        self.last_docs_count = 0
        self.instant_rate = 0
    
    async def initialize(self) -> bool:
        """Initialize the migration engine"""
        try:
            # Create source database client
            source_config = DatabaseConfig(
                connection_string=self.config.source_database.connection_string,
                database_name=self.config.source_database.database_name,
                collection_name=self.config.source_database.collection_name,
                db_type=DatabaseType.COSMOS_DB,
                batch_size=self.config.source_database.batch_size,
                max_pool_size=self.config.source_database.max_pool_size,
                min_pool_size=self.config.source_database.min_pool_size,
                max_idle_time_ms=self.config.source_database.max_idle_time_ms,
                socket_timeout_ms=self.config.source_database.socket_timeout_ms,
                connect_timeout_ms=self.config.source_database.connect_timeout_ms,
                warmup_connections=self.config.source_database.warmup_connections
            )
            
            self.source_client = create_database_client(source_config)
            
            # Create target database client
            target_config = DatabaseConfig(
                connection_string=self.config.target_database.connection_string,
                database_name=self.config.target_database.database_name,
                collection_name=self.config.target_database.collection_name,
                db_type=DatabaseType.MONGODB_ATLAS,
                batch_size=self.config.target_database.batch_size,
                max_pool_size=self.config.target_database.max_pool_size,
                min_pool_size=self.config.target_database.min_pool_size,
                max_idle_time_ms=self.config.target_database.max_idle_time_ms,
                socket_timeout_ms=self.config.target_database.socket_timeout_ms,
                connect_timeout_ms=self.config.target_database.connect_timeout_ms,
                warmup_connections=self.config.target_database.warmup_connections
            )
            
            self.target_client = create_database_client(target_config)
            
            # Connect to databases
            if not await self.source_client.connect():
                return False
            
            if not await self.target_client.connect():
                return False
            
            # Initialize write queue
            self.write_queue = asyncio.Queue(
                maxsize=self.config.workers.write_workers * self.config.workers.write_queue_multiplier
            )
            
            logger.info("Migration engine initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize migration engine: {e}")
            return False
    
    def set_migration_strategy(self, strategy: BaseMigrationStrategy):
        """Set the migration strategy"""
        self.migration_strategy = strategy
        logger.info(f"Migration strategy set: {strategy.__class__.__name__}")
    
    async def migrate(self, 
                     progress_callback: Optional[Callable] = None,
                     checkpoint_callback: Optional[Callable] = None,
                     force_from_start: bool = False) -> Dict[str, Any]:
        """Execute migration using the configured strategy"""
        
        if not self.migration_strategy:
            raise ValueError("Migration strategy not set")
        
        logger.info("Starting migration...")
        
        # Detect and recover from corrupted checkpoints
        await self._detect_and_recover_corrupted_checkpoint()
        
        # Get resume point (skip if force_from_start)
        resume_from = None
        if not force_from_start:
            resume_from = await self.migration_strategy.get_resume_point(force_from_start=force_from_start)
            if resume_from:
                logger.info(f"✅ Resume point found: {resume_from}")
            else:
                logger.info("❌ No resume point found - will start from beginning")
        else:
            logger.info("🚀 Force from start enabled - starting migration from beginning")
        
        # Start timing
        self.migration_strategy.start_time = time.time()
        self.is_running = True
        
        # Start write workers
        await self._start_write_workers()
        
        # Get total document count for progress bar
        total_docs = await self.source_client.get_estimated_count()
        logger.info(f"📊 Total documents in source: {total_docs:,}")
        
        # Seed progress with already migrated docs in target (simple and fast)
        # Skip seeding if force_from_start is True
        if force_from_start:
            migrated_so_far = 0
            logger.info("🚀 Force from start: Progress bar starting from 0")
        else:
            try:
                # Simple approach: count documents in target database = already migrated
                migrated_so_far = await self.target_client.get_estimated_count()
                logger.info(f"📊 Progress bar starting from {migrated_so_far:,} documents (target database count)")
            except Exception as e:
                logger.error(f"❌ Error getting target count: {e}")
                migrated_so_far = 0
                logger.info("📊 Progress bar starting from 0 (could not get target count)")
        
        # Update migration strategy with the correct starting count
        self.migration_strategy.documents_migrated = migrated_so_far
        
        # Create progress bar with custom rate formatting
        progress_desc = "🚀 Migrating data (from start)" if force_from_start else "🚀 Migrating data"
        pbar = tqdm(
            total=total_docs,
            desc=progress_desc,
            unit="docs",
            unit_scale=True,
            ncols=120,
            bar_format='{desc}: {percentage:3.0f}%|{bar:25}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, Rate: {rate_fmt}]',
            colour='blue',
            smoothing=self.config.performance.progress_smoothing,
            miniters=self.config.performance.progress_miniters,
            dynamic_ncols=True,
            leave=True,
            initial=migrated_so_far,
            position=0,  # Ensure progress bar is at top
            file=sys.stdout
        )
        
        # Log the progress bar initialization
        logger.info(f"📊 Progress bar initialized: {migrated_so_far:,}/{total_docs:,} documents ({migrated_so_far/total_docs*100:.1f}%)")
        logger.info(f"📊 Migration strategy documents_migrated set to: {self.migration_strategy.documents_migrated:,}")
        logger.info(f"📊 Progress bar current value: {pbar.n}")
        logger.info(f"📊 Progress bar total value: {pbar.total}")
        
        # Store progress bar reference for statistics updates
        self.progress_bar = pbar
        
        try:
            # Start write workers
            await self._start_write_workers()
            
            # ULTRA-FAST: Start aggressive read-ahead worker (like original migrate_to_atlas.py)
            await self._start_aggressive_read_worker(resume_from, pbar)
            
            # Wait for read workers to complete first
            if self.read_tasks:
                logger.info("🔄 Waiting for read workers to complete...")
                await asyncio.gather(*self.read_tasks, return_exceptions=True)
                logger.info("✅ Read workers completed")
            
            # Give write workers time to process remaining batches
            logger.info("🔄 Waiting for write workers to process final batches...")
            if hasattr(self, 'progress_bar') and self.progress_bar:
                self.progress_bar.set_description("🔄 Finalizing migration...")
            await asyncio.sleep(2)  # Give 2 seconds for final batch processing
            
            # Wait for write workers to complete with timeout
            if self.write_tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*self.write_tasks, return_exceptions=True),
                        timeout=30  # 30 second timeout for finalization
                    )
                    logger.info("✅ Write workers completed")
                except asyncio.TimeoutError:
                    logger.warning("⚠️ Write workers timeout - forcing completion")
                    # Force stop write workers
                    self.is_running = False
                    for task in self.write_tasks:
                        if not task.done():
                            task.cancel()
            
        except Exception as e:
            logger.error(f"Error during migration: {e}")
            raise
        finally:
            pbar.close()
            self.is_running = False
            self.progress_bar = None
        
        # Save migration metadata for reliable resume points
        await self._save_migration_metadata()
        
        # Return final statistics
        return self._get_final_stats()
    
    def _update_progress_statistics(self, current_rate: float, documents_migrated: int):
        """Update progress bar with enhanced statistics"""
        if not hasattr(self, 'progress_bar') or self.progress_bar is None:
            return
            
        # Update peak rate
        if current_rate > self.stats['peak_rate']:
            self.stats['peak_rate'] = current_rate
        
        # Calculate elapsed time
        elapsed_time = time.time() - self.stats['start_time']
        
        # Get memory usage (simplified)
        try:
            import psutil
            memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
            self.stats['memory_usage'] = memory_mb
        except ImportError:
            memory_mb = 0
        
        # Create enhanced statistics string
        stats_str = f"Peak: {self.stats['peak_rate']:.0f}docs/s | Mem: {memory_mb:.0f}MB | Errors: {self.stats['total_errors']}"
        
        # Update progress bar postfix
        self.progress_bar.set_postfix_str(stats_str)
    
    def _show_worker_scale_event(self, event_type: str, old_count: int, new_count: int):
        """Show worker scaling event"""
        logger.info(f"🔄 Worker scaling: {event_type} from {old_count} to {new_count} workers")
    
    def _show_checkpoint_complete(self, checkpoint_info: str):
        """Show checkpoint completion"""
        logger.info(f"✅ Checkpoint complete: {checkpoint_info}")
    
    async def _save_migration_metadata(self):
        """Save migration metadata for reliable resume points"""
        try:
            metadata_collection = self.target_client.database["migration_metadata"]
            
            # Get the last document ID from the migration
            last_document_id = None
            if hasattr(self.migration_strategy, 'last_document_id') and self.migration_strategy.last_document_id:
                last_document_id = self.migration_strategy.last_document_id
            
            # Create migration metadata document
            metadata_doc = {
                "collection": self.target_client.collection.name,
                "source_database": self.source_client.config.database_name,
                "target_database": self.target_client.config.database_name,
                "last_document_id": last_document_id,
                "documents_migrated": self.migration_strategy.documents_migrated,
                "documents_skipped": self.migration_strategy.documents_skipped,
                "errors": self.migration_strategy.errors,
                "started_at": getattr(self, '_migration_start_time', None),
                "completed_at": datetime.utcnow(),
                "migration_strategy": self.migration_strategy.__class__.__name__,
                "batch_size": self.config.source_database.batch_size,
                "workers": self.config.workers.write_workers,
                "status": "completed"
            }
            
            # Insert or update migration metadata
            await metadata_collection.replace_one(
                {
                    "collection": self.target_client.collection.name,
                    "completed_at": {"$exists": False}  # Find incomplete migrations
                },
                metadata_doc,
                upsert=True
            )
            
            logger.info(f"✅ Migration metadata saved: {last_document_id}")
            
        except Exception as e:
            logger.warning(f"Failed to save migration metadata: {e}")
    
    async def _start_read_workers(self, resume_from: Optional[str] = None):
        """Start parallel read-ahead workers"""
        self.read_tasks = []
        self.read_queue = asyncio.Queue(maxsize=self.config.performance.read_ahead_batches)
        
        for i in range(self.config.performance.read_ahead_workers):
            task = asyncio.create_task(self._read_ahead_worker(i, resume_from))
            self.read_tasks.append(task)
    
    async def _start_write_workers(self):
        """Start parallel write workers"""
        self.write_tasks = []
        self.write_queue = asyncio.Queue(maxsize=self.config.performance.max_concurrent_batches)
        
        for i in range(self.config.workers.write_workers):
            task = asyncio.create_task(self._write_worker(i))
            self.write_tasks.append(task)
    
    async def _start_aggressive_read_worker(self, resume_from: Optional[str], pbar):
        """ULTRA-FAST aggressive read-ahead worker (like original migrate_to_atlas.py)"""
        try:
            # Build query for resuming
            query = {}
            if resume_from:
                try:
                    import bson
                    if bson.ObjectId.is_valid(resume_from):
                        query["_id"] = {"$gte": bson.ObjectId(resume_from)}  # Use $gte instead of $gt
                except Exception as e:
                    logger.warning(f"Invalid resume point {resume_from}: {e}")
            
            # Create cursor with ULTRA-FAST optimizations
            cursor = (
                self.source_client.collection
                .find(query)
                .sort('_id', 1)
                .batch_size(self.config.source_database.batch_size)
                .hint([("_id", 1)])
            )
            
            logger.info(f"🚀 Starting aggressive read worker with query: {query}")
            logger.info(f"🚀 Resume from: {resume_from}")
            
            # Start aggressive read-ahead worker
            read_task = asyncio.create_task(self._aggressive_read_worker(cursor, query, 0, pbar, resume_from))
            self.read_tasks = [read_task]
            
        except Exception as e:
            logger.error(f"Error starting aggressive read worker: {e}")
            raise
    
    async def _aggressive_read_worker(self, cursor, query, worker_id: int, pbar, resume_from: Optional[str] = None):
        """ULTRA-FAST aggressive read-ahead worker with 100ms flush intervals"""
        try:
            batch = []
            batch_count = 0
            last_flush_time = time.time()
            flush_interval = 0.1  # ULTRA-FAST: 100ms timeout (like original)
            min_batch_size = 1000  # Minimum batch size for immediate processing
            skip_first = resume_from is not None  # Skip resume document if resuming
            
            documents_found = 0
            async for document in cursor:
                if not self.is_running or self.write_queue is None:
                    break
                
                documents_found += 1
                
                if skip_first:
                    # Skip the resume document itself
                    skip_first = False
                    logger.info(f"🚀 Skipped resume document: {document['_id']}")
                    continue
                
                batch.append(document)
                batch_count += 1
                
                # Update last_document_id in migration strategy for checkpoint saving
                if hasattr(self.migration_strategy, 'last_document_id'):
                    self.migration_strategy.last_document_id = str(document['_id'])
                    logger.debug(f"🔄 Updated last_document_id: {self.migration_strategy.last_document_id}")
                
                # Log first few documents for debugging
                if batch_count <= 3:
                    logger.debug(f"🚀 Processing document {batch_count}: {document['_id']}")
                
                current_time = time.time()
                should_flush_full = batch_count >= self.config.source_database.batch_size
                should_flush_min = batch_count >= min_batch_size
                should_flush_timeout = (batch_count > 0) and (current_time - last_flush_time >= flush_interval)
                
                # AGGRESSIVE: Flush on any condition to keep writers busy
                if should_flush_full or should_flush_min or should_flush_timeout:
                    # Create MigrationBatch for framework compatibility
                    migration_batch = MigrationBatch(
                        documents=batch,
                        batch_number=getattr(self, '_batch_number', 0) + 1,
                        source_cursor_position=str(batch[-1]['_id']) if batch else None,
                        metadata={
                            "worker_id": worker_id,
                            "batch_size": len(batch),
                            "read_at": time.time(),
                            "aggressive_flush": True
                        }
                    )
                    await self.write_queue.put((migration_batch, pbar))
                    batch = []
                    batch_count = 0
                    last_flush_time = current_time
            
            # Put remaining documents in final batch
            if batch and self.is_running and self.write_queue is not None:
                migration_batch = MigrationBatch(
                    documents=batch,
                    batch_number=getattr(self, '_batch_number', 0) + 1,
                    source_cursor_position=str(batch[-1]['_id']) if batch else None,
                    metadata={
                        "worker_id": worker_id,
                        "batch_size": len(batch),
                        "read_at": time.time(),
                        "final_batch": True
                    }
                )
                await self.write_queue.put((migration_batch, pbar))
                
        except Exception as e:
            logger.error(f"Error in aggressive read worker {worker_id}: {e}")
        finally:
            logger.info(f"🚀 Aggressive read worker {worker_id} completed. Documents found: {documents_found}, Documents processed: {batch_count}")
            # Signal end to write workers with proper logging
            if self.write_queue is not None:
                logger.info(f"📤 Signaling {self.config.workers.write_workers} write workers to complete...")
                for i in range(self.config.workers.write_workers):
                    await self.write_queue.put(None)
                logger.info("✅ End signals sent to all write workers")

    async def _write_worker(self, worker_id: int):
        """Parallel write worker with batch aggregation"""
        try:
            batch_buffer = []
            last_process_time = time.time()
            
            while self.is_running and self.write_queue is not None:
                try:
                    # Collect batches with timeout
                    try:
                        batch_data = await asyncio.wait_for(self.write_queue.get(), timeout=2.0)
                    except asyncio.TimeoutError:
                        # Process any buffered batches
                        if batch_buffer:
                            await self._write_batch_aggregated(batch_buffer, worker_id)
                            batch_buffer = []
                        continue
                    
                    if batch_data is None:  # End signal
                        logger.debug(f"📥 Write worker {worker_id} received end signal")
                        if batch_buffer:
                            logger.debug(f"📤 Write worker {worker_id} processing final batch of {len(batch_buffer)} batches")
                            await self._write_batch_aggregated(batch_buffer, worker_id, pbar)
                        logger.info(f"✅ Write worker {worker_id} completed")
                        break
                    
                    batch, pbar = batch_data
                    batch_buffer.append(batch)
                    
                    # Process aggregated batches
                    current_time = time.time()
                    should_process = (
                        len(batch_buffer) >= self.config.performance.batch_aggregation_size or
                        (batch_buffer and current_time - last_process_time > self.config.performance.batch_aggregation_timeout_ms / 1000.0)
                    )
                    
                    if should_process:
                        await self._write_batch_aggregated(batch_buffer, worker_id, pbar)
                        batch_buffer = []
                        last_process_time = current_time
                    
                    self.write_queue.task_done()
                    
                except Exception as e:
                    logger.error(f"Error in write worker {worker_id}: {e}")
                    
        except Exception as e:
            logger.error(f"Write worker {worker_id} failed: {e}")
    
    async def _write_batch_aggregated(self, batches: List[MigrationBatch], worker_id: int, pbar=None):
        """ULTRA-FAST aggregated batch migration with direct bulk operations (like original migrate_to_atlas.py)"""
        batch_start_time = time.time()
        stats = {'migrated': 0, 'skipped': 0, 'errors': 0}
        
        try:
            if not batches:
                return stats
            
            # Flatten all batches into one massive batch (like original)
            all_docs = []
            for batch in batches:
                all_docs.extend(batch.documents)
            
            if not all_docs:
                return stats
            
            logger.debug(f"Worker {worker_id}: Processing ULTRA-FAST aggregated batch of {len(all_docs)} documents")
            
            # Track metrics (keeping enterprise monitoring)
            operation = self.metrics_collector.start_operation(
                f"write_worker_{worker_id}",
                OperationType.WRITE,
                len(all_docs)
            )
            
            # ULTRA-FAST bulk write with maximum parallelization (like original)
            async def _bulk_write_aggregated():
                try:
                    from pymongo import InsertOne
                    
                    # Convert all documents to bulk operations
                    bulk_operations = [InsertOne(doc) for doc in all_docs]
                    
                    # ULTRA-FAST: Use full batch size for maximum efficiency (like original)
                    chunk_size = len(all_docs)  # Single massive operation
                    chunks = [bulk_operations[i:i + chunk_size] 
                            for i in range(0, len(bulk_operations), chunk_size)]
                    
                    # Create parallel bulk write tasks
                    bulk_tasks = []
                    for chunk in chunks:
                        task = self.target_client.collection.bulk_write(
                            chunk,
                            ordered=False  # Continue on errors for speed
                        )
                        bulk_tasks.append(task)
                    
                    # Execute all chunks in parallel
                    results = await asyncio.gather(*bulk_tasks, return_exceptions=True)
                    
                    # Aggregate results
                    total_inserted = 0
                    total_skipped = 0
                    for result in results:
                        if isinstance(result, Exception):
                            error_str = str(result).lower()
                            if "duplicate key" in error_str or "e11000" in error_str:
                                # Count duplicates as skipped
                                total_skipped += len(chunk) if 'chunk' in locals() else 0
                                logger.debug(f"Worker {worker_id}: Skipped {len(chunk) if 'chunk' in locals() else 0} duplicate documents")
                                continue  # Skip duplicates silently
                            else:
                                logger.error(f"Worker {worker_id}: Bulk write error: {result}")
                                raise result
                        else:
                            try:
                                total_inserted += result.inserted_count
                                # Count duplicates from bulk write result
                                if hasattr(result, 'upserted_count'):
                                    total_skipped += result.upserted_count
                            except (AttributeError, InvalidOperation):
                                # With unacknowledged writes, assume all documents were inserted
                                total_inserted += len(chunk) if 'chunk' in locals() else 0
                    
                    stats['migrated'] = total_inserted
                    stats['skipped'] = total_skipped
                    return results[0] if results else None
                    
                except Exception as e:
                    error_str = str(e).lower()
                    if "duplicate key" in error_str or "e11000" in error_str:
                        stats['skipped'] = len(all_docs)
                        logger.debug(f"Worker {worker_id}: Bulk write failed with duplicates - skipped {len(all_docs)} documents")
                        class DuplicateResult:
                            inserted_count = 0
                        return DuplicateResult()
                    else:
                        logger.error(f"Worker {worker_id}: Bulk write failed with error: {e}")
                        raise e
            
            try:
                result = await _bulk_write_aggregated()
            except Exception as e:
                # If bulk write fails completely, fall back to individual document insertion
                logger.warning(f"Worker {worker_id}: Bulk write failed, falling back to individual insertion: {e}")
                result = await self._insert_documents_individually(all_docs, worker_id, stats)
            
            # Update migration strategy stats
            self.migration_strategy.documents_migrated += stats['migrated']
            self.migration_strategy.documents_skipped += stats['skipped']
            self.migration_strategy.errors += stats['errors']
            
            # Save checkpoint after every batch (non-blocking)
            if stats['migrated'] > 0 or stats['skipped'] > 0:
                asyncio.create_task(self._save_checkpoint_async())
            
            # Update metrics (keeping enterprise monitoring)
            self.metrics_collector.end_operation(operation, stats['migrated'], True)
            
            # Track performance for this aggregated batch
            batch_time = time.time() - batch_start_time
            docs_per_second = len(all_docs) / batch_time if batch_time > 0 else 0
            
            # ULTRA-FAST real-time progress update (immediate)
            if pbar and stats['migrated'] > 0:
                # Update progress bar (migration strategy already updated above)
                pbar.update(stats['migrated'])
                self._update_realtime_rate(pbar, stats['migrated'])
                pbar.refresh()  # Force immediate display update
            
            logger.debug(f"Worker {worker_id}: ULTRA-FAST aggregated batch completed - {docs_per_second:.0f} docs/s")
            
        except Exception as e:
            logger.error(f"Worker {worker_id}: Failed to migrate batch: {e}")
            self.migration_strategy.errors += 1
            if pbar:
                pbar.update(0)  # Update progress even on failure
    
    async def _insert_documents_individually(self, documents: List[Dict], worker_id: int, stats: Dict):
        """Fallback method: Insert documents individually with duplicate handling"""
        logger.info(f"Worker {worker_id}: Inserting {len(documents)} documents individually...")
        
        for doc in documents:
            try:
                await self.target_client.collection.insert_one(doc)
                stats['migrated'] += 1
            except Exception as e:
                error_str = str(e).lower()
                if "duplicate key" in error_str or "e11000" in error_str:
                    stats['skipped'] += 1
                    logger.debug(f"Worker {worker_id}: Skipped duplicate document: {doc.get('_id', 'unknown')}")
                else:
                    stats['errors'] += 1
                    logger.error(f"Worker {worker_id}: Failed to insert document {doc.get('_id', 'unknown')}: {e}")
        
        logger.info(f"Worker {worker_id}: Individual insertion completed - {stats['migrated']} migrated, {stats['skipped']} skipped, {stats['errors']} errors")
        return None
    
    async def _save_checkpoint_async(self):
        """Save checkpoint asynchronously after each batch (non-blocking)"""
        try:
            metadata_collection = self.target_client.database["migration_metadata"]
            
            # Get the last document ID from the migration strategy
            last_document_id = None
            if hasattr(self.migration_strategy, 'last_document_id') and self.migration_strategy.last_document_id:
                last_document_id = self.migration_strategy.last_document_id
                logger.debug(f"💾 Saving checkpoint with last_document_id: {last_document_id}")
            else:
                logger.debug("💾 Saving checkpoint with no last_document_id (strategy doesn't have it)")
            
            # Create checkpoint document
            checkpoint_doc = {
                "collection": self.target_client.collection.name,
                "source_database": self.source_client.config.database_name,
                "target_database": self.target_client.config.database_name,
                "last_document_id": last_document_id,
                "documents_migrated": self.migration_strategy.documents_migrated,
                "documents_skipped": self.migration_strategy.documents_skipped,
                "errors": self.migration_strategy.errors,
                "checkpoint_at": datetime.utcnow(),
                "migration_strategy": self.migration_strategy.__class__.__name__,
                "batch_size": self.config.source_database.batch_size,
                "workers": self.config.workers.write_workers,
                "status": "in_progress"
            }
            
            # Upsert checkpoint (replace if exists, insert if not)
            await metadata_collection.replace_one(
                {
                    "collection": self.target_client.collection.name,
                    "status": "in_progress"
                },
                checkpoint_doc,
                upsert=True
            )
            
            logger.debug(f"✅ Checkpoint saved: {self.migration_strategy.documents_migrated:,} migrated, {self.migration_strategy.documents_skipped:,} skipped")
            
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")
    
    async def _detect_and_recover_corrupted_checkpoint(self):
        """Detect and recover from corrupted checkpoints"""
        try:
            metadata_collection = self.target_client.database["migration_metadata"]
            
            # Find all checkpoints for this collection
            checkpoints = await metadata_collection.find(
                {"collection": self.target_client.collection.name}
            ).sort([("checkpoint_at", -1), ("completed_at", -1)]).to_list(10)
            
            if not checkpoints:
                logger.info("No checkpoints found - starting fresh")
                return
            
            # Check for corrupted checkpoints
            corrupted_count = 0
            for checkpoint in checkpoints:
                if not await self._is_checkpoint_valid(checkpoint):
                    corrupted_count += 1
                    logger.warning(f"Found corrupted checkpoint: {checkpoint.get('_id')}")
            
            if corrupted_count > 0:
                logger.warning(f"Found {corrupted_count} corrupted checkpoints - cleaning up")
                await self._cleanup_corrupted_checkpoints(metadata_collection, checkpoints)
            
        except Exception as e:
            logger.error(f"Checkpoint corruption detection failed: {e}")
    
    async def _is_checkpoint_valid(self, checkpoint: Dict) -> bool:
        """Check if a checkpoint is valid"""
        try:
            # Basic validation
            required_fields = ["collection", "documents_migrated", "documents_skipped", "errors"]
            for field in required_fields:
                if field not in checkpoint:
                    return False
            
            # Check for reasonable values
            if checkpoint["documents_migrated"] < 0 or checkpoint["documents_skipped"] < 0 or checkpoint["errors"] < 0:
                return False
            
            # Check last_document_id if present
            last_id = checkpoint.get("last_document_id")
            if last_id:
                import bson
                if not bson.ObjectId.is_valid(last_id):
                    return False
            
            return True
            
        except Exception:
            return False
    
    async def _cleanup_corrupted_checkpoints(self, metadata_collection, checkpoints):
        """Clean up corrupted checkpoints"""
        try:
            corrupted_ids = []
            for checkpoint in checkpoints:
                if not await self._is_checkpoint_valid(checkpoint):
                    corrupted_ids.append(checkpoint["_id"])
            
            if corrupted_ids:
                result = await metadata_collection.delete_many({"_id": {"$in": corrupted_ids}})
                logger.info(f"Cleaned up {result.deleted_count} corrupted checkpoints")
            
        except Exception as e:
            logger.error(f"Failed to cleanup corrupted checkpoints: {e}")
    
    def _update_realtime_rate(self, pbar, docs_processed: int):
        """Update real-time rate calculation for progress bar"""
        try:
            current_time = time.time()
            
            # Initialize tracking variables if needed
            if not hasattr(self, 'last_update_time') or self.last_update_time == 0:
                self.last_update_time = current_time
                self.last_docs_count = 0
                self.instant_rate = 0
                return
            
            # Calculate instant rate based on incremental documents processed
            time_diff = current_time - self.last_update_time
            
            if time_diff > 0:
                # docs_processed is the incremental count from this batch
                self.instant_rate = docs_processed / time_diff
                
                # Update progress bar description with instant rate
                instant_rate_str = f"{self.instant_rate:,.0f} docs/s"
                pbar.set_description(f"🚀 Migrating data | Instant: {instant_rate_str}")
                
                # Update tracking variables
                self.last_update_time = current_time
                
        except Exception as e:
            logger.debug(f"Error updating real-time rate: {e}")
    
    def _create_checkpoint(self) -> Checkpoint:
        """Create a checkpoint"""
        return Checkpoint(
            documents_migrated=self.migration_strategy.documents_migrated,
            documents_skipped=self.migration_strategy.documents_skipped,
            errors=self.migration_strategy.errors,
            last_checkpoint=time.strftime("%Y-%m-%d %H:%M:%S"),
            last_document_id=None,  # Will be set by specific strategies
            cursor_position=None,    # Will be set by specific strategies
            metadata=self.migration_strategy.get_migration_stats()
        )
    
    def _get_final_stats(self) -> Dict[str, Any]:
        """Get final migration statistics"""
        migration_stats = self.migration_strategy.get_migration_stats()
        
        # Add database metrics
        source_performance = self.source_client.get_performance_summary()
        target_performance = self.target_client.get_performance_summary()
        
        # Add framework metrics
        framework_metrics = self.metrics_collector.get_summary()
        
        return {
            "migration_stats": migration_stats,
            "source_performance": source_performance,
            "target_performance": target_performance,
            "framework_metrics": framework_metrics,
            "total_documents_migrated": migration_stats["documents_migrated"],
            "average_rate": migration_stats["migration_rate"]
        }
    
    async def cleanup(self):
        """Clean up resources"""
        self.is_running = False
        
        if self.write_queue:
            # Signal all workers to stop
            for _ in range(self.config.workers.write_workers):
                await self.write_queue.put(None)
        
        # Wait for workers to finish
        if self.write_tasks:
            await asyncio.gather(*self.write_tasks, return_exceptions=True)
        
        # Disconnect from databases
        if self.source_client:
            await self.source_client.disconnect()
        
        if self.target_client:
            await self.target_client.disconnect()
        
        logger.info("Migration engine cleaned up")

# Factory function for creating migration engines
def create_migration_engine(config: FrameworkConfig) -> MigrationEngine:
    """Create a migration engine with the given configuration"""
    return MigrationEngine(config)
