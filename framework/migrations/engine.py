"""
Migration Framework
Enterprise-ready data migration with parallel processing, checkpointing, and monitoring
"""
import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from enum import Enum

from tqdm import tqdm

from ..core.database import BaseDatabaseClient, DatabaseConfig, DatabaseType, create_database_client
from ..config.manager import FrameworkConfig, MigrationConfig
from ..monitoring.metrics import MetricsCollector, OperationType

logger = logging.getLogger(__name__)

class MigrationStrategy(Enum):
    """Migration strategies"""
    FULL_MIGRATION = "full_migration"
    INCREMENTAL = "incremental"
    STREAMING = "streaming"
    BATCH = "batch"

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
        self.checkpoint_file = "migration_checkpoint.json"
    
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
                     checkpoint_callback: Optional[Callable] = None) -> Dict[str, Any]:
        """Execute migration using the configured strategy"""
        
        if not self.migration_strategy:
            raise ValueError("Migration strategy not set")
        
        logger.info("Starting migration...")
        
        # Get resume point
        resume_from = await self.migration_strategy.get_resume_point()
        if resume_from:
            logger.info(f"Resuming migration from: {resume_from}")
        
        # Start timing
        self.migration_strategy.start_time = time.time()
        self.is_running = True
        
        # Start write workers
        await self._start_write_workers()
        
        # Get total document count for progress bar
        total_docs = await self.source_client.get_estimated_count()
        
        # Create progress bar
        pbar = tqdm(
            total=total_docs,
            desc="🚀 Migrating data",
            unit="docs",
            unit_scale=True,
            ncols=120,
            bar_format='{desc}: {percentage:3.0f}%|{bar:25}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}',
            colour='blue',
            smoothing=self.config.performance.progress_smoothing,
            miniters=self.config.performance.progress_miniters,
            dynamic_ncols=True,
            leave=True
        )
        
        try:
            # Migration loop
            batch_number = 0
            
            while self.is_running:
                # Read batch
                batch = await self.migration_strategy.read_batch(
                    self.config.source_database.batch_size,
                    resume_from
                )
                
                if not batch.documents:
                    break  # No more documents
                
                batch_number += 1
                
                # Queue for writing
                await self.write_queue.put((batch, pbar))
                
                # Update resume point
                resume_from = batch.source_cursor_position
                
                # Update progress periodically
                if batch_number % self.config.performance.progress_update_interval == 0:
                    stats = self.migration_strategy.get_migration_stats()
                    pbar.set_postfix_str(f"Rate: {stats['migration_rate']:.0f} docs/s")
                    pbar.refresh()
                    
                    # Call progress callback if provided
                    if progress_callback:
                        await progress_callback(stats)
                    
                    # Save checkpoint
                    if checkpoint_callback:
                        checkpoint = self._create_checkpoint()
                        await checkpoint_callback(checkpoint)
            
            # Signal end to write workers
            for _ in range(self.config.workers.write_workers):
                await self.write_queue.put(None)
            
            # Wait for write workers to complete
            await asyncio.gather(*self.write_tasks, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Error during migration: {e}")
            raise
        finally:
            pbar.close()
            self.is_running = False
        
        # Return final statistics
        return self._get_final_stats()
    
    async def _start_write_workers(self):
        """Start parallel write workers"""
        self.write_tasks = []
        for i in range(self.config.workers.write_workers):
            task = asyncio.create_task(self._write_worker(i))
            self.write_tasks.append(task)
    
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
                        if batch_buffer:
                            await self._write_batch_aggregated(batch_buffer, worker_id)
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
        """Write aggregated batches to target database"""
        if not batches:
            return
        
        try:
            # Flatten all batches
            all_docs = []
            for batch in batches:
                all_docs.extend(batch.documents)
            
            if not all_docs:
                return
            
            # Track metrics
            operation = self.metrics_collector.start_operation(
                f"write_worker_{worker_id}",
                OperationType.WRITE,
                len(all_docs)
            )
            
            # Write to target database
            inserted_count = await self.target_client.bulk_insert(all_docs, ordered=False)
            
            # Update migration strategy stats
            self.migration_strategy.documents_migrated += inserted_count
            
            # Update metrics
            self.metrics_collector.end_operation(operation, inserted_count, True)
            
            # Update progress bar
            if pbar:
                pbar.update(inserted_count)
                pbar.refresh()
            
            logger.debug(f"Worker {worker_id}: Migrated {inserted_count} documents")
            
        except Exception as e:
            logger.error(f"Worker {worker_id}: Failed to migrate batch: {e}")
            self.migration_strategy.errors += 1
            if pbar:
                pbar.update(0)  # Update progress even on failure
    
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
