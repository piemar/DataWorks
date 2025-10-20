"""
Data Generation Framework
Enterprise-ready data generation with pluggable generators, parallel processing, and monitoring
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
from ..config.manager import FrameworkConfig, DataGenerationConfig
from ..monitoring.metrics import MetricsCollector, OperationType

logger = logging.getLogger(__name__)

class GeneratorType(Enum):
    """Types of data generators"""
    SERVICE_ORDER = "service_order"
    USER_PROFILE = "user_profile"
    PRODUCT_CATALOG = "product_catalog"
    TRANSACTION_LOG = "transaction_log"
    CUSTOM = "custom"

@dataclass
class GenerationBatch:
    """A batch of generated data"""
    documents: List[Dict[str, Any]]
    batch_number: int
    generator_type: GeneratorType
    metadata: Dict[str, Any] = None

class BaseDataGenerator(ABC):
    """
    Abstract base class for data generators
    
    Provides common functionality for:
    - Document generation
    - Batch processing
    - Metadata tracking
    - Performance monitoring
    """
    
    def __init__(self, generator_type: GeneratorType):
        self.generator_type = generator_type
        self.documents_generated = 0
        self.start_time = None
        self.metadata = {}
    
    @abstractmethod
    def generate_document(self, document_id: Optional[str] = None) -> Dict[str, Any]:
        """Generate a single document - must be implemented by subclasses"""
        pass
    
    def generate_batch(self, batch_size: int, batch_number: int) -> GenerationBatch:
        """Generate a batch of documents"""
        documents = []
        for i in range(batch_size):
            doc = self.generate_document()
            documents.append(doc)
            self.documents_generated += 1
        
        return GenerationBatch(
            documents=documents,
            batch_number=batch_number,
            generator_type=self.generator_type,
            metadata=self.metadata
        )
    
    def get_generation_stats(self) -> Dict[str, Any]:
        """Get generation statistics"""
        elapsed = time.time() - self.start_time if self.start_time else 0
        rate = self.documents_generated / elapsed if elapsed > 0 else 0
        
        return {
            "generator_type": self.generator_type.value,
            "documents_generated": self.documents_generated,
            "elapsed_time": elapsed,
            "generation_rate": rate,
            "metadata": self.metadata
        }

class DataGenerationEngine:
    """
    Enterprise data generation engine with:
    - Parallel processing
    - Multiple generator support
    - Real-time monitoring
    - Configurable workers
    - Progress tracking
    """
    
    def __init__(self, config: FrameworkConfig):
        self.config = config
        self.db_client: Optional[BaseDatabaseClient] = None
        self.metrics_collector = MetricsCollector()
        self.generators: Dict[GeneratorType, BaseDataGenerator] = {}
        self.is_running = False
        self.write_queue = None
        self.write_tasks = []
        
        # Performance tracking
        self.last_update_time = 0
        self.last_docs_count = 0
        self.instant_rate = 0
        
    def register_generator(self, generator: BaseDataGenerator):
        """Register a data generator"""
        self.generators[generator.generator_type] = generator
        logger.info(f"Registered generator: {generator.generator_type.value}")
    
    async def initialize(self) -> bool:
        """Initialize the generation engine"""
        try:
            # Create database client
            db_config = DatabaseConfig(
                connection_string=self.config.source_database.connection_string,
                database_name=self.config.source_database.database_name,
                collection_name=self.config.source_database.collection_name,
                db_type=DatabaseType.COSMOS_DB,  # Default to Cosmos DB for generation
                batch_size=self.config.source_database.batch_size,
                max_pool_size=self.config.source_database.max_pool_size,
                min_pool_size=self.config.source_database.min_pool_size,
                max_idle_time_ms=self.config.source_database.max_idle_time_ms,
                socket_timeout_ms=self.config.source_database.socket_timeout_ms,
                connect_timeout_ms=self.config.source_database.connect_timeout_ms,
                warmup_connections=self.config.source_database.warmup_connections
            )
            
            self.db_client = create_database_client(db_config)
            
            # Connect to database
            if not await self.db_client.connect():
                return False
            
            # Initialize write queue
            self.write_queue = asyncio.Queue(
                maxsize=self.config.workers.write_workers * self.config.workers.write_queue_multiplier
            )
            
            logger.info("Data generation engine initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize generation engine: {e}")
            return False
    
    async def generate_data(self, 
                          generator_type: GeneratorType, 
                          total_documents: Optional[int] = None,
                          progress_callback: Optional[Callable] = None) -> Dict[str, Any]:
        """Generate data using specified generator"""
        
        if generator_type not in self.generators:
            raise ValueError(f"Generator {generator_type.value} not registered")
        
        generator = self.generators[generator_type]
        total_docs = total_documents or self.config.data_generation.total_documents
        
        logger.info(f"Starting data generation: {total_docs:,} documents using {generator_type.value}")
        
        # Start timing
        generator.start_time = time.time()
        self.is_running = True
        
        # Start write workers
        await self._start_write_workers()
        
        # Create progress bar
        pbar = tqdm(
            total=total_docs,
            desc=f"🚀 Generating {generator_type.value} data",
            unit="docs",
            unit_scale=True,
            ncols=120,
            bar_format='{desc}: {percentage:3.0f}%|{bar:25}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}',
            colour='green',
            smoothing=self.config.performance.progress_smoothing,
            miniters=self.config.performance.progress_miniters,
            dynamic_ncols=True,
            leave=True
        )
        
        try:
            # Generate and queue batches
            current_batch_number = 0
            remaining_docs = total_docs
            
            while remaining_docs > 0 and self.is_running:
                current_batch_size = min(self.config.source_database.batch_size, remaining_docs)
                current_batch_number += 1
                
                # Generate batch
                batch = generator.generate_batch(current_batch_size, current_batch_number)
                
                # Queue for writing
                await self.write_queue.put((batch, pbar))
                
                remaining_docs -= current_batch_size
                
                # Update progress periodically
                if current_batch_number % self.config.performance.progress_update_interval == 0:
                    stats = generator.get_generation_stats()
                    pbar.set_postfix_str(f"Rate: {stats['generation_rate']:.0f} docs/s")
                    pbar.refresh()
                    
                    # Call progress callback if provided
                    if progress_callback:
                        await progress_callback(stats)
            
            # Signal end to write workers
            for _ in range(self.config.workers.write_workers):
                await self.write_queue.put(None)
            
            # Wait for write workers to complete
            await asyncio.gather(*self.write_tasks, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Error during data generation: {e}")
            raise
        finally:
            pbar.close()
            self.is_running = False
        
        # Return final statistics
        return self._get_final_stats(generator)
    
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
    
    async def _write_batch_aggregated(self, batches: List[GenerationBatch], worker_id: int, pbar=None):
        """Write aggregated batches to database"""
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
            
            # Write to database
            inserted_count = await self.db_client.bulk_insert(all_docs, ordered=False)
            
            # Update metrics
            self.metrics_collector.end_operation(operation, inserted_count, True)
            
            # ULTRA-FAST: Real-time progress update with rate calculation
            if pbar and inserted_count > 0:
                pbar.update(inserted_count)
                self._update_realtime_rate(pbar, inserted_count)
                pbar.refresh()  # Force immediate display update
            
            logger.debug(f"Worker {worker_id}: Wrote {inserted_count} documents")
            
        except Exception as e:
            logger.error(f"Worker {worker_id}: Failed to write batch: {e}")
            if pbar:
                pbar.update(0)  # Update progress even on failure
    
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
            
            # Calculate instant rate
            time_diff = current_time - self.last_update_time
            docs_diff = docs_processed - self.last_docs_count
            
            if time_diff > 0:
                self.instant_rate = docs_diff / time_diff
                
                # Update progress bar description with instant rate
                current_desc = pbar.desc or ""
                if "Instant:" in current_desc:
                    # Remove existing instant rate
                    current_desc = current_desc.split("Instant:")[0].strip()
                
                # Add new instant rate
                instant_rate_str = f"{self.instant_rate:,.0f} docs/s"
                new_desc = f"{current_desc} | Instant: {instant_rate_str}"
                pbar.set_description(new_desc)
                
                # Update tracking variables
                self.last_update_time = current_time
                self.last_docs_count = docs_processed
                
        except Exception as e:
            logger.debug(f"Error updating real-time rate: {e}")
    
    def _get_final_stats(self, generator: BaseDataGenerator) -> Dict[str, Any]:
        """Get final generation statistics"""
        stats = generator.get_generation_stats()
        
        # Add database metrics
        db_performance = self.db_client.get_performance_summary()
        
        # Add framework metrics
        framework_metrics = self.metrics_collector.get_summary()
        
        return {
            "generation_stats": stats,
            "database_performance": db_performance,
            "framework_metrics": framework_metrics,
            "total_documents_written": stats["documents_generated"],
            "average_rate": stats["generation_rate"]
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
        
        # Disconnect from database
        if self.db_client:
            await self.db_client.disconnect()
        
        logger.info("Data generation engine cleaned up")

# Factory function for creating generation engines
def create_generation_engine(config: FrameworkConfig) -> DataGenerationEngine:
    """Create a data generation engine with the given configuration"""
    return DataGenerationEngine(config)
