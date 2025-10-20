"""
Core Database Client Framework
Enterprise-ready database operations with connection pooling, monitoring, and error handling
"""
import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass  # uvloop not available, using default event loop

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
from pymongo.errors import InvalidOperation, ServerSelectionTimeoutError
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class DatabaseType(Enum):
    """Supported database types"""
    COSMOS_DB = "cosmos_db"
    MONGODB_ATLAS = "mongodb_atlas"
    MONGODB_LOCAL = "mongodb_local"

@dataclass
class DatabaseConfig:
    """Database configuration"""
    connection_string: str
    database_name: str
    collection_name: str
    db_type: DatabaseType
    batch_size: int = 1000
    max_pool_size: int = 100
    min_pool_size: int = 10
    max_idle_time_ms: int = 300000
    socket_timeout_ms: int = 30000
    connect_timeout_ms: int = 20000
    warmup_connections: int = 10

@dataclass
class OperationMetrics:
    """Operation performance metrics"""
    operation_name: str
    start_time: float
    end_time: float
    documents_processed: int
    success: bool
    error_message: Optional[str] = None
    
    @property
    def duration(self) -> float:
        return self.end_time - self.start_time
    
    @property
    def rate(self) -> float:
        return self.documents_processed / self.duration if self.duration > 0 else 0

class BaseDatabaseClient(ABC):
    """
    Abstract base class for database operations
    
    Provides common functionality for:
    - Connection management
    - Performance monitoring
    - Error handling
    - Batch operations
    """
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.client: Optional[AsyncIOMotorClient] = None
        self.database = None
        self.collection = None
        self.metrics: List[OperationMetrics] = []
        self.is_connected = False
        
    async def connect(self) -> bool:
        """Connect to database with optimized settings"""
        try:
            logger.info(f"Connecting to {self.config.db_type.value}...")
            
            # Create client with optimized settings
            self.client = AsyncIOMotorClient(
                self.config.connection_string,
                maxPoolSize=self.config.max_pool_size,
                minPoolSize=self.config.min_pool_size,
                maxIdleTimeMS=self.config.max_idle_time_ms,
                socketTimeoutMS=self.config.socket_timeout_ms,
                connectTimeoutMS=self.config.connect_timeout_ms
            )
            
            # Get database and collection
            self.database = self.client[self.config.database_name]
            self.collection = self.database[self.config.collection_name]
            
            # Pre-warm connections
            await self._warmup_connections()
            
            # Test connection
            await self.client.admin.command('ping')
            
            self.is_connected = True
            logger.info(f"✅ Connected to {self.config.db_type.value}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to {self.config.db_type.value}: {e}")
            return False
    
    async def _warmup_connections(self):
        """Pre-warm connection pool for better performance"""
        if self.config.warmup_connections <= 0:
            return
            
        logger.info(f"Pre-warming {self.config.warmup_connections} connections...")
        warmup_tasks = []
        for i in range(min(self.config.warmup_connections, 50)):  # Limit to 50
            warmup_tasks.append(self.client.admin.command('ping'))
        
        await asyncio.gather(*warmup_tasks, return_exceptions=True)
        logger.info("Connection pool pre-warmed")
    
    async def disconnect(self):
        """Disconnect from database"""
        if self.client:
            self.client.close()
            self.is_connected = False
            logger.info(f"Disconnected from {self.config.db_type.value}")
    
    async def get_document_count(self, query: Optional[Dict] = None) -> int:
        """Get document count with caching"""
        try:
            if query is None:
                # Use fast estimate when no filter is provided
                return await self.collection.estimated_document_count()
            # For filtered counts, fall back to precise count
            if isinstance(query, dict) and len(query) == 0:
                return await self.collection.estimated_document_count()
            return await self.collection.count_documents(query)
        except Exception as e:
            logger.error(f"Error getting document count: {e}")
            return 0
    
    async def get_estimated_count(self) -> int:
        """Get estimated document count (faster for large collections)"""
        try:
            return await self.collection.estimated_document_count()
        except Exception as e:
            logger.error(f"Error getting estimated count: {e}")
            return 0
    
    async def find_max_id(self) -> Optional[Any]:
        """Find maximum _id in collection"""
        try:
            doc = await self.collection.find_one(sort=[("_id", -1)])
            return doc["_id"] if doc else None
        except Exception as e:
            logger.error(f"Error finding max _id: {e}")
            return None
    
    def start_operation(self, operation_name: str) -> OperationMetrics:
        """Start tracking an operation"""
        return OperationMetrics(
            operation_name=operation_name,
            start_time=time.time(),
            end_time=0,
            documents_processed=0,
            success=False
        )
    
    def end_operation(self, metrics: OperationMetrics, documents_processed: int, success: bool, error_message: Optional[str] = None):
        """End tracking an operation"""
        metrics.end_time = time.time()
        metrics.documents_processed = documents_processed
        metrics.success = success
        metrics.error_message = error_message
        self.metrics.append(metrics)
        
        if success:
            # Suppress bulk_insert INFO logs to keep progress bar clean
            if metrics.operation_name == "bulk_insert":
                logger.debug(f"✅ {metrics.operation_name}: {documents_processed:,} docs in {metrics.duration:.2f}s ({metrics.rate:.0f} docs/s)")
            else:
                logger.info(f"✅ {metrics.operation_name}: {documents_processed:,} docs in {metrics.duration:.2f}s ({metrics.rate:.0f} docs/s)")
        else:
            logger.error(f"❌ {metrics.operation_name}: Failed - {error_message}")
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary of all operations"""
        if not self.metrics:
            return {"message": "No operations recorded"}
        
        successful_ops = [m for m in self.metrics if m.success]
        failed_ops = [m for m in self.metrics if not m.success]
        
        total_docs = sum(m.documents_processed for m in successful_ops)
        total_time = sum(m.duration for m in successful_ops)
        
        return {
            "total_operations": len(self.metrics),
            "successful_operations": len(successful_ops),
            "failed_operations": len(failed_ops),
            "total_documents_processed": total_docs,
            "total_time": total_time,
            "average_rate": total_docs / total_time if total_time > 0 else 0,
            "success_rate": len(successful_ops) / len(self.metrics) if self.metrics else 0
        }
    
    @abstractmethod
    async def bulk_insert(self, documents: List[Dict], ordered: bool = False) -> int:
        """Bulk insert documents - must be implemented by subclasses"""
        pass
    
    @abstractmethod
    async def bulk_write(self, operations: List[Any], ordered: bool = False) -> int:
        """Bulk write operations - must be implemented by subclasses"""
        pass

class CosmosDBClient(BaseDatabaseClient):
    """Cosmos DB client with Cosmos-specific optimizations"""
    
    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
        self.ru_consumed = 0
        self.ru_per_second = 0
        self.last_ru_check = time.time()
    
    async def bulk_insert(self, documents: List[Dict], ordered: bool = False) -> int:
        """Bulk insert with Cosmos DB optimizations"""
        metrics = self.start_operation("bulk_insert")
        
        try:
            result = await self.collection.insert_many(documents, ordered=ordered)
            inserted_count = len(result.inserted_ids)
            
            # Track RU consumption
            self.ru_consumed += len(documents) * 6  # Approximate RU per document
            
            self.end_operation(metrics, inserted_count, True)
            return inserted_count
            
        except Exception as e:
            self.end_operation(metrics, 0, False, str(e))
            return 0
    
    async def bulk_write(self, operations: List[Any], ordered: bool = False) -> int:
        """Bulk write with Cosmos DB optimizations"""
        metrics = self.start_operation("bulk_write")
        
        try:
            result = await self.collection.bulk_write(operations, ordered=ordered)
            
            # Get inserted count (may not be available with unacknowledged writes)
            try:
                inserted_count = result.inserted_count
            except (AttributeError, InvalidOperation):
                inserted_count = len(operations)
            
            self.end_operation(metrics, inserted_count, True)
            return inserted_count
            
        except Exception as e:
            self.end_operation(metrics, 0, False, str(e))
            return 0

class MongoDBAtlasClient(BaseDatabaseClient):
    """MongoDB Atlas client with Atlas-specific optimizations"""
    
    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
    
    async def bulk_insert(self, documents: List[Dict], ordered: bool = False) -> int:
        """Bulk insert with Atlas optimizations"""
        metrics = self.start_operation("bulk_insert")
        
        try:
            result = await self.collection.insert_many(documents, ordered=ordered)
            inserted_count = len(result.inserted_ids)
            
            self.end_operation(metrics, inserted_count, True)
            return inserted_count
            
        except Exception as e:
            self.end_operation(metrics, 0, False, str(e))
            return 0
    
    async def bulk_write(self, operations: List[Any], ordered: bool = False) -> int:
        """Bulk write with Atlas optimizations"""
        metrics = self.start_operation("bulk_write")
        
        try:
            result = await self.collection.bulk_write(operations, ordered=ordered)
            
            # Get inserted count (may not be available with unacknowledged writes)
            try:
                inserted_count = result.inserted_count
            except (AttributeError, InvalidOperation):
                inserted_count = len(operations)
            
            self.end_operation(metrics, inserted_count, True)
            return inserted_count
            
        except Exception as e:
            self.end_operation(metrics, 0, False, str(e))
            return 0

def create_database_client(config: DatabaseConfig) -> BaseDatabaseClient:
    """Factory function to create appropriate database client"""
    if config.db_type == DatabaseType.COSMOS_DB:
        return CosmosDBClient(config)
    elif config.db_type in [DatabaseType.MONGODB_ATLAS, DatabaseType.MONGODB_LOCAL]:
        return MongoDBAtlasClient(config)
    else:
        raise ValueError(f"Unsupported database type: {config.db_type}")
