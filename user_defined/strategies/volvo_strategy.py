"""
Volvo Service Orders Migration Strategy
Specific implementation for migrating Volvo service order data
"""
import logging
import bson
from typing import List, Dict, Any, Optional
from datetime import datetime

from framework.migrations.engine import BaseMigrationStrategy, MigrationBatch
from framework.core.database import BaseDatabaseClient

logger = logging.getLogger(__name__)

class VolvoServiceOrderMigrationStrategy(BaseMigrationStrategy):
    """
    Volvo-specific migration strategy with:
    - Cursor-based reading
    - Resume point management
    - Document validation
    - Error handling
    """
    
    def __init__(self, source_client: BaseDatabaseClient, target_client: BaseDatabaseClient):
        super().__init__(source_client, target_client)
        self.cursor_position = None
        self.last_document_id = None
    
    async def read_batch(self, batch_size: int, resume_from: Optional[str] = None) -> 'MigrationBatch':
        """Read a batch of documents using cursor-based approach"""
        try:
            # Build query
            query = {}
            if resume_from:
                try:
                    # Resume from specific ObjectId
                    # Validate ObjectId format before creating
                    if not bson.ObjectId.is_valid(resume_from):
                        raise ValueError(f"Invalid ObjectId format: {resume_from}")
                    query["_id"] = {"$gt": bson.ObjectId(resume_from)}
                except Exception as e:
                    logger.warning(f"Invalid resume point {resume_from}: {e}")
                    query = {}
            
            # Read documents with cursor
            cursor = self.source_client.collection.find(query).limit(batch_size)
            documents = []
            
            async for doc in cursor:
                documents.append(doc)
                self.last_document_id = str(doc["_id"])
                self.documents_read += 1
            
            # Update cursor position
            if documents:
                self.cursor_position = self.last_document_id
            
            logger.debug(f"Read batch: {len(documents)} documents")
            
            return MigrationBatch(
                documents=documents,
                batch_number=getattr(self, '_batch_number', 0) + 1,
                source_cursor_position=self.cursor_position,
                metadata={
                    "strategy": "volvo_service_order",
                    "resume_from": resume_from,
                    "batch_size": batch_size,
                    "read_at": datetime.utcnow().isoformat()
                }
            )
            
        except Exception as e:
            logger.error(f"Error reading batch: {e}")
            self.errors += 1
            return MigrationBatch(
                documents=[],
                batch_number=getattr(self, '_batch_number', 0) + 1,
                source_cursor_position=self.cursor_position,
                metadata={
                    "strategy": "volvo_service_order",
                    "error": str(e),
                    "read_at": datetime.utcnow().isoformat()
                }
            )
    
    async def get_resume_point(self) -> Optional[str]:
        """Get resume point from target database max _id"""
        try:
            logger.info("Getting resume point from target database...")
            
            # Get max _id from target database
            max_doc = await self.target_client.collection.find_one(sort=[("_id", -1)])
            
            if not max_doc:
                logger.info("No documents in target database; starting from beginning")
                return None
            
            max_id = str(max_doc["_id"])
            logger.info(f"Target database max ID: {max_id}")
            
            # Validate that this ID exists in source database
            # Validate ObjectId format before creating
            if not bson.ObjectId.is_valid(max_id):
                logger.warning(f"Invalid ObjectId format from target database: {max_id}")
                return None
            source_doc = await self.source_client.collection.find_one({"_id": bson.ObjectId(max_id)})
            if not source_doc:
                logger.warning("Max ID from target not found in source; starting from beginning")
                return None
            
            logger.info("✅ Resume point validated successfully")
            return max_id
            
        except Exception as e:
            logger.error(f"Error getting resume point: {e}")
            return None
    
    def get_migration_stats(self) -> Dict[str, Any]:
        """Get migration statistics with Volvo-specific metrics"""
        base_stats = super().get_migration_stats()
        
        # Add Volvo-specific metrics
        base_stats.update({
            "volvo_metrics": {
                "service_orders_migrated": self.documents_migrated,
                "service_orders_skipped": self.documents_skipped,
                "migration_errors": self.errors,
                "last_document_id": self.last_document_id,
                "cursor_position": self.cursor_position
            }
        })
        
        return base_stats
