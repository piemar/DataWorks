"""
Default Migration Strategy
Simple, generic migration strategy that copies data from source to target
"""
import logging
import bson
from typing import List, Dict, Any, Optional
from datetime import datetime

from framework.migrations.engine import BaseMigrationStrategy, MigrationBatch
from framework.core.database import BaseDatabaseClient

logger = logging.getLogger(__name__)

class DefaultMigrationStrategy(BaseMigrationStrategy):
    """
    Default migration strategy - simple copy from source to target
    
    Features:
    - Cursor-based reading with resume point
    - Simple document copying
    - Configurable resume field (default: _id)
    - Optional document transformation
    - Error handling and logging
    """
    
    def __init__(self, source_client: BaseDatabaseClient, target_client: BaseDatabaseClient, 
                 resume_field: str = "_id", transform_document: Optional[callable] = None):
        super().__init__(source_client, target_client)
        self.resume_field = resume_field
        self.transform_document = transform_document
        self.cursor_position = None
        self.last_document_value = None
    
    async def read_batch(self, batch_size: int, resume_from: Optional[str] = None) -> 'MigrationBatch':
        """Read a batch of documents using cursor-based approach"""
        try:
            # Build query for resume point
            query = {}
            if resume_from:
                try:
                    # Try to parse as ObjectId first
                    if self.resume_field == "_id":
                        # Validate ObjectId format before creating
                        if not bson.ObjectId.is_valid(resume_from):
                            raise ValueError(f"Invalid ObjectId format: {resume_from}")
                        query[self.resume_field] = {"$gt": bson.ObjectId(resume_from)}
                    else:
                        # For other fields, use string comparison
                        query[self.resume_field] = {"$gt": resume_from}
                except Exception as e:
                    logger.warning(f"Invalid resume point {resume_from}: {e}")
                    query = {}
            
            # Read documents with cursor
            cursor = self.source_client.collection.find(query).limit(batch_size)
            documents = []
            
            async for doc in cursor:
                # Transform document if transformation function provided
                if self.transform_document:
                    try:
                        doc = self.transform_document(doc)
                    except Exception as e:
                        logger.error(f"Error transforming document: {e}")
                        continue
                
                documents.append(doc)
                
                # Update cursor position
                if self.resume_field in doc:
                    self.last_document_value = str(doc[self.resume_field])
                    self.cursor_position = self.last_document_value
                
                self.documents_read += 1
            
            logger.debug(f"Read batch: {len(documents)} documents")
            
            return MigrationBatch(
                documents=documents,
                batch_number=getattr(self, '_batch_number', 0) + 1,
                source_cursor_position=self.cursor_position,
                metadata={
                    "strategy": "default",
                    "resume_field": self.resume_field,
                    "resume_from": resume_from,
                    "batch_size": batch_size,
                    "read_at": datetime.utcnow().isoformat(),
                    "transform_applied": self.transform_document is not None
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
                    "strategy": "default",
                    "error": str(e),
                    "read_at": datetime.utcnow().isoformat()
                }
            )
    
    async def get_resume_point(self) -> Optional[str]:
        """Get resume point from target database max value of resume field"""
        try:
            logger.info(f"Getting resume point from target database using field: {self.resume_field}")
            
            # Get max value of resume field from target database
            sort_field = [(self.resume_field, -1)]
            max_doc = await self.target_client.collection.find_one(sort=sort_field)
            
            if not max_doc:
                logger.info("No documents in target database; starting from beginning")
                return None
            
            max_value = max_doc.get(self.resume_field)
            if not max_value:
                logger.warning(f"Resume field '{self.resume_field}' not found in target document")
                return None
            
            max_value_str = str(max_value)
            logger.info(f"Target database max {self.resume_field}: {max_value_str}")
            
            # Validate that this value exists in source database
            if self.resume_field == "_id":
                # Validate ObjectId format before creating
                if not bson.ObjectId.is_valid(max_value_str):
                    logger.warning(f"Invalid ObjectId format from target database: {max_value_str}")
                    return None
                source_doc = await self.source_client.collection.find_one({self.resume_field: bson.ObjectId(max_value_str)})
            else:
                source_doc = await self.source_client.collection.find_one({self.resume_field: max_value})
            
            if not source_doc:
                logger.warning(f"Max {self.resume_field} from target not found in source; starting from beginning")
                return None
            
            logger.info("✅ Resume point validated successfully")
            return max_value_str
            
        except Exception as e:
            logger.error(f"Error getting resume point: {e}")
            return None
    
    def get_migration_stats(self) -> Dict[str, Any]:
        """Get migration statistics"""
        base_stats = super().get_migration_stats()
        
        # Add strategy-specific metrics
        base_stats.update({
            "strategy_metrics": {
                "strategy_type": "default",
                "resume_field": self.resume_field,
                "documents_migrated": self.documents_migrated,
                "documents_skipped": self.documents_skipped,
                "migration_errors": self.errors,
                "last_document_value": self.last_document_value,
                "cursor_position": self.cursor_position,
                "transform_enabled": self.transform_document is not None
            }
        })
        
        return base_stats
