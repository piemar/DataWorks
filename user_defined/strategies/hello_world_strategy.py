"""
Custom Migration Strategy Example - Hello World
This is a simple example showing how to create a custom migration strategy
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from framework.migrations.default_strategy import DefaultMigrationStrategy
from framework.migrations.engine import MigrationBatch

logger = logging.getLogger(__name__)

class HelloWorldMigrationStrategy(DefaultMigrationStrategy):
    """
    Hello World Custom Migration Strategy
    
    This example shows how to:
    1. Extend the default strategy
    2. Add custom document transformation
    3. Use custom resume field
    4. Add custom logging
    """
    
    def __init__(self, source_client, target_client, **kwargs):
        # Initialize with custom resume field
        super().__init__(
            source_client=source_client,
            target_client=target_client,
            resume_field="_id",  # Use _id as resume field
            transform_document=self.transform_document  # Add transformation
        )
        
        # Custom configuration
        self.custom_field = kwargs.get('custom_field', 'hello_world')
        self.enrichment_enabled = kwargs.get('enrichment_enabled', True)
        
        logger.info(f"Hello World Strategy initialized with custom_field: {self.custom_field}")
    
    def transform_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform document before migration
        
        This is where you can:
        - Add new fields
        - Modify existing fields
        - Remove fields
        - Validate data
        """
        try:
            # Add custom field
            doc['_migration_metadata'] = {
                'migrated_by': 'hello_world_strategy',
                'migration_timestamp': datetime.utcnow().isoformat(),
                'custom_field': self.custom_field
            }
            
            # Example: Add enrichment if enabled
            if self.enrichment_enabled:
                doc['_enrichment'] = {
                    'enriched_at': datetime.utcnow().isoformat(),
                    'enrichment_type': 'hello_world'
                }
            
            # Example: Modify existing field
            if 'status' in doc:
                doc['status'] = f"migrated_{doc['status']}"
            
            logger.debug(f"Transformed document: {doc.get('_id', 'unknown')}")
            return doc
            
        except Exception as e:
            logger.error(f"Error transforming document: {e}")
            return doc  # Return original document on error
    
    async def read_batch(self, batch_size: int, resume_from: Optional[str] = None) -> MigrationBatch:
        """Override read_batch to add custom logging"""
        logger.info(f"Hello World Strategy: Reading batch of {batch_size} documents")
        
        # Call parent method
        batch = await super().read_batch(batch_size, resume_from)
        
        # Add custom logging
        if batch.documents:
            logger.info(f"Hello World Strategy: Successfully read {len(batch.documents)} documents")
        
        return batch
    
    def get_migration_stats(self) -> Dict[str, Any]:
        """Override to add custom stats"""
        stats = super().get_migration_stats()
        
        # Add custom metrics
        stats['hello_world_metrics'] = {
            'custom_field': self.custom_field,
            'enrichment_enabled': self.enrichment_enabled,
            'strategy_type': 'hello_world',
            'documents_processed': self.documents_read,
            'documents_migrated': self.documents_migrated
        }
        
        return stats
