"""
Advanced Custom Migration Strategy Example
Shows advanced features like custom resume logic, data validation, and enrichment
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import bson

from framework.migrations.default_strategy import DefaultMigrationStrategy
from framework.migrations.engine import MigrationBatch

logger = logging.getLogger(__name__)

class AdvancedMigrationStrategy(DefaultMigrationStrategy):
    """
    Advanced Custom Migration Strategy
    
    Features:
    - Custom resume field (order_date instead of _id)
    - Data validation and cleaning
    - Field enrichment
    - Error handling and retry logic
    - Custom batch processing
    """
    
    def __init__(self, source_client, target_client, **kwargs):
        # Use custom resume field
        super().__init__(
            source_client=source_client,
            target_client=target_client,
            resume_field="order_date",  # Custom resume field
            transform_document=self.transform_document
        )
        
        # Advanced configuration
        self.validation_enabled = kwargs.get('validation_enabled', True)
        self.enrichment_enabled = kwargs.get('enrichment_enabled', True)
        self.cleanup_enabled = kwargs.get('cleanup_enabled', True)
        self.max_retries = kwargs.get('max_retries', 3)
        
        logger.info("Advanced Strategy initialized with custom resume field: order_date")
    
    def transform_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Advanced document transformation with validation and enrichment"""
        try:
            # Step 1: Validate document
            if self.validation_enabled:
                doc = self._validate_document(doc)
            
            # Step 2: Clean up document
            if self.cleanup_enabled:
                doc = self._cleanup_document(doc)
            
            # Step 3: Enrich document
            if self.enrichment_enabled:
                doc = self._enrich_document(doc)
            
            # Step 4: Add migration metadata
            doc['_migration_metadata'] = {
                'migrated_by': 'advanced_strategy',
                'migration_timestamp': datetime.utcnow().isoformat(),
                'strategy_version': '2.0.0',
                'validation_applied': self.validation_enabled,
                'enrichment_applied': self.enrichment_enabled,
                'cleanup_applied': self.cleanup_enabled
            }
            
            return doc
            
        except Exception as e:
            logger.error(f"Error in advanced transformation: {e}")
            # Return original document with error metadata
            doc['_migration_error'] = str(e)
            return doc
    
    def _validate_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Validate document structure and data"""
        # Example validations
        if 'order_id' not in doc:
            logger.warning("Document missing order_id, adding default")
            doc['order_id'] = f"ORDER_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        
        if 'customer' not in doc:
            logger.warning("Document missing customer info")
            doc['customer'] = {'name': 'Unknown Customer'}
        
        # Validate email format
        if 'customer' in doc and 'email' in doc['customer']:
            email = doc['customer']['email']
            if '@' not in email:
                logger.warning(f"Invalid email format: {email}")
                doc['customer']['email'] = f"invalid_{email}@example.com"
        
        return doc
    
    def _cleanup_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Clean up document by removing unnecessary fields"""
        # Remove fields that shouldn't be migrated
        fields_to_remove = ['_temp_field', '_debug_field', '_test_field']
        for field in fields_to_remove:
            if field in doc:
                del doc[field]
                logger.debug(f"Removed field: {field}")
        
        # Clean up empty strings
        for key, value in doc.items():
            if isinstance(value, str) and value.strip() == '':
                doc[key] = None
                logger.debug(f"Cleaned empty string field: {key}")
        
        return doc
    
    def _enrich_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich document with additional data"""
        # Add computed fields
        if 'service_items' in doc and isinstance(doc['service_items'], list):
            doc['_computed_fields'] = {
                'total_items': len(doc['service_items']),
                'has_items': len(doc['service_items']) > 0,
                'enriched_at': datetime.utcnow().isoformat()
            }
        
        # Add customer segment based on total amount
        if 'total_amount' in doc:
            amount = doc['total_amount']
            if amount > 1000:
                doc['customer_segment'] = 'premium'
            elif amount > 500:
                doc['customer_segment'] = 'standard'
            else:
                doc['customer_segment'] = 'basic'
        
        # Add geographic enrichment
        if 'customer' in doc and 'address' in doc['customer']:
            doc['_geographic_info'] = {
                'region': 'unknown',  # Could be enriched from external API
                'timezone': 'UTC',
                'enriched_at': datetime.utcnow().isoformat()
            }
        
        return doc
    
    async def get_resume_point(self) -> Optional[str]:
        """Override to handle custom resume field (order_date)"""
        try:
            logger.info("Getting resume point using custom field: order_date")
            
            # Get max order_date from target database
            max_doc = await self.target_client.collection.find_one(sort=[("order_date", -1)])
            
            if not max_doc:
                logger.info("No documents in target database; starting from beginning")
                return None
            
            max_date = max_doc.get('order_date')
            if not max_date:
                logger.warning("order_date field not found in target document")
                return None
            
            max_date_str = str(max_date)
            logger.info(f"Target database max order_date: {max_date_str}")
            
            # Validate that this date exists in source database
            source_doc = await self.source_client.collection.find_one({"order_date": max_date})
            
            if not source_doc:
                logger.warning("Max order_date from target not found in source; starting from beginning")
                return None
            
            logger.info("✅ Custom resume point validated successfully")
            return max_date_str
            
        except Exception as e:
            logger.error(f"Error getting custom resume point: {e}")
            return None
    
    async def read_batch(self, batch_size: int, resume_from: Optional[str] = None) -> MigrationBatch:
        """Override with custom batch processing"""
        logger.info(f"Advanced Strategy: Processing batch with custom logic")
        
        # Call parent method
        batch = await super().read_batch(batch_size, resume_from)
        
        # Add custom batch processing
        if batch.documents:
            logger.info(f"Advanced Strategy: Processed {len(batch.documents)} documents")
            
            # Example: Log statistics about the batch
            valid_docs = [doc for doc in batch.documents if '_migration_error' not in doc]
            error_docs = [doc for doc in batch.documents if '_migration_error' in doc]
            
            logger.info(f"Batch stats: {len(valid_docs)} valid, {len(error_docs)} errors")
        
        return batch
    
    def get_migration_stats(self) -> Dict[str, Any]:
        """Override to add advanced metrics"""
        stats = super().get_migration_stats()
        
        # Add advanced metrics
        stats['advanced_metrics'] = {
            'strategy_type': 'advanced',
            'resume_field': self.resume_field,
            'validation_enabled': self.validation_enabled,
            'enrichment_enabled': self.enrichment_enabled,
            'cleanup_enabled': self.cleanup_enabled,
            'max_retries': self.max_retries,
            'documents_processed': self.documents_read,
            'documents_migrated': self.documents_migrated,
            'custom_resume_logic': True
        }
        
        return stats
