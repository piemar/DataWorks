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
            skip_first = False
            if resume_from:
                try:
                    # Resume from specific ObjectId
                    # Validate ObjectId format before creating
                    if not bson.ObjectId.is_valid(resume_from):
                        raise ValueError(f"Invalid ObjectId format: {resume_from}")
                    query["_id"] = {"$gte": bson.ObjectId(resume_from)}
                    skip_first = True  # Skip the resume document itself
                except Exception as e:
                    logger.warning(f"Invalid resume point {resume_from}: {e}")
                    query = {}
            
            # Read documents with cursor
            cursor = self.source_client.collection.find(query).limit(batch_size)
            documents = []
            
            async for doc in cursor:
                if skip_first:
                    # Skip the resume document itself
                    skip_first = False
                    continue
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
    
    async def get_resume_point(self, force_from_start: bool = False) -> Optional[str]:
        """Get reliable resume point - ALWAYS try metadata table first"""
        if force_from_start:
            logger.info("🚀 Force from start enabled - skipping resume point detection")
            return None
            
        try:
            logger.info("Getting resume point from migration metadata table...")
            
            # Strategy 1: ALWAYS try migration metadata collection first (most reliable)
            resume_point = await self._get_resume_from_metadata()
            if resume_point:
                logger.info(f"✅ Resume point from metadata: {resume_point}")
                return resume_point
            
            # Strategy 2: Fallback to count-based resume (only if no metadata)
            logger.info("No metadata checkpoint found, trying count-based resume...")
            resume_point = await self._get_resume_from_count()
            if resume_point:
                logger.info(f"✅ Resume point from count: {resume_point}")
                return resume_point
            
            # Strategy 3: Fallback to timestamp-based resume (only if no metadata/count)
            logger.info("No count-based resume found, trying timestamp-based resume...")
            resume_point = await self._get_resume_from_timestamp()
            if resume_point:
                logger.info(f"✅ Resume point from timestamp: {resume_point}")
                return resume_point
            
            # Strategy 4: Final fallback to max _id (only if all else fails)
            logger.info("No timestamp-based resume found, trying max _id fallback...")
            resume_point = await self._get_resume_from_max_id()
            if resume_point:
                logger.info(f"✅ Resume point from max _id: {resume_point}")
                return resume_point
            
            logger.info("No reliable resume point found; starting from beginning")
            return None
            
        except Exception as e:
            logger.error(f"Error getting resume point: {e}")
            return None
    
    async def _get_resume_from_metadata(self) -> Optional[str]:
        """Strategy 1: Get resume point from migration metadata collection with validation"""
        try:
            # Check if migration metadata collection exists
            metadata_collection = self.target_client.database["migration_metadata"]
            
            # Get the latest migration run (prefer in_progress, then completed)
            # Sort by checkpoint_at first (most recent), then completed_at
            latest_run = await metadata_collection.find_one(
                {
                    "collection": self.target_client.collection.name,
                    "status": {"$in": ["in_progress", "completed"]}
                },
                sort=[("checkpoint_at", -1), ("completed_at", -1)]
            )
            
            if latest_run and latest_run.get("last_document_id"):
                last_id = latest_run["last_document_id"]
                status = latest_run.get("status", "unknown")
                checkpoint_time = latest_run.get("checkpoint_at") or latest_run.get("completed_at", "unknown")
                
                logger.info(f"Found migration metadata: last_document_id={last_id}, status={status}, time={checkpoint_time}")
                
                # Validate checkpoint consistency
                if await self._validate_checkpoint(latest_run):
                    logger.info("✅ Migration metadata resume point validated and ready to use")
                    return last_id
                else:
                    logger.warning("❌ Migration metadata checkpoint validation failed - will try fallback strategies")
                    return None
            
            logger.info("No migration metadata checkpoint found")
            return None
            
        except Exception as e:
            logger.debug(f"Migration metadata strategy failed: {e}")
            return None
    
    async def _validate_checkpoint(self, checkpoint_doc: Dict) -> bool:
        """Validate checkpoint consistency and integrity"""
        try:
            # Check if last_document_id exists in source
            last_id = checkpoint_doc.get("last_document_id")
            if not last_id or not bson.ObjectId.is_valid(last_id):
                logger.warning("Invalid last_document_id in checkpoint")
                return False
            
            source_doc = await self.source_client.collection.find_one({"_id": bson.ObjectId(last_id)})
            if not source_doc:
                logger.warning("Last document ID from checkpoint not found in source")
                return False
            
            # Check if checkpoint counts are reasonable
            migrated_count = checkpoint_doc.get("documents_migrated", 0)
            skipped_count = checkpoint_doc.get("documents_skipped", 0)
            error_count = checkpoint_doc.get("errors", 0)
            
            if migrated_count < 0 or skipped_count < 0 or error_count < 0:
                logger.warning("Invalid negative counts in checkpoint")
                return False
            
            # Check if checkpoint is not too old (optional)
            checkpoint_time = checkpoint_doc.get("checkpoint_at") or checkpoint_doc.get("completed_at")
            if checkpoint_time:
                from datetime import datetime, timedelta
                try:
                    if isinstance(checkpoint_time, str):
                        checkpoint_dt = datetime.fromisoformat(checkpoint_time.replace('Z', '+00:00'))
                    else:
                        checkpoint_dt = checkpoint_time
                    
                    # Check if checkpoint is older than 7 days (optional warning)
                    if datetime.utcnow() - checkpoint_dt.replace(tzinfo=None) > timedelta(days=7):
                        logger.warning("Checkpoint is older than 7 days - may be stale")
                        # Don't fail validation, just warn
                except Exception:
                    pass  # Ignore date parsing errors
            
            logger.debug("✅ Checkpoint validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Checkpoint validation error: {e}")
            return False
    
    async def _get_resume_from_count(self) -> Optional[str]:
        """Strategy 2: Get resume point based on document counts"""
        try:
            # Get counts from both databases
            source_count = await self.source_client.collection.estimated_document_count({})
            target_count = await self.target_client.collection.estimated_document_count({})
            
            logger.info(f"Source count: {source_count:,}, Target count: {target_count:,}")
            
            if target_count >= source_count:
                logger.info("Target has same or more documents than source - migration complete")
                return None
            
            if target_count == 0:
                logger.info("Target is empty - starting from beginning")
                return None
            
            # Find the document at the target_count position in source
            # This gives us a more reliable resume point than max _id
            cursor = self.source_client.collection.find().sort("_id", 1).skip(target_count).limit(1)
            resume_doc = await cursor.to_list(1)
            
            if resume_doc:
                resume_id = str(resume_doc[0]["_id"])
                logger.info(f"✅ Count-based resume point: {resume_id} (position {target_count})")
                return resume_id
            
            return None
            
        except Exception as e:
            logger.debug(f"Count-based strategy failed: {e}")
            return None
    
    async def _get_resume_from_timestamp(self) -> Optional[str]:
        """Strategy 3: Get resume point based on timestamp"""
        try:
            # Get the latest document from target by timestamp
            # Assuming documents have a timestamp field (created_at, updated_at, etc.)
            timestamp_fields = ["created_at", "updated_at", "timestamp", "date_created"]
            
            for field in timestamp_fields:
                try:
                    max_doc = await self.target_client.collection.find_one(sort=[(field, -1)])
                    if max_doc and field in max_doc:
                        timestamp_value = max_doc[field]
                        logger.info(f"Found latest document by {field}: {timestamp_value}")
                        
                        # Find documents after this timestamp in source
                        source_doc = await self.source_client.collection.find_one(
                            {field: {"$gt": timestamp_value}},
                            sort=[(field, 1)]
                        )
                        
                        if source_doc:
                            resume_id = str(source_doc["_id"])
                            logger.info(f"✅ Timestamp-based resume point: {resume_id}")
                            return resume_id
                            
                except Exception:
                    continue
            
            return None
            
        except Exception as e:
            logger.debug(f"Timestamp-based strategy failed: {e}")
            return None
    
    async def _get_resume_from_max_id(self) -> Optional[str]:
        """Strategy 4: Original max _id method (fallback)"""
        try:
            # Get max _id from target database
            max_doc = await self.target_client.collection.find_one(sort=[("_id", -1)])
            
            if not max_doc:
                return None
            
            max_id = str(max_doc["_id"])
            logger.info(f"Target database max ID: {max_id}")
            
            # Validate that this ID exists in source database
            if not bson.ObjectId.is_valid(max_id):
                logger.warning(f"Invalid ObjectId format from target database: {max_id}")
                return None
                
            source_doc = await self.source_client.collection.find_one({"_id": bson.ObjectId(max_id)})
            if not source_doc:
                logger.warning("Max ID from target not found in source")
                return None
            
            logger.info("✅ Max _id resume point validated")
            return max_id
            
        except Exception as e:
            logger.debug(f"Max _id strategy failed: {e}")
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
