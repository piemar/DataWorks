#!/usr/bin/env python3
"""
Volvo Service Orders Migration - Framework Version
Enterprise-ready data migration using the Volvo Data Framework
"""
import asyncio
import logging
import sys
from pathlib import Path

# Add framework to path
sys.path.insert(0, str(Path(__file__).parent))

from framework import (
    ConfigManager,
    create_migration_engine
)
from framework.migrations.volvo_strategy import VolvoServiceOrderMigrationStrategy

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data_migration.log')
    ]
)
logger = logging.getLogger(__name__)

async def main():
    """Main function for data migration"""
    try:
        logger.info("🚀 Starting Volvo Service Orders Migration (Framework Version)")
        
        # Load configuration
        config_manager = ConfigManager("FRAMEWORK")
        config = config_manager.load_config()
        
        logger.info(f"Configuration loaded for {config.environment.value} environment")
        logger.info(f"Source: {config.source_database.database_name}.{config.source_database.collection_name}")
        logger.info(f"Target: {config.target_database.database_name}.{config.target_database.collection_name}")
        logger.info(f"Write workers: {config.workers.write_workers}")
        logger.info(f"Batch size: {config.source_database.batch_size}")
        
        # Create migration engine
        engine = create_migration_engine(config)
        
        # Initialize engine
        if not await engine.initialize():
            logger.error("Failed to initialize migration engine")
            return 1
        
        # Create and set migration strategy
        migration_strategy = VolvoServiceOrderMigrationStrategy(
            engine.source_client,
            engine.target_client
        )
        engine.set_migration_strategy(migration_strategy)
        
        # Progress callback
        async def progress_callback(stats):
            logger.info(f"Progress: {stats['documents_migrated']:,} docs migrated at {stats['migration_rate']:.0f} docs/s")
        
        # Checkpoint callback
        async def checkpoint_callback(checkpoint):
            logger.info(f"Checkpoint: {checkpoint.documents_migrated:,} documents migrated")
        
        # Execute migration
        result = await engine.migrate(
            progress_callback=progress_callback,
            checkpoint_callback=checkpoint_callback
        )
        
        # Print final results
        logger.info("🎉 Migration completed!")
        logger.info(f"📊 Final Results:")
        logger.info(f"   • Documents migrated: {result['total_documents_migrated']:,}")
        logger.info(f"   • Average rate: {result['average_rate']:.0f} docs/s")
        logger.info(f"   • Source performance: {result['source_performance']}")
        logger.info(f"   • Target performance: {result['target_performance']}")
        logger.info(f"   • Framework metrics: {result['framework_metrics']}")
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        return 1
    
    finally:
        # Cleanup
        if 'engine' in locals():
            await engine.cleanup()

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
