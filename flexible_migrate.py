#!/usr/bin/env python3
"""
Flexible Migration Script - Framework Version
Supports default strategy, builtin strategies, and custom strategies
"""
import asyncio
import logging
import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add framework to path
sys.path.insert(0, str(Path(__file__).parent))

from framework import (
    ConfigManager,
    create_migration_engine,
    MigrationStrategyFactory
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('flexible_migration.log')
    ]
)
logger = logging.getLogger(__name__)

async def create_indexes_from_source(source_client, target_client, config):
    """Create indexes from source database in target database"""
    try:
        source_collection = source_client.collection
        target_collection = target_client.collection
        
        # Get all indexes from source collection
        source_indexes = await source_collection.list_indexes().to_list(length=None)
        
        logger.info(f"📋 Found {len(source_indexes)} indexes in source collection")
        
        created_count = 0
        skipped_count = 0
        
        for index_info in source_indexes:
            # Skip the default _id index
            if index_info['name'] == '_id_':
                skipped_count += 1
                continue
            
            try:
                # Check if index already exists in target
                target_indexes = await target_collection.list_indexes().to_list(length=None)
                existing_names = [idx['name'] for idx in target_indexes]
                
                if index_info['name'] in existing_names:
                    logger.debug(f"   ⏭️  Skipping existing index: {index_info['name']}")
                    skipped_count += 1
                    continue
                
                # Create the index in target
                index_spec = index_info['key']
                index_options = {}
                
                # Copy relevant options
                if 'unique' in index_info:
                    index_options['unique'] = index_info['unique']
                if 'sparse' in index_info:
                    index_options['sparse'] = index_info['sparse']
                if 'background' in index_info:
                    index_options['background'] = index_info['background']
                
                await target_collection.create_index(
                    list(index_spec.items()),
                    name=index_info['name'],
                    **index_options
                )
                
                logger.info(f"   ✅ Created index: {index_info['name']} on {list(index_spec.keys())}")
                created_count += 1
                
            except Exception as e:
                logger.warning(f"   ⚠️  Failed to create index {index_info['name']}: {e}")
        
        logger.info(f"🔧 Index creation completed: {created_count} created, {skipped_count} skipped")
        
    except Exception as e:
        logger.error(f"❌ Failed to create indexes: {e}")
        raise

async def hide_indexes(target_client, config):
    """Temporarily hide indexes for better migration performance"""
    try:
        target_collection = target_client.collection
        
        # Get all indexes except _id
        indexes = await target_collection.list_indexes().to_list(length=None)
        non_id_indexes = [idx for idx in indexes if idx['name'] != '_id_']
        
        if not non_id_indexes:
            logger.info("   ℹ️  No indexes to hide")
            return
        
        logger.info(f"   🔍 Hiding {len(non_id_indexes)} indexes")
        
        # Store index information for later restoration
        hidden_indexes = []
        
        for index_info in non_id_indexes:
            try:
                # Drop the index
                await target_collection.drop_index(index_info['name'])
                
                # Store index info for restoration
                hidden_indexes.append({
                    'name': index_info['name'],
                    'key': index_info['key'],
                    'options': {k: v for k, v in index_info.items() 
                              if k not in ['name', 'key', 'v']}
                })
                
                logger.debug(f"   ⚡ Hidden index: {index_info['name']}")
                
            except Exception as e:
                logger.warning(f"   ⚠️  Failed to hide index {index_info['name']}: {e}")
        
        # Store hidden indexes info in a temporary collection for restoration
        if hidden_indexes:
            temp_collection = target_client.client[config.target_database.database_name]['_migration_hidden_indexes']
            await temp_collection.insert_one({
                'collection_name': config.target_database.collection_name,
                'hidden_indexes': hidden_indexes,
                'hidden_at': datetime.utcnow().isoformat()
            })
        
        logger.info(f"⚡ Successfully hidden {len(hidden_indexes)} indexes")
        
    except Exception as e:
        logger.error(f"❌ Failed to hide indexes: {e}")
        raise

async def restore_indexes(target_client, config):
    """Restore previously hidden indexes"""
    try:
        target_collection = target_client.collection
        temp_collection = target_client.client[config.target_database.database_name]['_migration_hidden_indexes']
        
        # Find the hidden indexes record
        hidden_record = await temp_collection.find_one({
            'collection_name': config.target_database.collection_name
        })
        
        if not hidden_record:
            logger.info("   ℹ️  No hidden indexes found to restore")
            return
        
        hidden_indexes = hidden_record['hidden_indexes']
        logger.info(f"   🔄 Restoring {len(hidden_indexes)} indexes")
        
        restored_count = 0
        
        for index_info in hidden_indexes:
            try:
                # Recreate the index
                index_spec = list(index_info['key'].items())
                index_options = index_info['options'].copy()
                
                await target_collection.create_index(
                    index_spec,
                    name=index_info['name'],
                    **index_options
                )
                
                logger.debug(f"   ✅ Restored index: {index_info['name']}")
                restored_count += 1
                
            except Exception as e:
                logger.warning(f"   ⚠️  Failed to restore index {index_info['name']}: {e}")
        
        # Clean up the temporary record
        await temp_collection.delete_one({'_id': hidden_record['_id']})
        
        logger.info(f"🔄 Successfully restored {restored_count} indexes")
        
    except Exception as e:
        logger.error(f"❌ Failed to restore indexes: {e}")
        raise

async def main():
    """Main function for flexible migration"""
    parser = argparse.ArgumentParser(description='Flexible Migration Script')
    parser.add_argument('--strategy', '-s', default='default', 
                       help='Migration strategy: "default" or path to custom strategy file')
    parser.add_argument('--config', '-c', default='.env_local',
                       help='Configuration file path')
    parser.add_argument('--resume-field', '-r', default='_id',
                       help='Field to use for resume point (default: _id)')
    parser.add_argument('--list-strategies', '-l', action='store_true',
                       help='List available strategies and exit')
    parser.add_argument('--validation', action='store_true',
                       help='Enable document validation (for custom strategies)')
    parser.add_argument('--enrichment', action='store_true',
                       help='Enable document enrichment (for custom strategies)')
    parser.add_argument('--cleanup', action='store_true',
                       help='Enable document cleanup (for custom strategies)')
    parser.add_argument('--create-indexes', action='store_true',
                       help='Create indexes from source database in target database before migration')
    parser.add_argument('--disable-indexes', action='store_true',
                       help='Temporarily hide indexes during migration for better performance')
    
    args = parser.parse_args()
    
    try:
        # List strategies if requested
        if args.list_strategies:
            factory = MigrationStrategyFactory()
            strategies = factory.list_available_strategies()
            
            print("🚀 Available Migration Strategies:")
            print()
            
            print("📁 Builtin Strategies:")
            for strategy in strategies['builtin']:
                print(f"   • {strategy}")
            
            print()
            print("🐍 Custom Strategy Files:")
            for custom_strategy in strategies['custom_files']:
                print(f"   • {custom_strategy}")
            
            print()
            print("📖 Example Usage:")
            print("   python flexible_migrate.py --strategy default")
            print("   python flexible_migrate.py --strategy user_defined/strategies/volvo_strategy.py")
            print("   python flexible_migrate.py --strategy strategies/examples/hello_world_strategy.py")
            print("   python flexible_migrate.py --strategy strategies/examples/advanced_strategy.py --validation --enrichment")
            print("   python flexible_migrate.py --strategy default --create-indexes")
            print("   python flexible_migrate.py --strategy default --disable-indexes")
            print("   python flexible_migrate.py --strategy default --create-indexes --disable-indexes")
            
            return 0
        
        logger.info("🚀 Starting Flexible Migration")
        logger.info(f"Strategy: {args.strategy}")
        logger.info(f"Resume field: {args.resume_field}")
        
        # Load configuration
        config_manager = ConfigManager("FRAMEWORK")
        config = config_manager.load_config(args.config)
        
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
        
        # Handle index operations
        if args.create_indexes:
            logger.info("🔧 Creating indexes from source database...")
            await create_indexes_from_source(engine.source_client, engine.target_client, config)
        
        if args.disable_indexes:
            logger.info("⚡ Temporarily hiding indexes for better migration performance...")
            await hide_indexes(engine.target_client, config)
        
        # Create strategy using factory
        factory = MigrationStrategyFactory()
        strategy_config = {
            'resume_field': args.resume_field,
            'validation_enabled': args.validation,
            'enrichment_enabled': args.enrichment,
            'cleanup_enabled': args.cleanup
        }
        
        migration_strategy = factory.create_strategy(
            strategy_name=args.strategy,
            source_client=engine.source_client,
            target_client=engine.target_client,
            config=strategy_config
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
        
        # Print strategy-specific info
        if hasattr(migration_strategy, 'get_migration_stats'):
            strategy_stats = migration_strategy.get_migration_stats()
            if 'strategy_metrics' in strategy_stats:
                metrics = strategy_stats['strategy_metrics']
                logger.info(f"   • Strategy type: {metrics.get('strategy_type', 'unknown')}")
                logger.info(f"   • Resume field: {metrics.get('resume_field', 'unknown')}")
                logger.info(f"   • Transform enabled: {metrics.get('transform_enabled', False)}")
        
        # Restore indexes if they were disabled
        if args.disable_indexes:
            logger.info("🔄 Restoring indexes after migration...")
            await restore_indexes(engine.target_client, config)
        
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
