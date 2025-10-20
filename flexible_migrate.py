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
