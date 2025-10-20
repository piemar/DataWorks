#!/usr/bin/env python3
"""
Volvo Service Orders Data Generator - Framework Version
Enterprise-ready data generation using the Volvo Data Framework
"""
import asyncio
import logging
import sys
from pathlib import Path

# Add framework to path
sys.path.insert(0, str(Path(__file__).parent))

from framework import (
    ConfigManager,
    create_generation_engine,
    GeneratorType
)
from framework.generators.volvo_generator import VolvoServiceOrderGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data_generation.log')
    ]
)
logger = logging.getLogger(__name__)

async def main():
    """Main function for data generation"""
    try:
        logger.info("🚀 Starting Volvo Service Orders Data Generation (Framework Version)")
        
        # Load configuration
        config_manager = ConfigManager("FRAMEWORK")
        config = config_manager.load_config()
        
        logger.info(f"Configuration loaded for {config.environment.value} environment")
        logger.info(f"Target documents: {config.data_generation.total_documents:,}")
        logger.info(f"Write workers: {config.workers.write_workers}")
        logger.info(f"Batch size: {config.source_database.batch_size}")
        
        # Create generation engine
        engine = create_generation_engine(config)
        
        # Register Volvo generator
        volvo_generator = VolvoServiceOrderGenerator()
        engine.register_generator(volvo_generator)
        
        # Initialize engine
        if not await engine.initialize():
            logger.error("Failed to initialize generation engine")
            return 1
        
        # Progress callback
        async def progress_callback(stats):
            logger.info(f"Progress: {stats['documents_generated']:,} docs generated at {stats['generation_rate']:.0f} docs/s")
        
        # Generate data
        result = await engine.generate_data(
            generator_type=GeneratorType.SERVICE_ORDER,
            total_documents=config.data_generation.total_documents,
            progress_callback=progress_callback
        )
        
        # Print final results
        logger.info("🎉 Data generation completed!")
        logger.info(f"📊 Final Results:")
        logger.info(f"   • Documents generated: {result['total_documents_written']:,}")
        logger.info(f"   • Average rate: {result['average_rate']:.0f} docs/s")
        logger.info(f"   • Database performance: {result['database_performance']}")
        logger.info(f"   • Framework metrics: {result['framework_metrics']}")
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Data generation failed: {e}")
        return 1
    
    finally:
        # Cleanup
        if 'engine' in locals():
            await engine.cleanup()

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
