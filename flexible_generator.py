#!/usr/bin/env python3
"""
DataWorks Flexible Data Generator
Supports JSON samples, Python generators, and builtin generators
"""
import asyncio
import logging
import sys
import argparse
import os
from pathlib import Path

# Add framework to path
sys.path.insert(0, str(Path(__file__).parent))

from framework import (
    ConfigManager,
    create_generation_engine,
    GeneratorType,
    GeneratorFactory
)

# Configure logging
logging.basicConfig(
    level=logging.WARNING,  # Reduced from INFO to WARNING to prevent interference with progress bar
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('flexible_data_generation.log')
    ]
)
logger = logging.getLogger(__name__)

async def main():
    """Main function for flexible data generation"""
    parser = argparse.ArgumentParser(description='Flexible Data Generator')
    parser.add_argument('--source', '-s', 
                       help='Source: JSON sample file, Python generator file, or builtin generator name')
    parser.add_argument('--type', '-t', default='auto', 
                       choices=['auto', 'json', 'python', 'builtin'],
                       help='Generator type (default: auto-detect based on file extension)')
    parser.add_argument('--domain', '-d', 
                       help='Domain name for builtin generators')
    parser.add_argument('--total', '-n', type=int, default=1000000,
                       help='Total number of documents to generate')
    parser.add_argument('--config', '-c', default='.env_local',
                       help='Configuration file path')
    parser.add_argument('--list-generators', '-l', action='store_true',
                       help='List available generators and exit')
    parser.add_argument('--list-templates', action='store_true',
                       help='List available JSON templates and exit')
    parser.add_argument('--resume', action='store_true',
                       help='Resume generation from last checkpoint')
    parser.add_argument('--force-from-start', action='store_true',
                       help='Force start from beginning, ignoring any checkpoint')
    parser.add_argument('--auto-scaling', action='store_true',
                       help='Enable intelligent auto-scaling (adaptive workers and batch sizes)')
    parser.add_argument('--auto-scaling-profile', default='balanced',
                       choices=['conservative', 'balanced', 'aggressive'],
                       help='Auto-scaling profile (default: balanced)')
    
    args = parser.parse_args()
    
    try:
        # Validate arguments
        if not args.list_generators and not args.list_templates and not args.source:
            parser.error("--source is required unless using --list-generators or --list-templates")
        
        # List generators if requested
        if args.list_generators:
            factory = GeneratorFactory()
            generators = factory.list_available_generators()
            
            print("🚀 Available Generators:")
            print()
            
            print("📁 Builtin Generators:")
            for gen in generators['builtin']:
                print(f"   • {gen}")
            
            print()
            print("📄 JSON Sample Files:")
            for sample in generators['json_samples']:
                print(f"   • {sample}")
            
            print()
            print("🐍 Python Generator Files:")
            for py_gen in generators['python_generators']:
                print(f"   • {py_gen}")
            
            return 0
        
        # List templates if requested
        if args.list_templates:
            templates_dir = Path("user_defined/templates")
            print("📄 Available JSON Templates:")
            print()
            
            if templates_dir.exists():
                for domain_dir in templates_dir.iterdir():
                    if domain_dir.is_dir():
                        print(f"📁 {domain_dir.name}:")
                        for template_file in domain_dir.glob("*.json"):
                            print(f"   • {template_file}")
                        print()
            else:
                print("   No templates directory found")
            
            print("📖 Example Usage:")
            print("   python flexible_generator.py --source user_defined/templates/service_orders/service_order_template.json --total 100000")
            print("   python flexible_generator.py --source user_defined/generators/volvo_generator.py --total 500000")
            print("   python flexible_generator.py --source user_defined/templates/service_orders/service_order_template.json --total 1000000 --auto-scaling")
            print("   python flexible_generator.py --source user_defined/templates/service_orders/service_order_template.json --total 1000000 --auto-scaling --auto-scaling-profile aggressive")
            print("   python flexible_generator.py --source user_defined/templates/service_orders/service_order_template.json --total 1000000 --resume")
            print("   python flexible_generator.py --source user_defined/templates/service_orders/service_order_template.json --total 1000000 --force-from-start")
            
            return 0
        
        logger.info("🚀 Starting Flexible Data Generation")
        logger.info(f"Source: {args.source}")
        logger.info(f"Type: {args.type}")
        logger.info(f"Total documents: {args.total:,}")
        
        # Load configuration
        config_manager = ConfigManager("FRAMEWORK")
        config = config_manager.load_config(args.config)
        
        logger.info(f"Configuration loaded for {config.environment.value} environment")
        logger.info(f"Write workers: {config.workers.write_workers}")
        logger.info(f"Batch size: {config.source_database.batch_size}")
        
        # Create generator using factory
        factory = GeneratorFactory()
        generator = factory.create_generator(
            source=args.source,
            generator_type=args.type,
            domain=args.domain
        )
        
        # Create generation engine
        engine = create_generation_engine(config)
        
        # Initialize auto-scaling if enabled
        if args.auto_scaling:
            logger.info(f"🚀 Enabling intelligent auto-scaling for generation with profile: {args.auto_scaling_profile}")
            os.environ["AUTO_SCALING_PROFILE"] = args.auto_scaling_profile
            await engine.initialize_auto_scaling(enabled=True)
        else:
            await engine.initialize_auto_scaling(enabled=False)
        
        # Register generator
        engine.register_generator(generator)
        
        # Initialize engine
        if not await engine.initialize():
            logger.error("Failed to initialize generation engine")
            return 1
        
        # Progress callback
        async def progress_callback(stats):
            logger.info(f"Progress: {stats['documents_generated']:,} docs generated at {stats['generation_rate']:.0f} docs/s")
        
        # Generate data using the registered generator type
        generator_type = generator.generator_type
        result = await engine.generate_data(
            generator_type=generator_type,
            total_documents=args.total,
            progress_callback=progress_callback
        )
        
        # Print final results
        logger.info("🎉 Data generation completed!")
        logger.info(f"📊 Final Results:")
        logger.info(f"   • Documents generated: {result['total_documents_written']:,}")
        logger.info(f"   • Average rate: {result['average_rate']:.0f} docs/s")
        logger.info(f"   • Database performance: {result['database_performance']}")
        logger.info(f"   • Framework metrics: {result['framework_metrics']}")
        
        # Print generator-specific info
        if hasattr(generator, 'get_generation_stats'):
            gen_stats = generator.get_generation_stats()
            if 'schema_version' in gen_stats:
                logger.info(f"   • Schema version: {gen_stats['schema_version']}")
            if 'sample_source' in gen_stats:
                logger.info(f"   • Sample source: {gen_stats['sample_source']}")
        
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
