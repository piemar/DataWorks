"""
Volvo Data Framework
Enterprise-ready framework for data generation and migration
"""

__version__ = "1.0.0"
__author__ = "Volvo Data Team"

# Core components
from .core.database import (
    BaseDatabaseClient,
    CosmosDBClient,
    MongoDBAtlasClient,
    DatabaseConfig,
    DatabaseType,
    OperationMetrics,
    create_database_client
)

# Configuration management
from .config.manager import (
    ConfigManager,
    FrameworkConfig,
    DatabaseSettings,
    WorkerSettings,
    PerformanceSettings,
    MonitoringSettings,
    DataGenerationConfig,
    MigrationConfig,
    Environment,
    config_manager
)

# Data generation
from .generators.engine import (
    DataGenerationEngine,
    BaseDataGenerator,
    GeneratorType,
    GenerationBatch,
    create_generation_engine
)
from .generators.json_sample_generator import JSONSampleGenerator
from .generators.factory import GeneratorFactory

# Migration
from .migrations.engine import (
    MigrationEngine,
    BaseMigrationStrategy,
    MigrationStrategy,
    MigrationBatch,
    Checkpoint,
    create_migration_engine
)
from .migrations.default_strategy import DefaultMigrationStrategy
from .migrations.factory import MigrationStrategyFactory

# Monitoring
from .monitoring.metrics import (
    MetricsCollector,
    PerformanceMonitor,
    OperationType,
    MetricType,
    Metric,
    OperationMetrics as MonitoringOperationMetrics,
    PerformanceSnapshot
)

__all__ = [
    # Core
    "BaseDatabaseClient",
    "CosmosDBClient", 
    "MongoDBAtlasClient",
    "DatabaseConfig",
    "DatabaseType",
    "OperationMetrics",
    "create_database_client",
    
    # Configuration
    "ConfigManager",
    "FrameworkConfig",
    "DatabaseSettings",
    "WorkerSettings", 
    "PerformanceSettings",
    "MonitoringSettings",
    "DataGenerationConfig",
    "MigrationConfig",
    "Environment",
    "config_manager",
    
    # Data Generation
    "DataGenerationEngine",
    "BaseDataGenerator",
    "GeneratorType",
    "GenerationBatch",
    "create_generation_engine",
    "JSONSampleGenerator",
    "GeneratorFactory",
    
    # Migration
    "MigrationEngine",
    "BaseMigrationStrategy",
    "MigrationStrategy",
    "MigrationBatch",
    "Checkpoint",
    "create_migration_engine",
    "DefaultMigrationStrategy",
    "MigrationStrategyFactory",
    
    # Monitoring
    "MetricsCollector",
    "PerformanceMonitor",
    "OperationType",
    "MetricType",
    "Metric",
    "MonitoringOperationMetrics",
    "PerformanceSnapshot"
]
