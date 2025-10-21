# 🚀 DataWorks Framework

## Enterprise-Ready Data Generation and Migration Framework

DataWorks is a comprehensive, enterprise-ready solution for data generation and migration operations. Built with modularity, scalability, and maintainability in mind, it provides a robust foundation for handling large-scale data operations (100GB+) with maximum performance and reliability. DataWorks handles migrations that existing tools like Compass, mongorestore, mongodump, mongoexport, mongoimport, and LiveMirror cannot support.

## 🎯 Key Features

- **🏗️ Modular Architecture**: Pluggable components for easy customization
- **⚡ High Performance**: Parallel processing with optimized connection pooling
- **📊 Real-time Monitoring**: Comprehensive metrics and performance tracking
- **🔄 Resumable Operations**: Checkpoint-based recovery for long-running processes
- **🛡️ Enterprise Ready**: Error handling, logging, and configuration management
- **🔧 Extensible**: Easy to add new generators and migration strategies
- **📈 Scalable**: Configurable workers and batch processing
- **🔐 Security First**: No hardcoded secrets, environment-based configuration

## 🏗️ Architecture Overview

```
dataworks/
├── core/                    # Core database operations
│   └── database.py         # Base database clients (CosmosDB, Atlas)
├── config/                 # Configuration management
│   └── manager.py         # Configuration system with environment variables
├── generators/             # Data generation framework
│   ├── engine.py          # Generation engine
│   ├── factory.py         # Generator factory
│   └── json_sample_generator.py # JSON-based generator
├── migrations/             # Migration framework
│   ├── engine.py          # Migration engine with checkpoint support
│   ├── factory.py         # Strategy factory
│   └── default_strategy.py # Default migration strategy
└── monitoring/             # Monitoring and metrics
    └── metrics.py         # Metrics collection and performance tracking

user_defined/               # User-specific implementations
├── generators/             # Custom data generators
│   └── volvo_generator.py # Volvo service order generator
├── strategies/             # Custom migration strategies
│   └── volvo_strategy.py  # Volvo migration strategy
└── templates/              # JSON templates for generation
    └── service_order_template.json
```

## 🚀 Quick Start

### 1. Installation

```bash
# Clone the repository
git clone <repository-url>
cd volvo-vida

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

```bash
# Copy DataWorks configuration
cp .env_local.example .env_local

# Edit with your credentials
nano .env_local
```

### 3. Data Generation

```bash
# Generate data using DataWorks
python flexible_generator.py --source user_defined/generators/volvo_generator.py --total 1000
```

### 4. Data Migration

```bash
# Migrate data using DataWorks
python flexible_migrate.py --strategy user_defined/strategies/volvo_strategy.py
```

## 📋 Configuration

DataWorks uses a comprehensive configuration system with environment variables:

### Database Configuration

```env
# Source Database (Cosmos DB)
FRAMEWORK_SOURCE_DB_CONNECTION_STRING="mongodb://..."
FRAMEWORK_SOURCE_DB_NAME=volvo-service-orders
FRAMEWORK_SOURCE_DB_COLLECTION=serviceorders

# Target Database (Atlas)
FRAMEWORK_TARGET_DB_CONNECTION_STRING="mongodb+srv://..."
FRAMEWORK_TARGET_DB_NAME=volvo-service-orders
FRAMEWORK_TARGET_DB_COLLECTION=serviceorders
```

### Performance Configuration

```env
# Workers
FRAMEWORK_WRITE_WORKERS=5
FRAMEWORK_GENERATION_WORKERS=3
FRAMEWORK_MAX_WORKERS=10

# Batch Processing
FRAMEWORK_BATCH_AGGREGATION_SIZE=5
FRAMEWORK_BATCH_AGGREGATION_TIMEOUT_MS=200
FRAMEWORK_SOURCE_DB_BATCH_SIZE=1000
```

## 🔧 Framework Components

### Core Database Client

```python
from framework import create_database_client, DatabaseConfig, DatabaseType

# Create database client
config = DatabaseConfig(
    connection_string="mongodb://...",
    database_name="volvo-service-orders",
    collection_name="serviceorders",
    db_type=DatabaseType.COSMOS_DB
)

client = create_database_client(config)
await client.connect()
```

### Data Generation Engine

```python
from framework import create_generation_engine, GeneratorType
from framework.generators.volvo_generator import VolvoServiceOrderGenerator

# Create engine
engine = create_generation_engine(config)

# Register generator
generator = VolvoServiceOrderGenerator()
engine.register_generator(generator)

# Generate data
result = await engine.generate_data(
    generator_type=GeneratorType.SERVICE_ORDER,
    total_documents=1000000
)
```

### Migration Engine

```python
from framework import create_migration_engine
from framework.migrations.volvo_strategy import VolvoServiceOrderMigrationStrategy

# Create engine
engine = create_migration_engine(config)

# Set strategy
strategy = VolvoServiceOrderMigrationStrategy(
    engine.source_client,
    engine.target_client
)
engine.set_migration_strategy(strategy)

# Execute migration
result = await engine.migrate()
```

### Monitoring and Metrics

```python
from framework import MetricsCollector, PerformanceMonitor

# Create metrics collector
metrics = MetricsCollector()

# Start performance monitoring
monitor = PerformanceMonitor(metrics)
await monitor.start_monitoring()

# Get performance summary
summary = metrics.get_summary()
```

## 🎨 Customization

### Creating Custom Generators

```python
from framework.generators.engine import BaseDataGenerator, GeneratorType

class CustomGenerator(BaseDataGenerator):
    def __init__(self):
        super().__init__(GeneratorType.CUSTOM)
    
    def generate_document(self, document_id=None):
        return {
            "_id": document_id or str(uuid.uuid4()),
            "custom_field": "custom_value",
            "timestamp": datetime.utcnow().isoformat()
        }
```

### Creating Custom Migration Strategies

```python
from framework.migrations.engine import BaseMigrationStrategy

class CustomMigrationStrategy(BaseMigrationStrategy):
    async def read_batch(self, batch_size, resume_from=None):
        # Custom batch reading logic
        pass
    
    async def get_resume_point(self):
        # Custom resume point logic
        pass
```

## 📊 Performance Optimization

### Connection Pooling

```python
config = DatabaseConfig(
    max_pool_size=100,
    min_pool_size=10,
    max_idle_time_ms=300000,
    warmup_connections=50
)
```

### Batch Processing

```python
config = DatabaseConfig(
    batch_size=1000
)

# Framework settings
FRAMEWORK_BATCH_AGGREGATION_SIZE=5
FRAMEWORK_BATCH_AGGREGATION_TIMEOUT_MS=200
```

### Worker Configuration

```python
# Data generation
FRAMEWORK_WRITE_WORKERS=5
FRAMEWORK_GENERATION_WORKERS=3

# Migration
FRAMEWORK_MAX_INSERT_WORKERS=20
FRAMEWORK_PARALLEL_CURSORS=4
```

## 🔍 Monitoring and Alerting

### Real-time Metrics

```python
# Get current performance
snapshot = metrics.get_current_performance()
print(f"Documents/sec: {snapshot.documents_per_second}")
print(f"Error rate: {snapshot.error_rate}")
print(f"Memory usage: {snapshot.memory_usage_mb}MB")
```

### Performance Alerts

```python
# Add alert callback
def alert_callback(alert):
    print(f"ALERT: {alert['message']}")

metrics.add_alert_callback(alert_callback)
```

### Export Metrics

```python
# Export as JSON
json_metrics = metrics.export_metrics("json")

# Export as CSV
csv_metrics = metrics.export_metrics("csv")
```

## 🛠️ Development

### Adding New Components

1. **Create component class** inheriting from appropriate base class
2. **Implement required methods**
3. **Add to framework imports**
4. **Create factory functions**
5. **Add documentation**

### Testing

```bash
# Run framework tests
python -m pytest framework/tests/

# Test specific component
python -m pytest framework/tests/test_database.py
```

## 📚 API Reference

### Core Classes

- `BaseDatabaseClient`: Abstract database client
- `CosmosDBClient`: Cosmos DB implementation
- `MongoDBAtlasClient`: Atlas implementation
- `ConfigManager`: Configuration management
- `FrameworkConfig`: Main configuration class

### Generation Classes

- `DataGenerationEngine`: Main generation engine
- `BaseDataGenerator`: Abstract generator
- `VolvoServiceOrderGenerator`: Volvo-specific generator

### Migration Classes

- `MigrationEngine`: Main migration engine
- `BaseMigrationStrategy`: Abstract strategy
- `VolvoServiceOrderMigrationStrategy`: Volvo-specific strategy

### Monitoring Classes

- `MetricsCollector`: Metrics collection
- `PerformanceMonitor`: Performance monitoring
- `OperationMetrics`: Operation tracking

## 🚨 Error Handling

The framework provides comprehensive error handling:

- **Connection errors**: Automatic retry with exponential backoff
- **Batch failures**: Graceful degradation and error reporting
- **Resource cleanup**: Proper cleanup on errors
- **Logging**: Detailed error logging and stack traces

## 🔒 Security

- **🔐 Environment Variables**: All sensitive data stored in environment variables
- **📁 .env_local**: Local configuration file (not tracked by git)
- **🚫 No Hardcoded Secrets**: Framework designed to prevent credential exposure
- **🔄 Connection Strings**: Secure connection string handling with proper escaping
- **📋 Logging**: No sensitive data logged, sanitized output
- **✅ Input Validation**: Comprehensive input validation and sanitization
- **⚠️ Git History**: Files with secrets have been removed from git history
- **🛡️ Best Practices**: Follows enterprise security standards

## 📈 Performance Benchmarks

Typical performance with optimized settings:

- **Data Generation**: 10,000-20,000 documents/second
- **Migration**: 4,000-8,000 documents/second (conservative settings)
- **Memory Usage**: < 1GB for 1M documents
- **CPU Usage**: < 80% on modern hardware
- **Connection Pool**: Conservative settings prevent broken pipe errors
- **Batch Size**: 15,000 documents per batch (optimal for stability)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Update documentation
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For support and questions:

- Create an issue in the repository
- Check the documentation
- Review the examples
- Contact the development team
