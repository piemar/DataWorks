# 🏗️ DataWorks Framework

**Developer Documentation**

This document provides technical details for developers who want to extend DataWorks or understand its architecture.

---

## 🎯 Framework Overview

DataWorks is built with a modular architecture that separates concerns and provides pluggable components:

```
dataworks/
├── core/                    # Database operations
├── config/                  # Configuration management  
├── generators/              # Data generation engine
├── migrations/              # Migration engine
└── monitoring/              # Performance tracking
```

---

## 🚀 Quick Start for Developers

### 1. Basic Usage

```python
from framework import ConfigManager, create_generation_engine, create_migration_engine

# Load configuration
config_manager = ConfigManager("FRAMEWORK")
config = config_manager.load_config(".env_local")

# Data Generation
engine = create_generation_engine(config)
generator = VolvoServiceOrderGenerator()
engine.register_generator(generator)
result = await engine.generate_data(GeneratorType.SERVICE_ORDER, 1000)

# Data Migration  
engine = create_migration_engine(config)
strategy = VolvoServiceOrderMigrationStrategy(source_client, target_client)
engine.set_migration_strategy(strategy)
result = await engine.migrate()
```

### 2. Configuration System

DataWorks uses profile-based configuration:

```python
# Available profiles
profiles = {
    "dev": "Development and testing",
    "data-migration": "Production migration", 
    "data-ingest": "High-speed generation"
}

# Load with profile
config = config_manager.load_config(".env_local", profile="data-migration")
```

---

## 🔧 Core Components

### Database Clients

```python
from framework.core.database import create_database_client, DatabaseConfig, DatabaseType

# Create database client
config = DatabaseConfig(
    connection_string="mongodb://...",
    database_name="my-db",
    collection_name="my-collection",
    db_type=DatabaseType.COSMOS_DB
)

client = create_database_client(config)
await client.connect()
```

### Data Generation

```python
from framework.generators.engine import BaseDataGenerator, GeneratorType

class MyGenerator(BaseDataGenerator):
    def __init__(self):
        super().__init__(GeneratorType.CUSTOM)
    
    def generate_document(self, document_id=None):
        return {
            "_id": document_id or str(uuid.uuid4()),
            "data": "my-generated-data",
            "timestamp": datetime.utcnow().isoformat()
        }
```

### Migration Strategies

```python
from framework.migrations.engine import BaseMigrationStrategy

class MyStrategy(BaseMigrationStrategy):
    async def read_batch(self, batch_size, resume_from=None):
        # Your batch reading logic
        pass
    
    async def get_resume_point(self):
        # Your resume logic
        pass
```

---

## 📊 Performance Configuration

### Connection Pooling

```python
config = DatabaseConfig(
    max_pool_size=100,        # Maximum connections
    min_pool_size=10,         # Minimum connections
    max_idle_time_ms=300000,  # Connection timeout
    warmup_connections=50     # Pre-warm connections
)
```

### Batch Processing

```python
# Environment variables
FRAMEWORK_BATCH_AGGREGATION_SIZE=5
FRAMEWORK_BATCH_AGGREGATION_TIMEOUT_MS=200
FRAMEWORK_SOURCE_DB_BATCH_SIZE=1000
```

### Worker Configuration

```python
# Data generation workers
FRAMEWORK_WRITE_WORKERS=5
FRAMEWORK_GENERATION_WORKERS=3

# Migration workers  
FRAMEWORK_MAX_INSERT_WORKERS=20
FRAMEWORK_PARALLEL_CURSORS=4
```

---

## 🔍 Monitoring & Metrics

### Real-time Metrics

```python
from framework.monitoring.metrics import MetricsCollector

metrics = MetricsCollector()

# Get current performance
snapshot = metrics.get_current_performance()
print(f"Documents/sec: {snapshot.documents_per_second}")
print(f"Error rate: {snapshot.error_rate}")
```

### Performance Alerts

```python
def alert_callback(alert):
    print(f"ALERT: {alert['message']}")

metrics.add_alert_callback(alert_callback)
```

---

## 🎨 Customization Examples

### Custom Generator

```python
class UserProfileGenerator(BaseDataGenerator):
    def __init__(self):
        super().__init__(GeneratorType.USER_PROFILE)
    
    def generate_document(self, document_id=None):
        return {
            "_id": document_id or str(uuid.uuid4()),
            "name": fake.name(),
            "email": fake.email(),
            "created_at": datetime.utcnow().isoformat()
        }
```

### Custom Migration Strategy

```python
class DateBasedStrategy(BaseMigrationStrategy):
    def __init__(self, source_client, target_client):
        super().__init__(source_client, target_client, resume_field="created_at")
    
    def transform_document(self, doc):
        # Add migration metadata
        doc['migrated_at'] = datetime.utcnow().isoformat()
        return doc
```

---

## 🛠️ Development

### Adding New Components

1. **Create component class** inheriting from appropriate base class
2. **Implement required methods**
3. **Add to framework imports**
4. **Create factory functions**
5. **Add documentation**

### Testing

```bash
# Test specific component
python -c "from framework.generators.factory import GeneratorFactory; print('OK')"

# Test data generation
python flexible_generator.py --source user_defined/templates/service_orders/service_order_template.json --total 5
```

---

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
- `GeneratorFactory`: Generator factory

### Migration Classes

- `MigrationEngine`: Main migration engine
- `BaseMigrationStrategy`: Abstract strategy
- `MigrationStrategyFactory`: Strategy factory

### Monitoring Classes

- `MetricsCollector`: Metrics collection
- `PerformanceMonitor`: Performance monitoring
- `OperationMetrics`: Operation tracking

---

## 🚨 Error Handling

DataWorks provides comprehensive error handling:

- **Connection errors**: Automatic retry with exponential backoff
- **Batch failures**: Graceful degradation and error reporting
- **Resource cleanup**: Proper cleanup on errors
- **Logging**: Detailed error logging and stack traces

---

## 🔒 Security

- **🔐 Environment Variables**: All sensitive data stored in environment variables
- **📁 .env_local**: Local configuration file (not tracked by git)
- **🚫 No Hardcoded Secrets**: Framework designed to prevent credential exposure
- **🔄 Connection Strings**: Secure connection string handling with proper escaping
- **📋 Logging**: No sensitive data logged, sanitized output

---

## 📈 Performance Benchmarks

Typical performance with optimized settings:

- **Data Generation**: 10,000-20,000 documents/second
- **Migration**: 4,000-8,000 documents/second (conservative settings)
- **Memory Usage**: < 1GB for 1M documents
- **CPU Usage**: < 80% on modern hardware

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Update documentation
5. Submit a pull request

---

## 📄 License

This project is licensed under the MIT License.

---

**Need help?** Check the main [README.md](README.md) for user documentation or create an issue for technical questions.