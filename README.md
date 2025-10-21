# 🚀 Volvo Data Framework

## Enterprise-Ready Data Generation and Migration Solution

A comprehensive, enterprise-ready framework for data generation and migration operations, specifically designed for Volvo service orders data. Built with modularity, scalability, and maintainability in mind.

## 🎯 Project Purpose

This project provides a complete data migration solution for Volvo service orders, designed to handle large-scale data operations (60M+ documents) with maximum speed and reliability. It includes:

- **🏗️ Modular Framework**: Pluggable components for easy customization and extension
- **⚡ High-Performance Migration**: Optimized for maximum speed with parallel processing
- **🔄 Resumable Operations**: Checkpoint-based migration that can resume from interruptions
- **📊 Real-time Monitoring**: Track progress with detailed performance metrics
- **🛡️ Enterprise Ready**: Error handling, logging, and configuration management
- **🔧 Extensible**: Easy to add new generators and migration strategies

## 🚀 Key Features

- ⚡ **Maximum Speed Optimization**: Unacknowledged writes, direct batch inserts, optimized connection pools
- 🔄 **Resumable Migration**: Automatic checkpoint system for large migrations
- 📊 **Real-time Progress**: Live progress bars with RU consumption and performance metrics
- 🛡️ **Error Recovery**: Intelligent retry logic with exponential backoff
- 🎯 **Production Ready**: Handles 60M+ documents with optimized settings
- 🏗️ **Modular Architecture**: Clean separation of concerns with pluggable components
- 🔧 **Framework-Based**: Reusable components for different data types and migration scenarios

## 📋 Prerequisites

- **Python 3.8+**
- **Azure Cosmos DB** account with MongoDB API
- **MongoDB Atlas** cluster (M10+ recommended for large datasets)
- **Network connectivity** to both databases
- **Sufficient resources**: 8GB+ RAM, stable internet connection

## 🛠️ Quick Start

### 1. Clone and Setup

```bash
# Clone the repository
git clone <repository-url>
cd volvo-vida

# Install dependencies
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

### 2. Configure Environment

```bash
# Copy the template
cp .env_local.example .env_local

# Edit with your credentials
nano .env_local
```

**🎯 NEW: Profile-Based Configuration**

Instead of managing 100+ configuration parameters, you now only need to:

1. **Choose a profile** based on your use case:
   - `data-ingest`: High-speed data generation (40k+ docs/s)
   - `data-migration`: High-speed migration with stability (4k-8k docs/s)  
   - `dev`: Development and testing (minimal resources)

2. **Set your database connection strings**

3. **Optionally override specific settings**

**Required Configuration:**
```env
# Profile Selection (choose one)
FRAMEWORK_PROFILE=data-migration

# Database Configuration (REQUIRED)
GEN_DB_CONNECTION_STRING="mongodb://your-account:YOUR_PASSWORD@your-account.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@your-account@"
GEN_DB_NAME=volvo-service-orders
GEN_DB_COLLECTION=serviceorders

MIG_TARGET_DB_CONNECTION_STRING="mongodb+srv://your-username:YOUR_PASSWORD@your-cluster.mongodb.net/?retryWrites=false&w=0&appName=YourCluster"
MIG_TARGET_DB_NAME=volvo-service-orders
MIG_TARGET_DB_COLLECTION=serviceorders

# Optional Overrides (uncomment to customize)
# MIG_BATCH_SIZE=20000
# GEN_BATCH_SIZE=50000
```

### 3. Generate Data (Optional)

```bash
# Generate test data using flexible generator
python flexible_generator.py --source user_defined/generators/volvo_generator.py --total 1000
```

### 4. Run Migration

```bash
# Start migration using flexible migrate
python flexible_migrate.py --strategy user_defined/strategies/volvo_strategy.py
```

## 📊 Performance Optimization

### Current Optimized Settings

The project is configured for **maximum speed** with these optimizations:

- **Write Concern**: Unacknowledged writes (`w=0`) for fire-and-forget performance
- **Batch Sizes**: 2000-4000 documents per batch
- **Concurrent Workers**: 12 workers for parallel processing
- **Connection Pools**: 100-200 connections for high concurrency
- **Error Handling**: Minimal retry logic (2 attempts, 0.1s delay)

### Expected Performance

- **Data Generation**: 15,000-25,000 documents/second
- **Migration**: 8,000-15,000 documents/second
- **60M Documents**: ~1-2 hours (generation) + ~1-2 hours (migration)

## 🔧 Environment Setup

### Secure Credential Management

The project uses a secure environment variable setup:

- **`.env_local`**: Your actual sensitive credentials (NOT tracked by Git)
- **`.env_local.example`**: Template for other developers (tracked by Git)
- **`config.env`**: Example values only (tracked by Git)

**For Team Members:**
1. Copy `.env_local.example` to `.env_local`
2. Fill in your actual credentials in `.env_local`
3. Never commit `.env_local` to version control

## 📁 Project Structure

```
volvo-vida/
├── 🏗️ Framework Core
│   ├── framework/
│   │   ├── core/                 # Core database operations
│   │   │   └── database.py      # Base database clients
│   │   ├── config/              # Configuration management
│   │   │   └── manager.py       # Configuration system
│   │   ├── generators/          # Data generation framework
│   │   │   ├── engine.py        # Generation engine
│   │   │   ├── factory.py       # Generator factory
│   │   │   └── json_sample_generator.py # JSON-based generator
│   │   ├── migrations/          # Migration framework
│   │   │   ├── engine.py        # Migration engine
│   │   │   ├── factory.py       # Strategy factory
│   │   │   └── default_strategy.py # Default migration strategy
│   │   └── monitoring/          # Monitoring and metrics
│   │       └── metrics.py       # Metrics collection
│   │
│   ├── user_defined/            # User-specific implementations
│   │   ├── generators/          # Custom data generators
│   │   │   └── volvo_generator.py # Volvo service order generator
│   │   ├── strategies/          # Custom migration strategies
│   │   │   └── volvo_strategy.py # Volvo migration strategy
│   │   └── templates/           # JSON templates for generation
│   │       └── service_order_template.json
│   │
│   ├── flexible_migrate.py      # Main migration script
│   └── flexible_generator.py    # Main data generation script
│
├── 🔧 Utility Scripts
│   ├── dump_cosmos.sh           # Cosmos DB dump script
│   └── restore_to_atlas.sh      # Atlas restore script
│
├── ⚙️ Configuration
│   ├── .env_local               # Your credentials (not tracked)
│   ├── .env_local.example       # Template for developers
│   └── requirements.txt         # Python dependencies
│
└── 📚 Documentation
    ├── README.md                # This file
    ├── FRAMEWORK_README.md      # Framework documentation
    └── CUSTOM_STRATEGIES_GUIDE.md # Custom strategy guide
```

## 🚀 Usage Examples

### Data Generation

```bash
# Generate documents using framework
python flexible_generator.py --source user_defined/generators/volvo_generator.py

# Generate specific amount
export GEN_TOTAL_DOCUMENTS=1000000
source venv/bin/activate && FRAMEWORK_PROFILE=dev  python flexible_generator.py --source user_defined/templates/service_orders/service_order_template.json --total 5
```

### Migration

```bash
# Full migration with checkpoint support
source venv/bin/activate && FRAMEWORK_PROFILE=data-migration python flexible_migrate.py --strategy user_defined/strategies/volvo_strategy.py


# Force start from beginning
source venv/bin/activate && FRAMEWORK_PROFILE=data-migration python flexible_migrate.py --strategy user_defined/strategies/volvo_strategy.pypy --force-from-start

# Disable indexes for optimal performance
source venv/bin/activate && FRAMEWORK_PROFILE=data-migration python flexible_migrate.py --strategy user_defined/strategies/volvo_strategy.py --disable-indexes
```

### Index Management

```bash
# Create indexes from source database
source venv/bin/activate && FRAMEWORK_PROFILE=data-migration python flexible_migrate.py --strategy user_defined/strategies/volvo_strategy.py --create-indexes

# Disable indexes for optimal performance during migration
source venv/bin/activate && FRAMEWORK_PROFILE=data-migration python flexible_migrate.py --strategy user_defined/strategies/volvo_strategy.py --disable-indexes
```

## 📈 Monitoring and Progress

### Real-time Progress Bars

Both scripts provide real-time progress monitoring:

- **Document count** and rate (docs/sec)
- **RU consumption** (for Cosmos DB)
- **Throttling detection** (red color when throttling)
- **ETA** and elapsed time
- **Error counts** and retry statistics

### Checkpoint System

The migration automatically saves progress every 50,000 documents:
- **Resume**: Automatic on restart
- **No Duplicates**: Smart resume prevents data duplication

## 🛡️ Error Handling and Recovery

### Automatic Recovery

- **Connection Issues**: Automatic retry with exponential backoff
- **Throttling**: Intelligent throttling detection and backoff
- **Network Issues**: Robust error handling with minimal delays
- **Checkpoint Recovery**: Resume from last successful batch

### Common Issues

**Slow Performance:**
- Check network bandwidth
- Verify RU allocation in Cosmos DB
- Ensure Atlas cluster is appropriately sized

**Connection Errors:**
- Verify connection strings in `.env_local`
- Check network connectivity
- Ensure proper authentication

**Memory Issues:**
- Reduce batch size in configuration
- Increase system memory
- Monitor system resources

## 🔍 Verification

### Document Count Verification

```python
# Quick verification script
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv('.env_local')

# Cosmos DB count
cosmos_client = MongoClient(os.getenv('GEN_DB_CONNECTION_STRING'))
cosmos_count = cosmos_client[os.getenv('GEN_DB_NAME')][os.getenv('GEN_DB_COLLECTION')].estimated_document_count()

# Atlas count
atlas_client = MongoClient(os.getenv('MIG_TARGET_DB_CONNECTION_STRING'))
atlas_count = atlas_client[os.getenv('MIG_TARGET_DB_NAME')][os.getenv('MIG_TARGET_DB_COLLECTION')].estimated_document_count()

print(f"Cosmos DB: {cosmos_count:,} documents")
print(f"MongoDB Atlas: {atlas_count:,} documents")
print(f"Migration Complete: {cosmos_count == atlas_count}")
```

## 🚨 Important Notes

### Cost Considerations
- **Cosmos DB RU**: Monitor RU consumption during migration
- **Atlas Costs**: Ensure appropriate cluster sizing
- **Network Transfer**: Consider data transfer costs

### Performance Tips
- **Run during off-peak hours** for better performance
- **Monitor RU consumption** to avoid throttling
- **Use appropriate cluster sizes** for your data volume
- **Test with smaller datasets** before full migration

### Security

- **🔐 Never commit credentials** to version control
- **📁 Use `.env_local`** for sensitive data (not tracked by git)
- **🚫 Avoid hardcoded secrets** in scripts
- **🔄 Rotate credentials** regularly
- **📋 Use `.env_local.example`** as a template for team members
- **⚠️ Files with secrets have been removed** from git history for security

## 📞 Support and Troubleshooting

### Logs and Debugging
- Check console output for real-time progress
- Monitor RU consumption in Azure portal
- Verify Atlas cluster metrics
- Review error messages for specific issues

### Getting Help
1. **Check logs** for specific error messages
2. **Verify configuration** in `.env_local`
3. **Test data generation** with `python flexible_generator.py --source user_defined/generators/volvo_generator.py --total 5`
4. **Review performance metrics** in progress bars

## 📄 License

This project is provided as-is for Volvo service orders migration purposes.

---

**Ready to migrate?** Start with `python flexible_generator.py --source user_defined/generators/volvo_generator.py --total 5` to test your setup! 🚀