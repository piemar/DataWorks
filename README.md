# 🚀 Volvo Service Orders Data Migration Project

A high-performance solution for migrating Volvo service orders data from Azure Cosmos DB (MongoDB API) to MongoDB Atlas, with optimized data generation and migration tools.

## 🎯 Project Purpose

This project provides a complete data migration solution for Volvo service orders, designed to handle large-scale data operations (60M+ documents) with maximum speed and reliability. It includes:

- **Realistic Data Generation**: Creates authentic Volvo service order data with proper relationships
- **High-Performance Migration**: Optimized for maximum speed with parallel processing
- **Resumable Operations**: Checkpoint-based migration that can resume from interruptions
- **Real-time Monitoring**: Track progress with detailed performance metrics
- **Error Handling**: Robust error handling and retry mechanisms

## 🚀 Key Features

- ⚡ **Maximum Speed Optimization**: Unacknowledged writes, direct batch inserts, optimized connection pools
- 🔄 **Resumable Migration**: Automatic checkpoint system for large migrations
- 📊 **Real-time Progress**: Live progress bars with RU consumption and performance metrics
- 🛡️ **Error Recovery**: Intelligent retry logic with exponential backoff
- 🎯 **Production Ready**: Handles 60M+ documents with optimized settings

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
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
# Copy the template
cp .env_local.example .env_local

# Edit with your credentials
nano .env_local
```

**Required Configuration:**
```env
# Azure Cosmos DB Configuration
COSMOS_DB_CONNECTION_STRING="mongodb://your-account:YOUR_PASSWORD@your-account.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@your-account@"
COSMOS_DB_NAME=volvo-service-orders
COSMOS_DB_COLLECTION=serviceorders

# MongoDB Atlas Configuration
MONGODB_ATLAS_CONNECTION_STRING="mongodb+srv://your-username:YOUR_PASSWORD@your-cluster.mongodb.net/?retryWrites=false&w=majority&appName=YourCluster"
MONGODB_ATLAS_DB_NAME=volvo-service-orders
MONGODB_ATLAS_COLLECTION=serviceorders

# Data Generation Settings (OPTIMIZED for MAXIMUM SPEED)
BATCH_SIZE=2000
TOTAL_DOCUMENTS=61000000
CONCURRENT_WORKERS=12

# Migration Settings (MAXIMUM SPEED OPTIMIZED)
MIGRATION_BATCH_SIZE=4000
MIGRATION_WORKERS=12
RESUME_FROM_CHECKPOINT=true
```

### 3. Test Connection

```bash
python test_connection.py
```

### 4. Generate Data (Optional)

```bash
# Generate test data
python data_generator.py
```

### 5. Run Migration

```bash
# Start migration
python migrate_to_atlas.py
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
├── 📄 Core Scripts
│   ├── data_generator.py         # High-speed data generation
│   ├── migrate_to_atlas.py       # Optimized migration script
│   ├── models.py                 # Data models and generators
│   ├── mongodb_compatibility.py  # Compatibility layer
│   └── test_connection.py        # Connection testing
│
├── 🔧 Utility Scripts
│   ├── extract_indexes_only.py   # Extract indexes from Cosmos DB
│   ├── extract_and_create_indexes.py # Create indexes on Atlas
│   ├── dump_cosmos.sh            # Cosmos DB dump script
│   └── restore_to_atlas.sh       # Atlas restore script
│
├── ⚙️ Configuration
│   ├── .env_local                # Your credentials (not tracked)
│   ├── .env_local.example        # Template for developers
│   ├── config.env                # Example values
│   ├── requirements.txt          # Python dependencies
│   └── setup.py                  # Setup script
│
├── 📊 Runtime Files
│   ├── migration_checkpoint.json # Migration progress (auto-created)
│   ├── count_cache.json         # Document count cache
│   └── estimated_count_cache.json # Estimated count cache
│
└── 📚 Documentation
    └── README.md                 # This file
```

## 🚀 Usage Examples

### Data Generation

```bash
# Generate 61M documents (current setting)
python data_generator.py

# Generate specific amount
export TOTAL_DOCUMENTS=1000000
python data_generator.py
```

### Migration

```bash
# Full migration with checkpoint support
python migrate_to_atlas.py

# Resume from checkpoint (automatic)
python migrate_to_atlas.py
```

### Index Management

```bash
# Extract indexes from Cosmos DB
python extract_indexes_only.py

# Create indexes on Atlas
python extract_and_create_indexes.py
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
- **File**: `migration_checkpoint.json`
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
cosmos_client = MongoClient(os.getenv('COSMOS_DB_CONNECTION_STRING'))
cosmos_count = cosmos_client[os.getenv('COSMOS_DB_NAME')][os.getenv('COSMOS_DB_COLLECTION')].count_documents({})

# Atlas count
atlas_client = MongoClient(os.getenv('MONGODB_ATLAS_CONNECTION_STRING'))
atlas_count = atlas_client[os.getenv('MONGODB_ATLAS_DB_NAME')][os.getenv('MONGODB_ATLAS_COLLECTION')].count_documents({})

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
- **Never commit credentials** to version control
- **Use `.env_local`** for sensitive data
- **Rotate credentials** after migration
- **Monitor access logs** for security

## 📞 Support and Troubleshooting

### Logs and Debugging
- Check console output for real-time progress
- Monitor RU consumption in Azure portal
- Verify Atlas cluster metrics
- Review error messages for specific issues

### Getting Help
1. **Check logs** for specific error messages
2. **Verify configuration** in `.env_local`
3. **Test connections** with `test_connection.py`
4. **Review performance metrics** in progress bars

## 📄 License

This project is provided as-is for Volvo service orders migration purposes.

---

**Ready to migrate?** Start with `python test_connection.py` to verify your setup! 🚀