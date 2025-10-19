# Volvo Service Orders Migration Project

This project provides a comprehensive solution for migrating Volvo service orders data from Azure Cosmos DB RU MongoDB API to MongoDB Atlas, including data generation and high-performance migration tools.

## 🚀 Features

- **Realistic Data Generation**: Creates authentic Volvo service order data with proper relationships
- **High-Performance Migration**: Optimized for large datasets (100GB+) with parallel processing
- **Resumable Migration**: Checkpoint-based migration that can resume from interruptions
- **Real-time Monitoring**: Track migration progress and performance metrics
- **Error Handling**: Robust error handling and retry mechanisms
- **Index Management**: Automatic index creation for optimal query performance

## 📋 Prerequisites

- Python 3.8 or higher
- Azure Cosmos DB account with MongoDB API
- MongoDB Atlas cluster
- Network connectivity to both databases

## 🛠️ Installation

1. **Clone or download the project files**

2. **Run the setup script**:
   ```bash
   python setup.py
   ```

3. **Install dependencies manually** (if setup script fails):
   ```bash
   pip install -r requirements.txt
   ```

## ⚙️ Configuration

1. **Copy the environment template**:
   ```bash
   cp config.env.example .env
   ```

2. **Update `.env` file with your connection strings**:
   ```env
   # Azure Cosmos DB Configuration
   COSMOS_DB_CONNECTION_STRING=mongodb://your-cosmos-account:your-key@your-cosmos-account.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@your-cosmos-account@
   COSMOS_DB_NAME=volvo-service-orders
   COSMOS_DB_COLLECTION=serviceorders

   # MongoDB Atlas Configuration
   MONGODB_ATLAS_CONNECTION_STRING=mongodb+srv://username:password@cluster.mongodb.net/
   MONGODB_ATLAS_DB_NAME=volvo-service-orders
   MONGODB_ATLAS_COLLECTION=serviceorders

   # Data Generation Settings
   BATCH_SIZE=1000
   TOTAL_DOCUMENTS=10000000
   CONCURRENT_WORKERS=10

   # Migration Settings
   MIGRATION_BATCH_SIZE=5000
   MIGRATION_WORKERS=8
   RESUME_FROM_CHECKPOINT=true
   ```

## 📊 Data Generation

### Generate Test Data

To generate 100GB of test data (approximately 10 million documents):

```bash
# Set environment variables
export TOTAL_DOCUMENTS=10000000
export GENERATION_MODE=specific

# Run data generator
python data_generator.py
```

### Continuous Data Generation

For continuous data generation:

```bash
# Set environment variables
export GENERATION_MODE=continuous

# Run data generator
python data_generator.py
```

### Configuration Options

- `TOTAL_DOCUMENTS`: Total number of documents to generate (default: 10,000,000)
- `BATCH_SIZE`: Number of documents per batch (default: 1,000)
- `CONCURRENT_WORKERS`: Number of concurrent workers (default: 10)
- `GENERATION_MODE`: `specific` or `continuous` (default: `continuous`)

## 🔄 Migration Process

### 1. Pre-Migration Setup

**Azure Cosmos DB:**
- Ensure sufficient RU (Request Units) for read operations
- Consider scaling up during migration for better performance
- Monitor RU consumption during migration

**MongoDB Atlas:**
- Choose appropriate cluster tier (M10 or higher recommended for 100GB)
- Ensure sufficient storage capacity
- Configure network access and IP whitelisting

### 2. Run Migration

```bash
# Start migration
python migrate_to_atlas.py
```

### 3. Monitor Progress

In a separate terminal, run the monitoring script:

```bash
python monitor_migration.py
```

### 4. Migration Features

- **Resumable**: Migration can be interrupted and resumed using checkpoints
- **Parallel Processing**: Uses multiple workers for faster migration
- **Error Handling**: Continues migration even if individual documents fail
- **Progress Tracking**: Real-time progress updates and statistics
- **Index Creation**: Automatically creates indexes on target database

## 📈 Performance Optimization

### For 100GB Migration

1. **Batch Size**: Use larger batch sizes (5000-10000) for better throughput
2. **Workers**: Increase concurrent workers (8-16) based on your system
3. **Network**: Ensure stable, high-bandwidth connection
4. **Resources**: Use machines with sufficient CPU and memory

### Recommended Settings for 100GB

```env
MIGRATION_BATCH_SIZE=10000
MIGRATION_WORKERS=16
BATCH_SIZE=2000
CONCURRENT_WORKERS=20
```

## 🔍 Monitoring and Verification

### Real-time Monitoring

```bash
python monitor_migration.py
```

This script provides:
- Current document counts
- Migration progress percentage
- Documents per second rate
- Estimated time to completion
- Error tracking

### Manual Verification

```python
# Check document counts
from pymongo import MongoClient

# Cosmos DB
cosmos_client = MongoClient("your-cosmos-connection-string")
cosmos_count = cosmos_client["volvo-service-orders"]["serviceorders"].count_documents({})

# MongoDB Atlas
atlas_client = MongoClient("your-atlas-connection-string")
atlas_count = atlas_client["volvo-service-orders"]["serviceorders"].count_documents({})

print(f"Cosmos DB: {cosmos_count:,} documents")
print(f"MongoDB Atlas: {atlas_count:,} documents")
```

## 🛡️ Error Handling and Recovery

### Checkpoint System

The migration uses a checkpoint system that saves progress every 50,000 documents. If the migration is interrupted:

1. The checkpoint file (`migration_checkpoint.json`) contains the last processed document ID
2. Restart the migration - it will automatically resume from the checkpoint
3. No data will be duplicated

### Common Issues and Solutions

**Connection Timeouts:**
- Increase timeout values in connection strings
- Check network connectivity
- Verify firewall settings

**Rate Limiting:**
- Reduce batch size
- Decrease number of workers
- Add delays between batches

**Memory Issues:**
- Reduce batch size
- Increase system memory
- Use streaming processing

## 📁 Project Structure

```
volvo-vida/
├── models.py                 # Data models and generators
├── data_generator.py         # Data generation application
├── migrate_to_atlas.py       # Migration script
├── monitor_migration.py      # Migration monitoring
├── setup.py                  # Setup script
├── requirements.txt          # Python dependencies
├── config.env.example        # Environment template
├── README.md                 # This file
├── migration_checkpoint.json # Migration checkpoint (created during migration)
└── logs/                     # Log files
    ├── data_generator.log
    └── migration.log
```

## 🔧 Troubleshooting

### Data Generation Issues

1. **Connection Errors**: Verify Cosmos DB connection string and network access
2. **Slow Generation**: Increase batch size and concurrent workers
3. **Memory Issues**: Reduce batch size or increase system memory

### Migration Issues

1. **Slow Migration**: 
   - Increase batch size
   - Add more workers
   - Check network bandwidth
   - Scale up Cosmos DB RU

2. **Connection Errors**:
   - Verify both connection strings
   - Check network connectivity
   - Ensure proper authentication

3. **Document Count Mismatch**:
   - Check for duplicate documents
   - Verify filter criteria
   - Review error logs

## 📊 Expected Performance

### Data Generation
- **Rate**: 5,000-15,000 documents/second
- **100GB**: Approximately 2-4 hours (depending on system)

### Migration
- **Rate**: 2,000-8,000 documents/second
- **100GB**: Approximately 4-8 hours (depending on network and resources)

## 🚨 Important Notes

1. **Cost Considerations**: 
   - Cosmos DB RU consumption during migration
   - MongoDB Atlas cluster costs
   - Network transfer costs

2. **Downtime**: 
   - Plan for application downtime during migration
   - Consider using read replicas for zero-downtime migration

3. **Data Validation**: 
   - Always verify document counts after migration
   - Test application functionality with migrated data
   - Consider running parallel systems during transition

## 📞 Support

For issues or questions:
1. Check the logs in the `logs/` directory
2. Review the troubleshooting section
3. Verify your configuration settings
4. Check database connectivity and permissions

## 📄 License

This project is provided as-is for Volvo service orders migration purposes.
