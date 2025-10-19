# 🚀 Azure Cosmos DB to MongoDB Atlas Migration Guide

## Overview

This guide provides step-by-step instructions for migrating 100GB of Volvo service orders data from Azure Cosmos DB RU MongoDB API to MongoDB Atlas efficiently and safely.

## 📋 Pre-Migration Checklist

### 1. Azure Cosmos DB Preparation

**Scale Up Resources:**
- Increase RU (Request Units) to 10,000+ during migration
- Enable autoscale for better performance
- Monitor RU consumption during migration

**Network Configuration:**
- Ensure your migration machine has access to Cosmos DB
- Configure firewall rules if needed
- Test connectivity from migration environment

**Backup Strategy:**
- Create a point-in-time backup before migration
- Document current RU settings for rollback

### 2. MongoDB Atlas Preparation

**Cluster Configuration:**
- Choose M30 or higher cluster tier for 100GB
- Ensure sufficient storage (150GB+ recommended)
- Configure appropriate sharding if needed

**Network Access:**
- Add migration machine IP to Atlas IP whitelist
- Configure VPC peering if using private networks
- Test connectivity from migration environment

**User Permissions:**
- Create dedicated migration user with read/write permissions
- Use strong authentication credentials
- Document user permissions for audit

### 3. Migration Environment

**Hardware Requirements:**
- CPU: 8+ cores recommended
- RAM: 16GB+ recommended
- Storage: 50GB+ free space for logs and checkpoints
- Network: Stable, high-bandwidth connection

**Software Requirements:**
- Python 3.8+
- Stable internet connection
- Sufficient disk space for logs

## 🚀 Migration Process

### Step 1: Environment Setup

```bash
# Clone/download the migration project
cd volvo-vida

# Run the quick start script
./quick_start.sh
```

### Step 2: Configuration

1. **Update `.env` file** with your connection strings:
   ```env
   # Azure Cosmos DB
   COSMOS_DB_CONNECTION_STRING=mongodb://your-account:your-key@your-account.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@your-account@
   
   # MongoDB Atlas
   MONGODB_ATLAS_CONNECTION_STRING=mongodb+srv://username:password@cluster.mongodb.net/
   
   # Migration settings for 100GB
   MIGRATION_BATCH_SIZE=10000
   MIGRATION_WORKERS=16
   ```

2. **Test connections**:
   ```bash
   python -c "
   import asyncio
   from motor.motor_asyncio import AsyncIOMotorClient
   from dotenv import load_dotenv
   import os
   
   load_dotenv()
   
   async def test():
       cosmos = AsyncIOMotorClient(os.getenv('COSMOS_DB_CONNECTION_STRING'))
       await cosmos.admin.command('ping')
       print('✅ Cosmos DB OK')
       
       atlas = AsyncIOMotorClient(os.getenv('MONGODB_ATLAS_CONNECTION_STRING'))
       await atlas.admin.command('ping')
       print('✅ Atlas OK')
   
   asyncio.run(test())
   "
   ```

### Step 3: Performance Testing (Optional but Recommended)

```bash
# Run performance test to optimize settings
python performance_tuner.py
```

This will:
- Test optimal batch sizes
- Find best worker count
- Benchmark connection performance
- Generate recommendations

### Step 4: Start Migration

**Option A: Full Migration**
```bash
# Start migration (will run until complete)
python migrate_to_atlas.py
```

**Option B: Migration with Monitoring**
```bash
# Terminal 1: Start migration
python migrate_to_atlas.py

# Terminal 2: Monitor progress
python monitor_migration.py
```

### Step 5: Monitor Progress

The migration provides real-time feedback:
- Documents migrated per second
- Progress percentage
- Estimated time to completion
- Error tracking

### Step 6: Verification

```bash
# Verify document counts match
python -c "
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import os

load_dotenv()

async def verify():
    cosmos = AsyncIOMotorClient(os.getenv('COSMOS_DB_CONNECTION_STRING'))
    cosmos_count = await cosmos['volvo-service-orders']['serviceorders'].count_documents({})
    
    atlas = AsyncIOMotorClient(os.getenv('MONGODB_ATLAS_CONNECTION_STRING'))
    atlas_count = await atlas['volvo-service-orders']['serviceorders'].count_documents({})
    
    print(f'Cosmos DB: {cosmos_count:,} documents')
    print(f'MongoDB Atlas: {atlas_count:,} documents')
    print(f'Match: {cosmos_count == atlas_count}')

asyncio.run(verify())
"
```

## ⚡ Performance Optimization for 100GB

### Recommended Settings

```env
# For 100GB migration
MIGRATION_BATCH_SIZE=10000
MIGRATION_WORKERS=16
BATCH_SIZE=2000
CONCURRENT_WORKERS=20
```

### Performance Tuning Tips

1. **Batch Size**: Start with 5,000-10,000 documents per batch
2. **Workers**: Use 8-16 concurrent workers based on your system
3. **Network**: Ensure stable, high-bandwidth connection
4. **Resources**: Monitor CPU and memory usage

### Expected Performance

- **Rate**: 2,000-8,000 documents/second
- **100GB**: 4-8 hours (depending on network and resources)
- **Peak Performance**: Up to 10,000+ docs/sec with optimal settings

## 🛡️ Safety Features

### Checkpoint System

- Saves progress every 50,000 documents
- Resume from interruption point
- No data duplication
- Checkpoint file: `migration_checkpoint.json`

### Error Handling

- Continues on individual document errors
- Logs all errors for review
- Graceful shutdown on interruption
- Automatic retry for transient errors

### Data Integrity

- Preserves all document fields
- Maintains data types
- Handles BSON compatibility
- Index creation on target

## 🔧 Troubleshooting

### Common Issues

**Slow Migration:**
```bash
# Increase batch size
export MIGRATION_BATCH_SIZE=15000

# Add more workers
export MIGRATION_WORKERS=20

# Check network bandwidth
```

**Connection Errors:**
```bash
# Test connectivity
ping your-cosmos-account.mongo.cosmos.azure.com
ping cluster.mongodb.net

# Check firewall settings
# Verify connection strings
```

**Memory Issues:**
```bash
# Reduce batch size
export MIGRATION_BATCH_SIZE=5000

# Reduce workers
export MIGRATION_WORKERS=8
```

### Recovery Procedures

**Resume Interrupted Migration:**
```bash
# Migration will automatically resume from checkpoint
python migrate_to_atlas.py
```

**Reset Migration:**
```bash
# Delete checkpoint file to start fresh
rm migration_checkpoint.json

# Clear target database (if needed)
python -c "
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import os

load_dotenv()

async def clear_target():
    atlas = AsyncIOMotorClient(os.getenv('MONGODB_ATLAS_CONNECTION_STRING'))
    await atlas['volvo-service-orders']['serviceorders'].drop()
    print('Target collection cleared')

asyncio.run(clear_target())
"
```

## 📊 Monitoring and Metrics

### Key Metrics to Monitor

1. **Migration Rate**: Documents per second
2. **Error Rate**: Failed documents per batch
3. **Resource Usage**: CPU, memory, network
4. **Database Performance**: RU consumption, connection count

### Monitoring Commands

```bash
# Real-time monitoring
python monitor_migration.py

# Check logs
tail -f logs/migration.log

# Performance metrics
python performance_tuner.py
```

## 💰 Cost Optimization

### Azure Cosmos DB Costs

- **RU Consumption**: Monitor and optimize batch sizes
- **Storage**: Consider cleanup after migration
- **Network**: Minimize data transfer costs

### MongoDB Atlas Costs

- **Cluster Size**: Right-size for your workload
- **Storage**: Monitor usage and optimize
- **Data Transfer**: Consider compression

## 🎯 Post-Migration Steps

### 1. Application Testing

```bash
# Test application connectivity
# Verify query performance
# Check data integrity
```

### 2. Performance Validation

```bash
# Run performance tests on Atlas
# Compare query performance
# Validate index usage
```

### 3. Cutover Planning

- Plan application downtime
- Update connection strings
- Test failover procedures
- Monitor application performance

### 4. Cleanup

```bash
# Archive old Cosmos DB data
# Update documentation
# Clean up migration files
```

## 📞 Support and Resources

### Log Files

- `logs/migration.log`: Migration progress and errors
- `logs/data_generator.log`: Data generation logs
- `performance_results.json`: Performance test results

### Useful Commands

```bash
# Check migration status
cat migration_checkpoint.json

# Monitor system resources
top -p $(pgrep -f migrate_to_atlas.py)

# Check network connectivity
netstat -an | grep :27017
```

### Emergency Contacts

- Azure Support: For Cosmos DB issues
- MongoDB Support: For Atlas issues
- Internal IT: For infrastructure issues

## ✅ Success Criteria

Migration is considered successful when:

1. ✅ All documents migrated (count matches)
2. ✅ No data corruption or loss
3. ✅ Application functionality verified
4. ✅ Performance meets requirements
5. ✅ Monitoring and alerting configured
6. ✅ Documentation updated
7. ✅ Team trained on new system

---

**Remember**: Always test the migration process with a small dataset first before running the full 100GB migration!
