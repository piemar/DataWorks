# 🚀 DataWorks

**Enterprise Data Migration & Generation Framework**

DataWorks handles large-scale migrations of NoSQL legacy databases, such CosmosDB RU With MongoDB API, version 3.6, or MongoDB versions lower than 4 and up. The existing tools like Compass, mongorestore, mongodump, mongoexport, mongoimport, and LiveMirror cannot support. 

DataWorks focuses on handling large-scale migrations of NoSQL legacy databases, such CosmosDB RU With MongoDB API, version 3.6, or MongoDB versions lower than 4 and up. The existing tools like Compass, mongorestore, mongodump, mongoexport, mongoimport, and LiveMirror cannot support.  Also, DataWorks provides a flexible data generation engine with support for JSON templates, Python generators, and builtin generators. 

Dataworks data generation engine supports JSON templates, Python generators, and builtin generators. Domain templates are included to easy get started with your own data generation. E.g service orders, user profiles, products, orders, etc.

Dataworks data migration engine supports custom migration strategies. You can create your own migration strategy by extending the BaseMigrationStrategy class. E.g. migrate service orders to user profiles, migrate products to orders, etc. Custom strategies are supported to handle complex migrations with custom logic.

---

## 🎯 What is DataWorks?

DataWorks is a professional-grade framework designed for:
- **Large-scale migrations** (100GB+, 60M+ documents)
- **Legacy MongoDB versions** (Cosmos DB, Atlas, self-hosted)
- **Enterprise reliability** with checkpoint recovery
- **Maximum performance** with parallel processing

### Why DataWorks?

| Existing Tools | DataWorks |
|---|---|
| ❌ Compass | ✅ Handles 100GB+ datasets |
| ❌ mongorestore | ✅ Resumable migrations |
| ❌ mongodump | ✅ Real-time progress tracking |
| ❌ mongoexport/import | ✅ Enterprise error handling |
| ❌ LiveMirror | ✅ Custom migration strategies |

---

## 🚀 Quick Start (5 minutes)

### 1. Install DataWorks

```bash
# Clone and setup
git clone https://github.com/piemar/data-migrator.git
cd data-migrator
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Your Databases

```bash
# Copy configuration template
cp .env_local.example .env_local

# Edit with your connection strings
nano .env_local
```

**Required settings in `.env_local`:**
```env
# Choose your profile
FRAMEWORK_PROFILE=data-migration

# Source database (where your data is)
GEN_DB_CONNECTION_STRING="mongodb://your-cosmos-connection-string"
GEN_DB_NAME=your-source-database
GEN_DB_COLLECTION=your-collection

# Target database (where you want to migrate)
MIG_TARGET_DB_CONNECTION_STRING="mongodb+srv://your-atlas-connection-string"
MIG_TARGET_DB_NAME=your-target-database
MIG_TARGET_DB_COLLECTION=your-collection
```

### 3. Test Data Generation

```bash
# Generate 5 test documents
python flexible_generator.py --source user_defined/templates/service_orders/service_order_template.json --total 5
```

### 4. Run Migration

```bash
# Migrate your data
python flexible_migrate.py --strategy user_defined/strategies/volvo_strategy.py
```

**That's it!** 🎉

---

## 📊 Performance Profiles

DataWorks comes with three optimized profiles:

| Profile | Use Case | Performance | Resources |
|---------|----------|-------------|-----------|
| `dev` | Testing & Development | 1k docs/s | Minimal |
| `data-migration` | Production Migration | 4k-8k docs/s | Balanced |
| `data-ingest` | High-Speed Generation | 40k+ docs/s | Maximum |

**Change profile in `.env_local`:**
```env
FRAMEWORK_PROFILE=data-migration  # Choose your profile
```

---

## 🔧 Common Use Cases

### 1. Migrate from Cosmos DB to Atlas

```bash
# Your Cosmos DB → MongoDB Atlas migration
python flexible_migrate.py --strategy user_defined/strategies/volvo_strategy.py
```

### 2. Generate Test Data

```bash
# Generate 1M documents for testing
python flexible_generator.py --source user_defined/templates/service_orders/service_order_template.json --total 1000000
```

### 3. Custom Migration Strategy

```bash
# Use your own migration logic
python flexible_migrate.py --strategy my_custom_strategy.py
```

### 4. Index Management

```bash
# Disable indexes for faster migration
python flexible_migrate.py --strategy user_defined/strategies/volvo_strategy.py --disable-indexes

# Recreate indexes after migration
python flexible_migrate.py --strategy user_defined/strategies/volvo_strategy.py --create-indexes
```

---

## 📋 Available Commands

### Data Generation

```bash
# List available generators
python flexible_generator.py --list-generators

# List JSON templates
python flexible_generator.py --list-templates

# Generate with Python generator
python flexible_generator.py --source user_defined/generators/volvo_generator.py --total 1000

# Generate with JSON template
python flexible_generator.py --source user_defined/templates/service_orders/service_order_template.json --total 1000
```

### Data Migration

```bash
# List available strategies
python flexible_migrate.py --list-strategies

# Standard migration
python flexible_migrate.py --strategy user_defined/strategies/volvo_strategy.py

# Force restart from beginning
python flexible_migrate.py --strategy user_defined/strategies/volvo_strategy.py --force-from-start

# Disable indexes for performance
python flexible_migrate.py --strategy user_defined/strategies/volvo_strategy.py --disable-indexes
```

---

## 🏗️ Project Structure

```
dataworks/
├── 📁 framework/              # Core framework
│   ├── core/                 # Database clients
│   ├── config/               # Configuration system
│   ├── generators/           # Data generation
│   ├── migrations/           # Migration engine
│   └── monitoring/           # Performance tracking
├── 📁 user_defined/          # Your custom code
│   ├── generators/           # Custom generators
│   ├── strategies/           # Custom strategies
│   └── templates/           # JSON templates
├── 🔧 flexible_generator.py   # Data generation script
├── 🔧 flexible_migrate.py    # Migration script
└── ⚙️ .env_local             # Your configuration
```

---

## 🛠️ Configuration

### Database Connection Strings

**Cosmos DB (Source):**
```env
GEN_DB_CONNECTION_STRING="mongodb://account:password@account.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb"
```

**MongoDB Atlas (Target):**
```env
MIG_TARGET_DB_CONNECTION_STRING="mongodb+srv://username:password@cluster.mongodb.net/"
```

### Performance Tuning

```env
# Override profile settings
MIG_BATCH_SIZE=20000          # Documents per batch
GEN_BATCH_SIZE=50000          # Generation batch size
FRAMEWORK_WRITE_WORKERS=16    # Parallel workers
```

---

## 📈 Monitoring & Progress

DataWorks provides real-time monitoring:

- **📊 Progress bars** with document counts and rates
- **⚡ Performance metrics** (docs/sec, RU consumption)
- **🔄 Checkpoint recovery** (resume from interruptions)
- **📝 Detailed logging** for troubleshooting

---

## 🚨 Troubleshooting

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

### Getting Help

1. **Check logs** for specific error messages
2. **Verify configuration** in `.env_local`
3. **Test data generation** with small amounts first
4. **Review performance metrics** in progress bars

---

## 📄 License

This project is provided as-is for enterprise data migration purposes.

---

**Ready to migrate?** Start with a small test: `python flexible_generator.py --source user_defined/templates/service_orders/service_order_template.json --total 5` 🚀