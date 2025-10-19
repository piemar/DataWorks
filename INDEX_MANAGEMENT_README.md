# Index Management Scripts

This directory contains scripts to extract indexes from Azure Cosmos DB and create them on MongoDB Atlas.

## Scripts Overview

### 1. `extract_indexes_only.py`
**Purpose**: Extract indexes from Cosmos DB and save them to a JSON file for review.

**Usage**:
```bash
python extract_indexes_only.py
```

**Output**: Creates `cosmos_indexes.json` with all indexes from Cosmos DB.

### 2. `extract_and_create_indexes.py`
**Purpose**: Extract indexes from Cosmos DB and immediately create them on MongoDB Atlas.

**Usage**:
```bash
python extract_and_create_indexes.py
```

**Output**: 
- Creates `extracted_indexes.json` with index definitions
- Creates the same indexes on MongoDB Atlas
- Logs success/failure for each index

## When to Use Each Script

### Use `extract_indexes_only.py` when:
- You want to review the indexes before creating them
- You want to modify the indexes before applying them
- You want to create indexes manually later
- You're troubleshooting index issues

### Use `extract_and_create_indexes.py` when:
- You want to automatically recreate all Cosmos DB indexes on Atlas
- You're confident the indexes will work on Atlas
- You want a one-step process

## Index Types Supported

The scripts handle all MongoDB index types:
- **Single Field Indexes**: `{field: 1}` or `{field: -1}`
- **Compound Indexes**: `{field1: 1, field2: -1}`
- **Text Indexes**: For full-text search
- **2dsphere Indexes**: For geospatial queries
- **Hashed Indexes**: For sharding
- **Wildcard Indexes**: For dynamic fields
- **TTL Indexes**: For automatic document expiration
- **Unique Indexes**: For uniqueness constraints
- **Sparse Indexes**: For partial indexing
- **Partial Indexes**: With filter expressions

## Index Properties Preserved

- ✅ **Unique constraints**
- ✅ **Sparse indexing**
- ✅ **TTL (Time To Live)**
- ✅ **Partial filter expressions**
- ✅ **Collation settings**
- ✅ **Text search weights**
- ✅ **Geospatial settings**
- ✅ **Background creation**

## Example Output

### Console Output:
```
2025-10-19 14:45:00,123 - INFO - Found 8 indexes in Cosmos DB:
2025-10-19 14:45:00,124 - INFO -   - _id_: {'_id': 1}
2025-10-19 14:45:00,125 - INFO -   - order_id_1: {'order_id': 1} (UNIQUE)
2025-10-19 14:45:00,126 - INFO -   - customer.customer_id_1: {'customer.customer_id': 1}
2025-10-19 14:45:00,127 - INFO -   - vehicle.vin_1: {'vehicle.vin': 1}
2025-10-19 14:45:00,128 - INFO -   - order_date_1: {'order_date': 1}
2025-10-19 14:45:00,129 - INFO -   - status_1: {'status': 1}
2025-10-19 14:45:00,130 - INFO -   - service_center_id_1: {'service_center_id': 1}
2025-10-19 14:45:00,131 - INFO -   - technician_id_1: {'technician_id': 1}
```

### JSON Output (`cosmos_indexes.json`):
```json
[
  {
    "name": "order_id_1",
    "key": {"order_id": 1},
    "unique": true,
    "sparse": false,
    "background": false
  },
  {
    "name": "customer.customer_id_1",
    "key": {"customer.customer_id": 1},
    "unique": false,
    "sparse": false,
    "background": false
  }
]
```

## Prerequisites

1. **Environment Variables**: Ensure `.env` file has correct connection strings
2. **Database Access**: Both Cosmos DB and MongoDB Atlas must be accessible
3. **Permissions**: Atlas user must have index creation permissions
4. **Dependencies**: All required Python packages installed

## Troubleshooting

### Common Issues:

1. **Authentication Failed**: Check Atlas connection string and user permissions
2. **Index Already Exists**: Script will skip existing indexes
3. **Unsupported Index Type**: Some Cosmos DB specific indexes may not work on Atlas
4. **Timeout Errors**: Increase connection timeout settings

### Error Messages:
- `❌ Failed to create index`: Index creation failed (check logs for details)
- `✅ Created index`: Index created successfully
- `Skipping default _id index`: Normal behavior (Atlas creates this automatically)

## Best Practices

1. **Run After Migration**: Execute index creation after data migration is complete
2. **Monitor Performance**: Watch for performance impact during index creation
3. **Test Queries**: Verify that queries work correctly with new indexes
4. **Backup First**: Always backup your Atlas database before creating indexes
5. **Review Indexes**: Use `extract_indexes_only.py` to review before creating

## Performance Considerations

- **Background Creation**: Indexes are created in background by default
- **Large Collections**: Index creation on large collections can take time
- **Resource Usage**: Index creation uses CPU and memory resources
- **Query Impact**: Some queries may be slower during index creation

## Next Steps

After running the index creation script:

1. **Verify Indexes**: Check that all indexes were created successfully
2. **Test Queries**: Run your application queries to ensure they work
3. **Monitor Performance**: Watch query performance and adjust if needed
4. **Clean Up**: Remove any unnecessary indexes to save storage space
