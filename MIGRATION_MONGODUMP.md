## Mongodump/Mongorestore Migration (Cosmos Mongo API -> MongoDB Atlas)

This guide provides scripts and steps to export data from Azure Cosmos DB (MongoDB API) using `mongodump` and import it into MongoDB Atlas using `mongorestore`.

### Prerequisites

- Install MongoDB Database Tools (mongodump/mongorestore):
  - macOS: `brew tap mongodb/brew && brew install mongodb-database-tools`
  - Download: https://www.mongodb.com/try/download/database-tools
- Ensure `config.env` is configured with:

```
COSMOS_DB_CONNECTION_STRING=...
COSMOS_DB_NAME=volvo-service-orders
COSMOS_DB_COLLECTION=serviceorders  # optional; if omitted, dumps all collections

MONGODB_ATLAS_CONNECTION_STRING=...
MONGODB_ATLAS_DB_NAME=volvo-service-orders
MONGODB_ATLAS_COLLECTION=serviceorders  # optional; if omitted, restores all
```

### 1) Dump from Cosmos

Dump to a directory (default gzip):

```
./dump_cosmos.sh dumps/volvo-service-orders
```

Dump to a single gzip archive:

```
./dump_cosmos.sh --archive cosmos_dump.gz
```

Dump only one collection:

```
./dump_cosmos.sh dumps/volvo --collection serviceorders
```

Notes:

- Uses `--gzip` and `--numParallelCollections 4` for speed.
- Cosmos Mongo API requires `retryWrites=false` in the URI (already in your .env).
- Sharded collections: mongodump operates at the cluster level through the connection string; if the shard key exists, documents must retain their shard key fields.

### 2) Restore to Atlas

Restore from directory:

```
./restore_to_atlas.sh dumps/volvo-service-orders
```

Restore from single archive:

```
./restore_to_atlas.sh --archive cosmos_dump.gz
```

Restore one collection only:

```
./restore_to_atlas.sh dumps/volvo --collection serviceorders
```

Replace/Drop target collection(s) first:

```
./restore_to_atlas.sh dumps/volvo --drop
```

Notes:

- Uses `--gzip`, `--numInsertionWorkersPerCollection 8`, and `--maintainInsertionOrder`.
- If Atlas has validation or unique indexes not present in source, restore may fail; drop or adjust indexes before restore, then recreate afterward.
- For large restores, consider temporarily scaling Atlas tier (e.g., M40+) to accelerate import, then scale back.

### 3) Post-Restore Indexes

If you extracted index specs earlier, apply them on Atlas after restore. Or use your existing index scripts in this repo.

### 4) Verification

Compare counts:

```
# Optionally use your query_tool or mongo shell to compare counts
```

### 5) Cutover Strategy (optional)

For minimal downtime:

1. Run `mongodump` and initial `mongorestore`.
2. Use your live-sync process (existing migration tool or change streams) to copy deltas.
3. Quiesce writes on Cosmos, perform final sync, switch app connection string to Atlas.


