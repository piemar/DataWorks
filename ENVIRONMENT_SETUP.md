# Environment Setup

## Setting Up Environment Variables

This project uses a secure environment variable setup to protect sensitive credentials:

### 1. Create Your Local Environment File

Copy the example file and fill in your actual credentials:

```bash
cp .env_local.example .env_local
```

### 2. Edit `.env_local` with Your Credentials

Open `.env_local` and replace the placeholder values with your actual credentials:

```bash
# Azure Cosmos DB Configuration - SENSITIVE CREDENTIALS
COSMOS_DB_CONNECTION_STRING="mongodb://your-account:YOUR_PASSWORD@your-account.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@your-account@"
COSMOS_DB_NAME=your-database-name
COSMOS_DB_COLLECTION=your-collection-name

# MongoDB Atlas Configuration - SENSITIVE CREDENTIALS
MONGODB_ATLAS_CONNECTION_STRING="mongodb+srv://your-username:YOUR_PASSWORD@your-cluster.mongodb.net/?retryWrites=false&w=majority&appName=YourCluster"
MONGODB_ATLAS_DB_NAME=your-database-name
MONGODB_ATLAS_COLLECTION=your-collection-name
```

### 3. Security Notes

- **`.env_local`** contains your actual sensitive credentials and is **NOT tracked by Git**
- **`config.env`** contains example values and **IS tracked by Git** for reference
- The application will automatically load from `.env_local` first, then fallback to `config.env`

### 4. File Structure

```
├── .env_local          # Your actual credentials (NOT tracked by Git)
├── .env_local.example  # Template for other developers (tracked by Git)
├── config.env          # Example values (tracked by Git)
└── .gitignore          # Excludes .env_local from version control
```

### 5. For Team Members

When setting up the project:

1. Copy `.env_local.example` to `.env_local`
2. Fill in your actual credentials in `.env_local`
3. Never commit `.env_local` to version control

This setup ensures that:
- ✅ Sensitive credentials are never committed to the repository
- ✅ Example values are available for reference
- ✅ Each developer can have their own local credentials
- ✅ The application works seamlessly with both files
