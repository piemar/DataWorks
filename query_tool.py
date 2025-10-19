"""
Simple query tool for Azure Cosmos DB when Compass doesn't work
Provides basic database operations and data exploration
"""
import asyncio
import os
import json
from datetime import datetime
from typing import Dict, List, Any
import logging

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from mongodb_compatibility import create_compatible_cosmos_client

# Load environment variables from .env_local (sensitive) or config.env (examples)
load_dotenv('.env_local')  # Try to load sensitive credentials first
load_dotenv('config.env')  # Fallback to example values

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CosmosDBQueryTool:
    """Simple query tool for Cosmos DB operations"""
    
    def __init__(self):
        self.connection_string = os.getenv('COSMOS_DB_CONNECTION_STRING')
        self.db_name = os.getenv('COSMOS_DB_NAME', 'volvo-service-orders')
        self.collection_name = os.getenv('COSMOS_DB_COLLECTION', 'serviceorders')
        self.client = None
        self.collection = None

    async def connect(self):
        """Connect to Cosmos DB"""
        try:
            logger.info("Connecting to Azure Cosmos DB...")
            compatible_client = create_compatible_cosmos_client(self.connection_string)
            self.client = compatible_client.create_client(async_client=True)
            
            # Test connection
            await self.client.admin.command('ping')
            
            db = self.client[self.db_name]
            self.collection = db[self.collection_name]
            
            logger.info("✅ Connected to Cosmos DB successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect: {e}")
            return False

    async def get_stats(self):
        """Get database statistics"""
        try:
            total_docs = await self.collection.count_documents({})
            
            # Get sample of documents to analyze
            sample_docs = await self.collection.find({}).limit(100).to_list(length=100)
            
            # Initialize variables
            estimated_size_gb = 0
            statuses = set()
            years = set()
            service_centers = set()
            
            # Analyze document structure
            if sample_docs:
                sample_doc = sample_docs[0]
                doc_size = len(json.dumps(sample_doc, default=str))
                estimated_size_gb = (total_docs * doc_size) / (1024**3)
                
                # Get unique values for key fields
                for doc in sample_docs:
                    if 'status' in doc:
                        statuses.add(doc['status'])
                    if 'order_date' in doc:
                        try:
                            year = datetime.fromisoformat(doc['order_date'].replace('Z', '+00:00')).year
                            years.add(year)
                        except:
                            pass
                    if 'service_center_id' in doc:
                        service_centers.add(doc['service_center_id'])
                
                print(f"\n📊 Database Statistics")
                print(f"=" * 50)
                print(f"Total Documents: {total_docs:,}")
                print(f"Estimated Size: {estimated_size_gb:.2f} GB")
                print(f"Sample Document Size: {doc_size:,} bytes")
                print(f"Unique Statuses: {sorted(statuses)}")
                print(f"Years: {sorted(years)}")
                print(f"Service Centers: {len(service_centers)}")
                
            return {
                'total_docs': total_docs,
                'estimated_size_gb': estimated_size_gb if sample_docs else 0,
                'statuses': list(statuses),
                'years': list(years),
                'service_centers': len(service_centers)
            }
            
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return None

    async def find_documents(self, query: Dict = None, limit: int = 10, skip: int = 0):
        """Find documents with optional query"""
        try:
            if query is None:
                query = {}
            
            cursor = self.collection.find(query).skip(skip).limit(limit)
            documents = await cursor.to_list(length=limit)
            
            print(f"\n🔍 Query Results (showing {len(documents)} documents)")
            print(f"=" * 50)
            
            for i, doc in enumerate(documents, 1):
                print(f"\nDocument {i}:")
                print(f"  Order ID: {doc.get('order_id', 'N/A')}")
                print(f"  Status: {doc.get('status', 'N/A')}")
                print(f"  Order Date: {doc.get('order_date', 'N/A')}")
                print(f"  Customer: {doc.get('customer', {}).get('name', 'N/A')}")
                print(f"  Vehicle: {doc.get('vehicle', {}).get('model', 'N/A')} {doc.get('vehicle', {}).get('year', 'N/A')}")
                print(f"  Total Amount: {doc.get('total_amount', 'N/A')}")
                
            return documents
            
        except Exception as e:
            logger.error(f"Error finding documents: {e}")
            return []

    async def count_by_field(self, field: str):
        """Count documents by field values"""
        try:
            pipeline = [
                {"$group": {"_id": f"${field}", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 20}
            ]
            
            cursor = self.collection.aggregate(pipeline)
            results = await cursor.to_list(length=20)
            
            print(f"\n📈 Count by {field}")
            print(f"=" * 50)
            
            for result in results:
                value = result['_id'] if result['_id'] is not None else 'null'
                count = result['count']
                print(f"  {value}: {count:,}")
                
            return results
            
        except Exception as e:
            logger.error(f"Error counting by field: {e}")
            return []

    async def search_orders(self, search_term: str, limit: int = 10):
        """Search orders by various fields"""
        try:
            # Create search query
            search_query = {
                "$or": [
                    {"order_id": {"$regex": search_term, "$options": "i"}},
                    {"customer.name": {"$regex": search_term, "$options": "i"}},
                    {"vehicle.model": {"$regex": search_term, "$options": "i"}},
                    {"vehicle.vin": {"$regex": search_term, "$options": "i"}},
                    {"status": {"$regex": search_term, "$options": "i"}}
                ]
            }
            
            cursor = self.collection.find(search_query).limit(limit)
            documents = await cursor.to_list(length=limit)
            
            print(f"\n🔍 Search Results for '{search_term}' ({len(documents)} documents)")
            print(f"=" * 50)
            
            for i, doc in enumerate(documents, 1):
                print(f"\nResult {i}:")
                print(f"  Order ID: {doc.get('order_id', 'N/A')}")
                print(f"  Customer: {doc.get('customer', {}).get('name', 'N/A')}")
                print(f"  Vehicle: {doc.get('vehicle', {}).get('model', 'N/A')}")
                print(f"  Status: {doc.get('status', 'N/A')}")
                
            return documents
            
        except Exception as e:
            logger.error(f"Error searching: {e}")
            return []

    async def get_recent_orders(self, days: int = 7, limit: int = 10):
        """Get recent orders"""
        try:
            from datetime import datetime, timedelta
            
            # Calculate date threshold
            threshold_date = datetime.utcnow() - timedelta(days=days)
            
            query = {
                "order_date": {"$gte": threshold_date.isoformat()}
            }
            
            cursor = self.collection.find(query).sort("order_date", -1).limit(limit)
            documents = await cursor.to_list(length=limit)
            
            print(f"\n📅 Recent Orders (last {days} days)")
            print(f"=" * 50)
            
            for i, doc in enumerate(documents, 1):
                print(f"\nOrder {i}:")
                print(f"  Order ID: {doc.get('order_id', 'N/A')}")
                print(f"  Date: {doc.get('order_date', 'N/A')}")
                print(f"  Customer: {doc.get('customer', {}).get('name', 'N/A')}")
                print(f"  Status: {doc.get('status', 'N/A')}")
                print(f"  Amount: {doc.get('total_amount', 'N/A')}")
                
            return documents
            
        except Exception as e:
            logger.error(f"Error getting recent orders: {e}")
            return []

    async def cleanup(self):
        """Close connection"""
        if self.client:
            self.client.close()

async def interactive_mode():
    """Interactive query mode"""
    tool = CosmosDBQueryTool()
    
    if not await tool.connect():
        return
    
    try:
        while True:
            print(f"\n🔧 Cosmos DB Query Tool")
            print(f"=" * 50)
            print(f"1. Get database statistics")
            print(f"2. Find documents (with query)")
            print(f"3. Count by field")
            print(f"4. Search orders")
            print(f"5. Get recent orders")
            print(f"6. Exit")
            
            choice = input("\nEnter your choice (1-6): ").strip()
            
            if choice == '1':
                await tool.get_stats()
                
            elif choice == '2':
                query_str = input("Enter query (JSON format, or press Enter for all): ").strip()
                limit = int(input("Limit (default 10): ") or "10")
                
                query = {}
                if query_str:
                    try:
                        query = json.loads(query_str)
                    except:
                        print("Invalid JSON, using empty query")
                
                await tool.find_documents(query, limit)
                
            elif choice == '3':
                field = input("Enter field name (e.g., 'status', 'service_center_id'): ").strip()
                if field:
                    await tool.count_by_field(field)
                    
            elif choice == '4':
                search_term = input("Enter search term: ").strip()
                if search_term:
                    await tool.search_orders(search_term)
                    
            elif choice == '5':
                days = int(input("Days to look back (default 7): ") or "7")
                await tool.get_recent_orders(days)
                
            elif choice == '6':
                break
                
            else:
                print("Invalid choice")
                
            input("\nPress Enter to continue...")
            
    finally:
        await tool.cleanup()

async def main():
    """Main function"""
    import sys
    
    if len(sys.argv) > 1:
        # Command line mode
        command = sys.argv[1]
        tool = CosmosDBQueryTool()
        
        if not await tool.connect():
            return
        
        try:
            if command == 'stats':
                await tool.get_stats()
            elif command == 'recent':
                days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
                await tool.get_recent_orders(days)
            elif command == 'search':
                if len(sys.argv) > 2:
                    await tool.search_orders(sys.argv[2])
                else:
                    print("Usage: python query_tool.py search <term>")
            else:
                print("Available commands: stats, recent [days], search <term>")
                
        finally:
            await tool.cleanup()
    else:
        # Interactive mode
        await interactive_mode()

if __name__ == "__main__":
    asyncio.run(main())
