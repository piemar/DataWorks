"""
Volvo Service Order Data Generator
Generates realistic Volvo service order data for testing and development
"""
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import random
import uuid
from bson import ObjectId
import bson

from framework.generators.engine import BaseDataGenerator, GeneratorType

logger = logging.getLogger(__name__)

class VolvoServiceOrderGenerator(BaseDataGenerator):
    """
    Volvo-specific service order data generator
    
    Generates realistic Volvo service order data with:
    - Proper relationships between entities
    - Realistic timestamps and durations
    - Volvo-specific part numbers and service types
    - Customer and vehicle information
    """
    
    def __init__(self):
        super().__init__(GeneratorType.SERVICE_ORDER)
        self.metadata = {
            "generator_version": "1.0.0",
            "data_model": "volvo_service_orders",
            "created_at": datetime.utcnow().isoformat()
        }
    
    def generate_document(self, document_id: Optional[str] = None) -> Dict[str, Any]:
        """Generate a single Volvo service order document"""
        try:
            # Generate service order directly
            service_order = self._generate_service_order()
            
            # Add framework metadata
            service_order["_framework_metadata"] = {
                "generated_at": datetime.utcnow().isoformat(),
                "generator_type": self.generator_type.value,
                "document_id": document_id or str(uuid.uuid4()),
                "version": "1.0.0"
            }
            
            # Only set _id if a specific document_id is provided
            # Otherwise, let MongoDB auto-generate it
            if document_id:
                service_order["_id"] = ObjectId(document_id) if bson.ObjectId.is_valid(document_id) else document_id
            
            return service_order
            
        except Exception as e:
            logger.error(f"Error generating service order document: {e}")
            # Return a minimal document on error
            return {
                "_id": ObjectId() if not document_id else (ObjectId(document_id) if bson.ObjectId.is_valid(document_id) else document_id),
                "error": str(e),
                "generated_at": datetime.utcnow().isoformat()
            }
    
    def _generate_service_order(self) -> Dict[str, Any]:
        """Generate a realistic Volvo service order"""
        # Generate realistic Volvo service order data
        order_id = f"SO-{random.randint(100000, 999999)}"
        customer_id = f"CUST-{random.randint(10000, 99999)}"
        vehicle_id = f"VIN-{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=17))}"
        
        # Generate service items
        service_items = []
        num_items = random.randint(1, 5)
        for i in range(num_items):
            item = {
                "item_id": f"ITEM-{random.randint(1000, 9999)}",
                "service_type": random.choice(["Oil Change", "Brake Service", "Engine Repair", "Transmission Service", "Tire Replacement"]),
                "description": f"Service item {i+1}",
                "quantity": random.randint(1, 3),
                "unit_price": round(random.uniform(50.0, 500.0), 2),
                "total_price": 0,  # Will be calculated
                "labor_hours": round(random.uniform(0.5, 8.0), 1),
                "parts_used": [f"PART-{random.randint(100, 999)}" for _ in range(random.randint(1, 3))]
            }
            item["total_price"] = round(item["unit_price"] * item["quantity"], 2)
            service_items.append(item)
        
        # Calculate totals
        total_amount = sum(item["total_price"] for item in service_items)
        total_labor_hours = sum(item["labor_hours"] for item in service_items)
        
        return {
            "order_id": order_id,
            "customer_id": customer_id,
            "vehicle_id": vehicle_id,
            "service_date": datetime.utcnow().isoformat(),
            "service_items": service_items,
            "total_amount": round(total_amount, 2),
            "total_labor_hours": round(total_labor_hours, 1),
            "status": random.choice(["Pending", "In Progress", "Completed", "Cancelled"]),
            "technician": f"TECH-{random.randint(100, 999)}",
            "notes": f"Service order for vehicle {vehicle_id}"
        }
    
    def get_generator_info(self) -> Dict[str, Any]:
        """Get information about this generator"""
        return {
            "name": "VolvoServiceOrderGenerator",
            "version": "1.0.0",
            "description": "Generates realistic Volvo service order data",
            "supported_types": ["service_order"],
            "metadata": self.metadata
        }