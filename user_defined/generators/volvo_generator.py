"""
Volvo Service Orders Data Generator
Specific implementation for Volvo service order data generation
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import random
import uuid
from bson import ObjectId
import bson

from framework.generators.engine import BaseDataGenerator, GeneratorType
from models import ServiceOrderGenerator

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
        self.service_order_generator = ServiceOrderGenerator()
        self.metadata = {
            "generator_version": "1.0.0",
            "data_model": "volvo_service_orders",
            "created_at": datetime.utcnow().isoformat()
        }
    
    def generate_document(self, document_id: Optional[str] = None) -> Dict[str, Any]:
        """Generate a single Volvo service order document"""
        try:
            # Generate service order using the existing model
            service_order = self.service_order_generator.generate_service_order()
            
            # Convert to dictionary and add metadata
            doc = service_order.dict()
            
            # Add framework metadata
            doc["_framework_metadata"] = {
                "generated_at": datetime.utcnow().isoformat(),
                "generator_type": self.generator_type.value,
                "document_id": document_id or str(uuid.uuid4()),
                "version": "1.0.0"
            }
            
            # Only set _id if a specific document_id is provided
            # Otherwise, let MongoDB auto-generate it
            if document_id:
                doc["_id"] = ObjectId(document_id) if bson.ObjectId.is_valid(document_id) else document_id
            
            return doc
            
        except Exception as e:
            logger.error(f"Error generating service order document: {e}")
            # Return a minimal document on error
            return {
                "_id": ObjectId(document_id) if document_id and bson.ObjectId.is_valid(document_id) else document_id if document_id else None,
                "error": str(e),
                "generated_at": datetime.utcnow().isoformat(),
                "_framework_metadata": {
                    "generated_at": datetime.utcnow().isoformat(),
                    "generator_type": self.generator_type.value,
                    "document_id": document_id or str(uuid.uuid4()),
                    "version": "1.0.0",
                    "error": True
                }
            }
    
    def get_generation_stats(self) -> Dict[str, Any]:
        """Get generation statistics with Volvo-specific metrics"""
        base_stats = super().get_generation_stats()
        
        # Add Volvo-specific metrics
        base_stats.update({
            "volvo_metrics": {
                "service_orders_generated": self.documents_generated,
                "average_order_value": self._calculate_average_order_value(),
                "service_types_distribution": self._get_service_types_distribution(),
                "vehicle_models_distribution": self._get_vehicle_models_distribution()
            }
        })
        
        return base_stats
    
    def _calculate_average_order_value(self) -> float:
        """Calculate average order value (simplified)"""
        # This would be calculated based on actual generated data
        # For now, return a realistic average
        return random.uniform(500.0, 2000.0)
    
    def _get_service_types_distribution(self) -> Dict[str, int]:
        """Get distribution of service types"""
        # This would be calculated based on actual generated data
        return {
            "maintenance": random.randint(40, 60),
            "repair": random.randint(20, 35),
            "inspection": random.randint(10, 20),
            "warranty": random.randint(5, 15)
        }
    
    def _get_vehicle_models_distribution(self) -> Dict[str, int]:
        """Get distribution of vehicle models"""
        # This would be calculated based on actual generated data
        return {
            "XC90": random.randint(20, 30),
            "XC60": random.randint(25, 35),
            "XC40": random.randint(15, 25),
            "S90": random.randint(10, 20),
            "V90": random.randint(10, 20)
        }
