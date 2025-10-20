"""
JSON Sample-Based Data Generator
Analyzes JSON samples and generates similar documents with realistic variations
"""
import json
import logging
import random
import uuid
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, timedelta
from bson import ObjectId
import bson
from pathlib import Path
import re

from faker import Faker

logger = logging.getLogger(__name__)

class JSONSampleAnalyzer:
    """Analyzes JSON samples to understand data patterns and types"""
    
    def __init__(self):
        self.faker = Faker()
        self.patterns = {
            'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
            'phone': r'^\+?[\d\s\-\(\)]+$',
            'uuid': r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
            'objectid': r'^[0-9a-f]{24}$',
            'date': r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z?$',
            'vin': r'^[A-HJ-NPR-Z0-9]{17}$',
            'license_plate': r'^[A-Z]{2,3}\d{2,4}$',
            'id_pattern': r'^[A-Z_]+_\d+$',
            'order_id': r'^SO_\d+$',
            'customer_id': r'^CUST_\d+$',
            'tech_id': r'^TECH_\d+$',
            'item_id': r'^ITEM_\d+$',
            'center_id': r'^CENTER_\d+$'
        }
    
    def analyze_value(self, value: Any, field_name: str = "") -> Dict[str, Any]:
        """Analyze a value and determine how to generate similar ones"""
        if value is None:
            return {"type": "null", "generator": "null"}
        
        if isinstance(value, bool):
            return {"type": "boolean", "generator": "boolean"}
        
        if isinstance(value, int):
            return self._analyze_integer(value, field_name)
        
        if isinstance(value, float):
            return self._analyze_float(value, field_name)
        
        if isinstance(value, str):
            return self._analyze_string(value, field_name)
        
        if isinstance(value, list):
            return self._analyze_list(value, field_name)
        
        if isinstance(value, dict):
            return self._analyze_object(value, field_name)
        
        return {"type": "string", "generator": "random"}
    
    def _analyze_integer(self, value: int, field_name: str) -> Dict[str, Any]:
        """Analyze integer values"""
        field_lower = field_name.lower()
        
        if 'year' in field_lower:
            return {"type": "integer", "generator": "range", "min": 1990, "max": 2024}
        elif 'quantity' in field_lower or 'count' in field_lower:
            return {"type": "integer", "generator": "range", "min": 1, "max": 10}
        elif 'mileage' in field_lower:
            return {"type": "integer", "generator": "range", "min": 1000, "max": 200000}
        else:
            # Estimate range based on value
            if value < 10:
                return {"type": "integer", "generator": "range", "min": 1, "max": 10}
            elif value < 100:
                return {"type": "integer", "generator": "range", "min": 1, "max": 100}
            elif value < 1000:
                return {"type": "integer", "generator": "range", "min": 1, "max": 1000}
            else:
                return {"type": "integer", "generator": "range", "min": value // 2, "max": value * 2}
    
    def _analyze_float(self, value: float, field_name: str) -> Dict[str, Any]:
        """Analyze float values"""
        field_lower = field_name.lower()
        
        if 'price' in field_lower or 'cost' in field_lower or 'amount' in field_lower:
            return {"type": "float", "generator": "range", "min": 10.0, "max": 2000.0}
        elif 'hours' in field_lower:
            return {"type": "float", "generator": "range", "min": 0.5, "max": 8.0}
        elif 'rate' in field_lower:
            return {"type": "float", "generator": "range", "min": 50.0, "max": 200.0}
        else:
            # Estimate range based on value
            if value < 1:
                return {"type": "float", "generator": "range", "min": 0.0, "max": 1.0}
            elif value < 10:
                return {"type": "float", "generator": "range", "min": 0.0, "max": 10.0}
            elif value < 100:
                return {"type": "float", "generator": "range", "min": 0.0, "max": 100.0}
            else:
                return {"type": "float", "generator": "range", "min": value * 0.5, "max": value * 1.5}
    
    def _analyze_string(self, value: str, field_name: str) -> Dict[str, Any]:
        """Analyze string values"""
        field_lower = field_name.lower()
        
        # Check for specific patterns
        for pattern_name, pattern in self.patterns.items():
            if re.match(pattern, value):
                return self._get_pattern_generator(pattern_name, value)
        
        # Check field names for hints
        if 'email' in field_lower:
            return {"type": "string", "generator": "faker", "faker_method": "email"}
        elif 'phone' in field_lower:
            return {"type": "string", "generator": "faker", "faker_method": "phone_number"}
        elif 'name' in field_lower and 'customer' in field_lower:
            return {"type": "string", "generator": "faker", "faker_method": "name"}
        elif 'name' in field_lower:
            return {"type": "string", "generator": "faker", "faker_method": "name"}
        elif 'description' in field_lower:
            return {"type": "string", "generator": "faker", "faker_method": "sentence"}
        elif 'address' in field_lower:
            return {"type": "string", "generator": "faker", "faker_method": "address"}
        elif 'city' in field_lower:
            return {"type": "string", "generator": "faker", "faker_method": "city"}
        elif 'country' in field_lower:
            return {"type": "string", "generator": "faker", "faker_method": "country"}
        elif 'url' in field_lower or 'link' in field_lower:
            return {"type": "string", "generator": "faker", "faker_method": "url"}
        elif 'status' in field_lower:
            return {"type": "string", "generator": "enum", "options": ["active", "inactive", "pending", "completed", "cancelled"]}
        elif 'type' in field_lower:
            return {"type": "string", "generator": "enum", "options": ["standard", "premium", "basic", "advanced"]}
        elif 'make' in field_lower:
            return {"type": "string", "generator": "enum", "options": ["Volvo", "BMW", "Mercedes", "Audi", "Toyota", "Ford", "Chevrolet"]}
        elif 'model' in field_lower:
            return {"type": "string", "generator": "enum", "options": ["XC90", "XC60", "XC40", "S90", "V90", "S60", "V60", "X5", "A4", "Camry"]}
        elif 'service_type' in field_lower:
            return {"type": "string", "generator": "enum", "options": ["oil_change", "maintenance", "repair", "inspection", "warranty", "recall"]}
        elif 'customer_type' in field_lower:
            return {"type": "string", "generator": "enum", "options": ["individual", "business", "fleet", "government"]}
        elif 'warranty_type' in field_lower:
            return {"type": "string", "generator": "enum", "options": ["standard", "extended", "premium", "basic"]}
        else:
            # Try to infer from value content
            if '@' in value:
                return {"type": "string", "generator": "faker", "faker_method": "email"}
            elif value.startswith('+') or any(c.isdigit() for c in value):
                return {"type": "string", "generator": "faker", "faker_method": "phone_number"}
            elif len(value) > 50:
                return {"type": "string", "generator": "faker", "faker_method": "text", "max_nb_chars": len(value)}
            else:
                return {"type": "string", "generator": "faker", "faker_method": "word"}
    
    def _analyze_list(self, value: List, field_name: str) -> Dict[str, Any]:
        """Analyze list values"""
        if not value:
            return {"type": "array", "generator": "array", "size": {"min": 0, "max": 3}}
        
        # Analyze first item to understand structure
        first_item = value[0]
        item_schema = self.analyze_value(first_item, f"{field_name}_item")
        
        return {
            "type": "array",
            "generator": "array",
            "size": {"min": 1, "max": len(value) + 2},
            "item_schema": item_schema
        }
    
    def _analyze_object(self, value: Dict, field_name: str) -> Dict[str, Any]:
        """Analyze object values"""
        schema = {}
        for key, val in value.items():
            schema[key] = self.analyze_value(val, key)
        
        return {
            "type": "object",
            "generator": "object",
            "schema": schema
        }
    
    def _get_pattern_generator(self, pattern_name: str, value: str) -> Dict[str, Any]:
        """Get generator configuration for specific patterns"""
        generators = {
            'email': {"type": "string", "generator": "faker", "faker_method": "email"},
            'phone': {"type": "string", "generator": "faker", "faker_method": "phone_number"},
            'uuid': {"type": "string", "generator": "uuid"},
            'objectid': {"type": "string", "generator": "objectid"},
            'date': {"type": "string", "generator": "date", "start_date": "2020-01-01", "end_date": "2024-12-31"},
            'vin': {"type": "string", "generator": "faker", "faker_method": "bothify", "faker_args": "1HGBH41JXMN109186"},
            'license_plate': {"type": "string", "generator": "faker", "faker_method": "bothify", "faker_args": "???###"},
            'id_pattern': {"type": "string", "generator": "faker", "faker_method": "bothify", "faker_args": "ID_#####"},
            'order_id': {"type": "string", "generator": "faker", "faker_method": "bothify", "faker_args": "SO_##########"},
            'customer_id': {"type": "string", "generator": "faker", "faker_method": "bothify", "faker_args": "CUST_########"},
            'tech_id': {"type": "string", "generator": "faker", "faker_method": "bothify", "faker_args": "TECH_####"},
            'item_id': {"type": "string", "generator": "faker", "faker_method": "bothify", "faker_args": "ITEM_######"},
            'center_id': {"type": "string", "generator": "faker", "faker_method": "bothify", "faker_args": "CENTER_##"}
        }
        
        return generators.get(pattern_name, {"type": "string", "generator": "random"})

class JSONSampleGenerator:
    """Generates documents based on JSON sample analysis"""
    
    def __init__(self, sample_path: str):
        self.sample_path = Path(sample_path)
        self.sample_data = self._load_sample()
        self.analyzer = JSONSampleAnalyzer()
        self.schema = self._analyze_sample()
        self.faker = Faker()
    
    def _load_sample(self) -> Dict[str, Any]:
        """Load JSON sample from file"""
        if not self.sample_path.exists():
            raise FileNotFoundError(f"Sample file not found: {self.sample_path}")
        
        with open(self.sample_path, 'r') as f:
            return json.load(f)
    
    def _analyze_sample(self) -> Dict[str, Any]:
        """Analyze the sample to create generation schema"""
        schema = {}
        for key, value in self.sample_data.items():
            schema[key] = self.analyzer.analyze_value(value, key)
        
        return schema
    
    def generate_document(self, document_id: Optional[str] = None) -> Dict[str, Any]:
        """Generate a document based on the analyzed sample"""
        doc = {}
        
        for field_name, field_config in self.schema.items():
            doc[field_name] = self._generate_field_value(field_config, field_name)
        
        # Only set _id if a specific document_id is provided
        # Otherwise, let MongoDB auto-generate it
        if document_id:
            doc['_id'] = ObjectId(document_id) if bson.ObjectId.is_valid(document_id) else document_id
        
        # Add generation metadata with schema version
        doc['_generation_metadata'] = {
            'generated_at': datetime.utcnow().isoformat(),
            'sample_source': str(self.sample_path),
            'generator_type': 'json_sample',
            'schema_version': self.get_schema_version(),
            'framework_version': '1.0.0'
        }
        
        return doc
    
    def _generate_field_value(self, field_config: Dict[str, Any], field_name: str) -> Any:
        """Generate a value for a specific field based on its configuration"""
        generator = field_config.get('generator', 'random')
        
        if generator == 'null':
            return None
        elif generator == 'boolean':
            return random.choice([True, False])
        elif generator == 'range':
            return self._generate_range_value(field_config)
        elif generator == 'enum':
            return random.choice(field_config.get('options', ['option1', 'option2']))
        elif generator == 'faker':
            return self._generate_faker_value(field_config)
        elif generator == 'uuid':
            return str(uuid.uuid4())
        elif generator == 'objectid':
            return str(ObjectId())  # Proper ObjectId
        elif generator == 'date':
            return self._generate_date_value(field_config)
        elif generator == 'array':
            return self._generate_array_value(field_config, field_name)
        elif generator == 'object':
            return self._generate_object_value(field_config)
        else:
            return self._generate_random_value(field_config)
    
    def _generate_range_value(self, field_config: Dict[str, Any]) -> Union[int, float]:
        """Generate a value within a range"""
        min_val = field_config.get('min', 0)
        max_val = field_config.get('max', 100)
        
        if field_config.get('type') == 'integer':
            return random.randint(min_val, max_val)
        else:
            return round(random.uniform(min_val, max_val), 2)
    
    def _generate_faker_value(self, field_config: Dict[str, Any]) -> str:
        """Generate value using Faker"""
        method = field_config.get('faker_method', 'word')
        args = field_config.get('faker_args')
        
        if args:
            return getattr(self.faker, method)(args)
        else:
            return getattr(self.faker, method)()
    
    def _generate_date_value(self, field_config: Dict[str, Any]) -> str:
        """Generate date value"""
        start_date = field_config.get('start_date', '2020-01-01')
        end_date = field_config.get('end_date', '2024-12-31')
        
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        random_date = start + timedelta(days=random.randint(0, (end - start).days))
        return random_date.isoformat() + 'Z'
    
    def _generate_array_value(self, field_config: Dict[str, Any], field_name: str) -> List:
        """Generate array value"""
        size_config = field_config.get('size', {'min': 1, 'max': 3})
        min_size = size_config.get('min', 1)
        max_size = size_config.get('max', 3)
        
        size = random.randint(min_size, max_size)
        items = []
        
        item_schema = field_config.get('item_schema')
        if item_schema:
            for _ in range(size):
                items.append(self._generate_field_value(item_schema, f"{field_name}_item"))
        else:
            # Generate simple items
            for _ in range(size):
                items.append(self.faker.word())
        
        return items
    
    def _generate_object_value(self, field_config: Dict[str, Any]) -> Dict:
        """Generate object value"""
        obj = {}
        schema = field_config.get('schema', {})
        
        for key, value_config in schema.items():
            obj[key] = self._generate_field_value(value_config, key)
        
        return obj
    
    def _generate_random_value(self, field_config: Dict[str, Any]) -> Any:
        """Generate random value based on type"""
        field_type = field_config.get('type', 'string')
        
        if field_type == 'string':
            return self.faker.word()
        elif field_type == 'integer':
            return random.randint(1, 1000)
        elif field_type == 'float':
            return round(random.uniform(0, 100), 2)
        elif field_type == 'boolean':
            return random.choice([True, False])
        else:
            return self.faker.word()
    
    def get_schema_version(self) -> str:
        """Get schema version based on sample analysis"""
        # Create a hash of the schema structure for versioning
        schema_hash = hash(json.dumps(self.schema, sort_keys=True))
        return f"1.0.{abs(schema_hash) % 10000}"
    
    def get_sample_info(self) -> Dict[str, Any]:
        """Get information about the sample"""
        return {
            'sample_path': str(self.sample_path),
            'sample_size': len(json.dumps(self.sample_data)),
            'fields_count': len(self.schema),
            'field_types': {k: v.get('type', 'unknown') for k, v in self.schema.items()},
            'generators_used': {k: v.get('generator', 'unknown') for k, v in self.schema.items()},
            'schema_version': self.get_schema_version()
        }
