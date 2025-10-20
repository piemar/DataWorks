"""
Template-Based Data Generation System
Supports JSON templates and custom Python generators for flexible data generation
"""
import json
import logging
import random
import uuid
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Union
from bson import ObjectId
import bson
from datetime import datetime, timedelta
from pathlib import Path
import importlib.util
import sys

from faker import Faker

logger = logging.getLogger(__name__)

class TemplateField:
    """Represents a field in a template with generation rules"""
    
    def __init__(self, field_name: str, field_config: Dict[str, Any]):
        self.field_name = field_name
        self.field_config = field_config
        self.faker = Faker()
    
    def generate_value(self) -> Any:
        """Generate a value based on field configuration"""
        field_type = self.field_config.get('type', 'string')
        generator = self.field_config.get('generator', 'random')
        
        if generator == 'faker':
            return self._generate_faker_value(field_type)
        elif generator == 'random':
            return self._generate_random_value(field_type)
        elif generator == 'enum':
            return self._generate_enum_value()
        elif generator == 'range':
            return self._generate_range_value()
        elif generator == 'date':
            return self._generate_date_value()
        elif generator == 'uuid':
            return str(uuid.uuid4())
        elif generator == 'objectid':
            return str(ObjectId())  # Proper ObjectId
        else:
            return self._generate_default_value(field_type)
    
    def _generate_faker_value(self, field_type: str) -> Any:
        """Generate value using Faker"""
        faker_method = self.field_config.get('faker_method')
        if faker_method:
            return getattr(self.faker, faker_method)()
        
        # Default faker methods by type
        faker_map = {
            'string': 'word',
            'email': 'email',
            'name': 'name',
            'address': 'address',
            'phone': 'phone_number',
            'company': 'company',
            'text': 'text',
            'url': 'url',
            'integer': 'random_int',
            'float': 'pyfloat',
            'boolean': 'boolean',
            'date': 'date',
            'datetime': 'date_time'
        }
        
        method = faker_map.get(field_type, 'word')
        return getattr(self.faker, method)()
    
    def _generate_random_value(self, field_type: str) -> Any:
        """Generate random value based on type"""
        if field_type == 'string':
            return self.faker.word()
        elif field_type == 'integer':
            min_val = self.field_config.get('min', 1)
            max_val = self.field_config.get('max', 1000)
            return random.randint(min_val, max_val)
        elif field_type == 'float':
            min_val = self.field_config.get('min', 0.0)
            max_val = self.field_config.get('max', 100.0)
            return round(random.uniform(min_val, max_val), 2)
        elif field_type == 'boolean':
            return random.choice([True, False])
        elif field_type == 'array':
            size = self.field_config.get('size', random.randint(1, 5))
            return [self.faker.word() for _ in range(size)]
        else:
            return self.faker.word()
    
    def _generate_enum_value(self) -> Any:
        """Generate value from enum options"""
        options = self.field_config.get('options', ['option1', 'option2'])
        return random.choice(options)
    
    def _generate_range_value(self) -> Any:
        """Generate value within a range"""
        min_val = self.field_config.get('min', 0)
        max_val = self.field_config.get('max', 100)
        return random.randint(min_val, max_val)
    
    def _generate_date_value(self) -> str:
        """Generate date value"""
        start_date = self.field_config.get('start_date', '2020-01-01')
        end_date = self.field_config.get('end_date', '2024-12-31')
        
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        random_date = start + timedelta(days=random.randint(0, (end - start).days))
        return random_date.isoformat()
    
    def _generate_default_value(self, field_type: str) -> Any:
        """Generate default value based on type"""
        defaults = {
            'string': 'default_string',
            'integer': 0,
            'float': 0.0,
            'boolean': False,
            'array': [],
            'object': {},
            'null': None
        }
        return defaults.get(field_type, 'default_value')

class JSONTemplateGenerator:
    """Generator that creates data based on JSON templates"""
    
    def __init__(self, template_path: str):
        self.template_path = Path(template_path)
        self.template_config = self._load_template()
        self.fields = self._parse_fields()
    
    def _load_template(self) -> Dict[str, Any]:
        """Load template configuration from JSON file"""
        if not self.template_path.exists():
            raise FileNotFoundError(f"Template file not found: {self.template_path}")
        
        with open(self.template_path, 'r') as f:
            return json.load(f)
    
    def _parse_fields(self) -> List[TemplateField]:
        """Parse fields from template configuration"""
        fields = []
        schema = self.template_config.get('schema', {})
        
        for field_name, field_config in schema.items():
            fields.append(TemplateField(field_name, field_config))
        
        return fields
    
    def generate_document(self, document_id: Optional[str] = None) -> Dict[str, Any]:
        """Generate a document based on the template"""
        doc = {}
        
        # Generate values for each field
        for field in self.fields:
            doc[field.field_name] = field.generate_value()
        
        # Only set _id if a specific document_id is provided
        # Otherwise, let MongoDB auto-generate it
        if document_id:
            doc['_id'] = ObjectId(document_id) if bson.ObjectId.is_valid(document_id) else document_id
        doc['_template_metadata'] = {
            'template_name': self.template_config.get('name', 'unknown'),
            'template_version': self.template_config.get('version', '1.0.0'),
            'generated_at': datetime.utcnow().isoformat(),
            'generator_type': 'json_template'
        }
        
        return doc

class PythonGeneratorLoader:
    """Loader for custom Python generators"""
    
    @staticmethod
    def load_generator(generator_path: str) -> Any:
        """Load a custom Python generator from file"""
        generator_file = Path(generator_path)
        
        if not generator_file.exists():
            raise FileNotFoundError(f"Generator file not found: {generator_path}")
        
        # Load the module
        spec = importlib.util.spec_from_file_location("custom_generator", generator_file)
        module = importlib.util.module_from_spec(spec)
        sys.modules["custom_generator"] = module
        spec.loader.exec_module(module)
        
        # Find the generator class
        generator_class = None
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (isinstance(attr, type) and 
                hasattr(attr, 'generate_document') and 
                attr_name != 'BaseDataGenerator'):
                generator_class = attr
                break
        
        if not generator_class:
            raise ValueError(f"No valid generator class found in {generator_path}")
        
        return generator_class()

class TemplateBasedGenerator:
    """Main template-based generator that can use JSON templates or Python generators"""
    
    def __init__(self, template_source: str, generator_type: str = "json"):
        self.template_source = template_source
        self.generator_type = generator_type
        self.generator = None
        self.documents_generated = 0
        self.start_time = None
        self.metadata = {}
        
        self._initialize_generator()
    
    def _initialize_generator(self):
        """Initialize the appropriate generator based on type"""
        if self.generator_type.lower() == "json":
            self.generator = JSONTemplateGenerator(self.template_source)
            self.metadata = {
                "generator_type": "json_template",
                "template_source": self.template_source,
                "created_at": datetime.utcnow().isoformat()
            }
        elif self.generator_type.lower() == "python":
            self.generator = PythonGeneratorLoader.load_generator(self.template_source)
            self.metadata = {
                "generator_type": "python_generator",
                "generator_source": self.template_source,
                "created_at": datetime.utcnow().isoformat()
            }
        else:
            raise ValueError(f"Unsupported generator type: {self.generator_type}")
    
    def generate_document(self, document_id: Optional[str] = None) -> Dict[str, Any]:
        """Generate a single document"""
        doc = self.generator.generate_document(document_id)
        self.documents_generated += 1
        return doc
    
    def generate_batch(self, batch_size: int, batch_number: int) -> 'GenerationBatch':
        """Generate a batch of documents"""
        documents = []
        for i in range(batch_size):
            doc = self.generate_document()
            documents.append(doc)
        
        return GenerationBatch(
            documents=documents,
            batch_number=batch_number,
            generator_type=GeneratorType.CUSTOM,
            metadata=self.metadata
        )
    
    def get_generation_stats(self) -> Dict[str, Any]:
        """Get generation statistics"""
        elapsed = time.time() - self.start_time if self.start_time else 0
        rate = self.documents_generated / elapsed if elapsed > 0 else 0
        
        return {
            "generator_type": self.metadata.get("generator_type", "unknown"),
            "template_source": self.template_source,
            "documents_generated": self.documents_generated,
            "elapsed_time": elapsed,
            "generation_rate": rate,
            "metadata": self.metadata
        }

# Import here to avoid circular imports
from ..generators.engine import GenerationBatch, GeneratorType
import time
