"""
Generator Factory System
Creates appropriate generators based on user input (JSON sample or Python generator)
"""
import logging
from typing import Dict, Any, Optional, Union
from pathlib import Path
import importlib.util
import sys

from .json_sample_generator import JSONSampleGenerator
from ..generators.engine import BaseDataGenerator, GeneratorType

logger = logging.getLogger(__name__)

class GeneratorFactory:
    """
    Factory for creating data generators based on user input
    
    Supports:
    - JSON sample files
    - Python generator files
    - Built-in domain generators
    """
    
    def __init__(self):
        self.builtin_generators = {
        }
    
    def create_generator(self, 
                        source: str, 
                        generator_type: str = "auto",
                        domain: Optional[str] = None) -> BaseDataGenerator:
        """
        Create a generator based on the source
        
        Args:
            source: Path to JSON sample file, Python generator file, or builtin generator name
            generator_type: Type of generator ("json", "python", "builtin", "auto")
            domain: Domain name for builtin generators
        
        Returns:
            BaseDataGenerator instance
        """
        source_path = Path(source)
        
        # Auto-detect generator type if not specified
        if generator_type == "auto":
            generator_type = self._detect_generator_type(source_path)
        
        logger.info(f"Creating {generator_type} generator from: {source}")
        
        if generator_type == "json":
            return self._create_json_generator(source_path)
        elif generator_type == "python":
            return self._create_python_generator(source_path)
        elif generator_type == "builtin":
            return self._create_builtin_generator(source, domain)
        else:
            raise ValueError(f"Unsupported generator type: {generator_type}")
    
    def _detect_generator_type(self, source_path: Path) -> str:
        """Auto-detect the generator type based on file extension and content"""
        if not source_path.exists():
            # Check if it's a builtin generator name
            if source_path.name in self.builtin_generators:
                return "builtin"
            raise FileNotFoundError(f"Source not found: {source_path}")
        
        # Check file extension
        if source_path.suffix.lower() == '.json':
            return "json"
        elif source_path.suffix.lower() == '.py':
            return "python"
        else:
            # Try to determine by content
            try:
                with open(source_path, 'r') as f:
                    content = f.read(100)  # Read first 100 characters
                    if content.strip().startswith('{'):
                        return "json"
                    elif 'class' in content and 'generate_document' in content:
                        return "python"
            except Exception:
                pass
            
            # Default to builtin if it matches a known generator
            if source_path.name in self.builtin_generators:
                return "builtin"
            
            raise ValueError(f"Cannot determine generator type for: {source_path}")
    
    def _create_json_generator(self, source_path: Path) -> 'JSONSampleGeneratorWrapper':
        """Create a generator from JSON sample file"""
        json_generator = JSONSampleGenerator(str(source_path))
        return JSONSampleGeneratorWrapper(json_generator)
    
    def _create_python_generator(self, source_path: Path) -> BaseDataGenerator:
        """Create a generator from Python file"""
        # Load the module
        spec = importlib.util.spec_from_file_location("custom_generator", source_path)
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
            raise ValueError(f"No valid generator class found in {source_path}")
        
        return generator_class()
    
    def _create_builtin_generator(self, generator_name: str, domain: Optional[str] = None) -> BaseDataGenerator:
        """Create a builtin generator"""
        if generator_name not in self.builtin_generators:
            available = list(self.builtin_generators.keys())
            raise ValueError(f"Unknown builtin generator: {generator_name}. Available: {available}")
        
        generator_class = self.builtin_generators[generator_name]
        return generator_class()
    
    def list_available_generators(self) -> Dict[str, Any]:
        """List all available generators"""
        return {
            'builtin': list(self.builtin_generators.keys()),
            'json_samples': self._find_json_samples(),
            'python_generators': self._find_python_generators()
        }
    
    def _find_json_samples(self) -> list:
        """Find JSON sample files in the templates directory"""
        templates_dir = Path("user_defined/templates")
        json_samples = []
        
        if templates_dir.exists():
            for json_file in templates_dir.rglob("*.json"):
                json_samples.append(str(json_file))
        
        return json_samples
    
    def _find_python_generators(self) -> list:
        """Find Python generator files"""
        generators_dir = Path("user_defined/generators")
        python_generators = []
        
        if generators_dir.exists():
            for py_file in generators_dir.rglob("*.py"):
                if py_file.name != "__init__.py":
                    python_generators.append(str(py_file))
        
        return python_generators

class JSONSampleGeneratorWrapper(BaseDataGenerator):
    """Wrapper to make JSONSampleGenerator compatible with BaseDataGenerator"""
    
    def __init__(self, json_generator: JSONSampleGenerator):
        super().__init__(GeneratorType.CUSTOM)
        self.json_generator = json_generator
        self.documents_generated = 0
        self.start_time = None
        self.metadata = json_generator.get_sample_info()
    
    def generate_document(self, document_id: Optional[str] = None) -> Dict[str, Any]:
        """Generate a single document"""
        doc = self.json_generator.generate_document(document_id)
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
            generator_type=self.generator_type,
            metadata=self.metadata
        )
    
    def get_generation_stats(self) -> Dict[str, Any]:
        """Get generation statistics"""
        elapsed = time.time() - self.start_time if self.start_time else 0
        rate = self.documents_generated / elapsed if elapsed > 0 else 0
        
        return {
            "generator_type": "json_sample",
            "sample_source": self.json_generator.sample_path,
            "schema_version": self.json_generator.get_schema_version(),
            "documents_generated": self.documents_generated,
            "elapsed_time": elapsed,
            "generation_rate": rate,
            "metadata": self.metadata
        }

# Import here to avoid circular imports
from ..generators.engine import GenerationBatch
import time
