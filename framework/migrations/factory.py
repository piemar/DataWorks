"""
Migration Strategy Factory
Creates appropriate migration strategies based on user input
"""
import logging
from typing import Dict, Any, Optional, Callable
from pathlib import Path
import importlib.util
import sys

from .default_strategy import DefaultMigrationStrategy
from ..core.database import BaseDatabaseClient

logger = logging.getLogger(__name__)

class MigrationStrategyFactory:
    """
    Factory for creating migration strategies
    
    Supports:
    - Default strategy (simple copy)
    - Builtin domain strategies
    - Custom Python strategies
    - Strategy configuration
    """
    
    def __init__(self):
        self.builtin_strategies = {
            'default': DefaultMigrationStrategy,
        }
    
    def create_strategy(self, 
                       strategy_name: str = "default",
                       source_client: BaseDatabaseClient = None,
                       target_client: BaseDatabaseClient = None,
                       config: Optional[Dict[str, Any]] = None) -> 'BaseMigrationStrategy':
        """
        Create a migration strategy
        
        Args:
            strategy_name: Name of strategy ("default", "volvo", or path to custom file)
            source_client: Source database client
            target_client: Target database client
            config: Strategy configuration
        
        Returns:
            BaseMigrationStrategy instance
        """
        if not source_client or not target_client:
            raise ValueError("Source and target clients are required")
        
        config = config or {}
        
        logger.info(f"Creating migration strategy: {strategy_name}")
        
        # Check if it's a builtin strategy
        if strategy_name in self.builtin_strategies:
            return self._create_builtin_strategy(strategy_name, source_client, target_client, config)
        
        # Check if it's a custom Python file
        strategy_path = Path(strategy_name)
        if strategy_path.exists() and strategy_path.suffix == '.py':
            return self._create_custom_strategy(strategy_path, source_client, target_client, config)
        
        # Default to default strategy
        logger.warning(f"Unknown strategy '{strategy_name}', using default strategy")
        return self._create_builtin_strategy("default", source_client, target_client, config)
    
    def _create_builtin_strategy(self, strategy_name: str, source_client: BaseDatabaseClient, 
                                target_client: BaseDatabaseClient, config: Dict[str, Any]) -> 'BaseMigrationStrategy':
        """Create a builtin strategy"""
        strategy_class = self.builtin_strategies[strategy_name]
        
        # Extract configuration parameters
        resume_field = config.get('resume_field', '_id')
        transform_document = config.get('transform_document')
        
        if strategy_name == 'default':
            return strategy_class(source_client, target_client, resume_field=resume_field, transform_document=transform_document)
        else:
            # For other builtin strategies, pass config as kwargs
            return strategy_class(source_client, target_client)
    
    def _create_custom_strategy(self, strategy_path: Path, source_client: BaseDatabaseClient,
                               target_client: BaseDatabaseClient, config: Dict[str, Any]) -> 'BaseMigrationStrategy':
        """Create a custom strategy from Python file"""
        # Load the module
        spec = importlib.util.spec_from_file_location("custom_strategy", strategy_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules["custom_strategy"] = module
        spec.loader.exec_module(module)
        
        # Find the strategy class
        strategy_class = None
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (isinstance(attr, type) and 
                hasattr(attr, 'read_batch') and 
                hasattr(attr, 'get_resume_point') and
                attr_name != 'BaseMigrationStrategy'):
                strategy_class = attr
                break
        
        if not strategy_class:
            raise ValueError(f"No valid migration strategy class found in {strategy_path}")
        
        # Create instance with config - only pass supported parameters
        try:
            # Try with config first
            return strategy_class(source_client, target_client, **config)
        except TypeError as e:
            if "unexpected keyword argument" in str(e):
                # Fall back to basic constructor if config parameters not supported
                logger.warning(f"Strategy {strategy_class.__name__} doesn't support config parameters, using basic constructor")
                return strategy_class(source_client, target_client)
            else:
                raise e
    
    def list_available_strategies(self) -> Dict[str, Any]:
        """List all available strategies"""
        return {
            'builtin': list(self.builtin_strategies.keys()),
            'custom_files': self._find_custom_strategies()
        }
    
    def _find_custom_strategies(self) -> list:
        """Find custom strategy files"""
        strategies_dir = Path("user_defined/strategies")
        custom_strategies = []
        
        if strategies_dir.exists():
            for py_file in strategies_dir.rglob("*.py"):
                if py_file.name != "__init__.py":
                    custom_strategies.append(str(py_file))
        
        return custom_strategies

# Import here to avoid circular imports
from ..migrations.engine import BaseMigrationStrategy
