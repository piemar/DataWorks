"""
Configuration Management Framework
Enterprise-ready configuration system with validation, environment variable support, and type safety
"""
import os
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union
from enum import Enum
from pathlib import Path
import json
import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class Environment(Enum):
    """Environment types"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TESTING = "testing"

@dataclass
class DatabaseSettings:
    """Database configuration settings - ULTRA-FAST optimized"""
    connection_string: str
    database_name: str
    collection_name: str
    batch_size: int = 10000  # Increased from 1000 to 10000
    max_pool_size: int = 200  # Increased from 100 to 200
    min_pool_size: int = 20   # Increased from 10 to 20
    max_idle_time_ms: int = 300000
    socket_timeout_ms: int = 30000
    connect_timeout_ms: int = 20000
    warmup_connections: int = 50  # Increased from 10 to 50

@dataclass
class WorkerSettings:
    """Worker configuration settings - ULTRA-FAST optimized"""
    max_workers: int = 30
    min_workers: int = 4
    write_workers: int = 20  # Increased from 5 to 20
    generation_workers: int = 8
    queue_multiplier: int = 10
    write_queue_multiplier: int = 10

@dataclass
class PerformanceSettings:
    """Performance optimization settings - ULTRA-FAST optimized"""
    batch_aggregation_size: int = 5
    batch_aggregation_timeout_ms: int = 200
    progress_update_interval: int = 10
    progress_smoothing: float = 0.1
    progress_miniters: int = 1000
    max_retries: int = 3
    backoff_base_seconds: float = 0.1
    read_ahead_batches: int = 10000  # ULTRA-FAST: Increased from 100
    read_ahead_workers: int = 4  # ULTRA-FAST: Multiple parallel cursors
    max_concurrent_batches: int = 50  # ULTRA-FAST: Increased from 20

@dataclass
class MonitoringSettings:
    """Monitoring and metrics settings"""
    enable_metrics: bool = True
    metrics_interval_seconds: int = 10
    performance_window_size: int = 20
    performance_check_interval_seconds: int = 30
    throttling_threshold_percentage: float = 10.0
    scaling_cooldown_seconds: int = 60
    scale_up_cooldown_seconds: int = 30

@dataclass
class DataGenerationConfig:
    """Data generation configuration"""
    total_documents: int = 1000000
    document_size_kb: float = 2.5
    document_size_multiplier: int = 1024
    ru_per_document: int = 6
    ru_check_interval_seconds: int = 10
    monitoring_batch_interval: int = 10
    ru_throttle_threshold: int = 40000
    ru_throttle_percentage: int = 90
    disable_throttling: bool = True

@dataclass
class MigrationConfig:
    """Migration configuration"""
    resume_from_checkpoint: bool = True
    parallel_cursors: int = 4
    max_insert_workers: int = 20
    read_ahead_batches: int = 100
    read_ahead_workers: int = 1
    max_concurrent_batches: int = 20
    fast_start: bool = False

@dataclass
class FrameworkConfig:
    """Main framework configuration"""
    environment: Environment = Environment.DEVELOPMENT
    log_level: str = "INFO"
    config_file: Optional[str] = None
    
    # Database configurations
    source_database: DatabaseSettings = field(default_factory=DatabaseSettings)
    target_database: DatabaseSettings = field(default_factory=DatabaseSettings)
    
    # Worker configurations
    workers: WorkerSettings = field(default_factory=WorkerSettings)
    
    # Performance configurations
    performance: PerformanceSettings = field(default_factory=PerformanceSettings)
    
    # Monitoring configurations
    monitoring: MonitoringSettings = field(default_factory=MonitoringSettings)
    
    # Feature-specific configurations
    data_generation: DataGenerationConfig = field(default_factory=DataGenerationConfig)
    migration: MigrationConfig = field(default_factory=MigrationConfig)
    
    # Custom settings
    custom_settings: Dict[str, Any] = field(default_factory=dict)

class ConfigManager:
    """
    Enterprise configuration manager with support for:
    - Environment variables
    - Configuration files (JSON/YAML)
    - Validation
    - Type safety
    - Environment-specific overrides
    """
    
    def __init__(self, config_prefix: str = "FRAMEWORK"):
        self.config_prefix = config_prefix
        self.config: Optional[FrameworkConfig] = None
        self._load_environment_variables()
    
    def _load_environment_variables(self):
        """Load environment variables from .env files"""
        # Try to load from multiple sources
        env_files = ['.env_local', '.env', 'config.env']
        for env_file in env_files:
            if Path(env_file).exists():
                load_dotenv(env_file)
                logger.info(f"Loaded environment variables from {env_file}")
                break
    
    def load_config(self, config_file: Optional[str] = None) -> FrameworkConfig:
        """Load configuration from file and environment variables"""
        config_data = {}
        
        # Load from file if provided
        if config_file and Path(config_file).exists():
            file_path = Path(config_file)
            
            # Handle dotenv files
            if (file_path.suffix.lower() in ['.env'] or 
                file_path.name.startswith('.env') or 
                file_path.name.endswith('.env')):
                load_dotenv(config_file)
            else:
                config_data.update(self._load_config_file(config_file))
        
        # Load from environment variables
        env_config = self._load_from_environment()
        config_data.update(env_config)
        
        # Create configuration object
        self.config = self._create_config_object(config_data)
        
        # Validate configuration
        self._validate_config(self.config)
        
        logger.info(f"Configuration loaded for {self.config.environment.value} environment")
        return self.config
    
    def _load_config_file(self, config_file: str) -> Dict[str, Any]:
        """Load configuration from JSON, YAML, or dotenv file"""
        file_path = Path(config_file)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
        
        with open(file_path, 'r') as f:
            if file_path.suffix.lower() == '.json':
                return json.load(f)
            elif file_path.suffix.lower() in ['.yml', '.yaml']:
                return yaml.safe_load(f)
            elif (file_path.suffix.lower() in ['.env'] or 
                  file_path.name.startswith('.env') or 
                  file_path.name.endswith('.env')):
                # For dotenv files, we don't return a dict here
                # The environment variables are loaded by load_dotenv() in load_config()
                return {}
            else:
                raise ValueError(f"Unsupported configuration file format: {file_path.suffix}")
    
    def _load_from_environment(self) -> Dict[str, Any]:
        """Load configuration from environment variables"""
        config = {}
        
        # Environment
        env = os.getenv(f"{self.config_prefix}_ENVIRONMENT", "development")
        config["environment"] = Environment(env)
        
        # Log level
        config["log_level"] = os.getenv(f"{self.config_prefix}_LOG_LEVEL", "INFO")
        
        # Source database
        config["source_database"] = {
            "connection_string": os.getenv(f"{self.config_prefix}_SOURCE_DB_CONNECTION_STRING") or os.getenv("GEN_DB_CONNECTION_STRING", ""),
            "database_name": os.getenv(f"{self.config_prefix}_SOURCE_DB_NAME") or os.getenv("GEN_DB_NAME", "volvo-service-orders"),
            "collection_name": os.getenv(f"{self.config_prefix}_SOURCE_DB_COLLECTION") or os.getenv("GEN_DB_COLLECTION", "serviceorders"),
            "batch_size": int(os.getenv(f"{self.config_prefix}_SOURCE_DB_BATCH_SIZE") or os.getenv("GEN_CURSOR_BATCH_SIZE", "1000")),
            "max_pool_size": int(os.getenv(f"{self.config_prefix}_SOURCE_DB_MAX_POOL_SIZE", "100")),
            "min_pool_size": int(os.getenv(f"{self.config_prefix}_SOURCE_DB_MIN_POOL_SIZE", "10")),
            "max_idle_time_ms": int(os.getenv(f"{self.config_prefix}_SOURCE_DB_MAX_IDLE_TIME_MS", "300000")),
            "socket_timeout_ms": int(os.getenv(f"{self.config_prefix}_SOURCE_DB_SOCKET_TIMEOUT_MS") or os.getenv("GEN_SOCKET_TIMEOUT_MS", "30000")),
            "connect_timeout_ms": int(os.getenv(f"{self.config_prefix}_SOURCE_DB_CONNECT_TIMEOUT_MS") or os.getenv("GEN_CONNECT_TIMEOUT_MS", "20000")),
            "warmup_connections": int(os.getenv(f"{self.config_prefix}_SOURCE_DB_WARMUP_CONNECTIONS", "10"))
        }
        
        # Target database
        config["target_database"] = {
            "connection_string": os.getenv(f"{self.config_prefix}_TARGET_DB_CONNECTION_STRING") or os.getenv("MIG_TARGET_DB_CONNECTION_STRING", ""),
            "database_name": os.getenv(f"{self.config_prefix}_TARGET_DB_NAME") or os.getenv("MIG_TARGET_DB_NAME", "volvo-service-orders"),
            "collection_name": os.getenv(f"{self.config_prefix}_TARGET_DB_COLLECTION") or os.getenv("MIG_TARGET_DB_COLLECTION", "serviceorders"),
            "batch_size": int(os.getenv(f"{self.config_prefix}_TARGET_DB_BATCH_SIZE", "1000")),
            "max_pool_size": int(os.getenv(f"{self.config_prefix}_TARGET_DB_MAX_POOL_SIZE", "100")),
            "min_pool_size": int(os.getenv(f"{self.config_prefix}_TARGET_DB_MIN_POOL_SIZE", "10")),
            "max_idle_time_ms": int(os.getenv(f"{self.config_prefix}_TARGET_DB_MAX_IDLE_TIME_MS", "300000")),
            "socket_timeout_ms": int(os.getenv(f"{self.config_prefix}_TARGET_DB_SOCKET_TIMEOUT_MS") or os.getenv("MIG_SOCKET_TIMEOUT_MS", "30000")),
            "connect_timeout_ms": int(os.getenv(f"{self.config_prefix}_TARGET_DB_CONNECT_TIMEOUT_MS") or os.getenv("MIG_CONNECT_TIMEOUT_MS", "20000")),
            "warmup_connections": int(os.getenv(f"{self.config_prefix}_TARGET_DB_WARMUP_CONNECTIONS", "10"))
        }
        
        # Workers
        config["workers"] = {
            "max_workers": int(os.getenv(f"{self.config_prefix}_MAX_WORKERS", "30")),
            "min_workers": int(os.getenv(f"{self.config_prefix}_MIN_WORKERS", "2")),
            "write_workers": int(os.getenv(f"{self.config_prefix}_WRITE_WORKERS", "20")),
            "generation_workers": int(os.getenv(f"{self.config_prefix}_GENERATION_WORKERS", "3")),
            "queue_multiplier": int(os.getenv(f"{self.config_prefix}_QUEUE_MULTIPLIER", "5")),
            "write_queue_multiplier": int(os.getenv(f"{self.config_prefix}_WRITE_QUEUE_MULTIPLIER", "5"))
        }
        
        # Performance
        config["performance"] = {
            "batch_aggregation_size": int(os.getenv(f"{self.config_prefix}_BATCH_AGGREGATION_SIZE", "5")),
            "batch_aggregation_timeout_ms": int(os.getenv(f"{self.config_prefix}_BATCH_AGGREGATION_TIMEOUT_MS", "200")),
            "progress_update_interval": int(os.getenv(f"{self.config_prefix}_PROGRESS_UPDATE_INTERVAL", "10")),
            "progress_smoothing": float(os.getenv(f"{self.config_prefix}_PROGRESS_SMOOTHING", "0.1")),
            "progress_miniters": int(os.getenv(f"{self.config_prefix}_PROGRESS_MINITERS", "1000")),
            "max_retries": int(os.getenv(f"{self.config_prefix}_MAX_RETRIES", "3")),
            "backoff_base_seconds": float(os.getenv(f"{self.config_prefix}_BACKOFF_BASE_SECONDS", "0.1"))
        }
        
        # Data generation
        config["data_generation"] = {
            "total_documents": int(os.getenv(f"{self.config_prefix}_TOTAL_DOCUMENTS", "1000000")),
            "document_size_kb": float(os.getenv(f"{self.config_prefix}_DOCUMENT_SIZE_KB", "2.5")),
            "document_size_multiplier": int(os.getenv(f"{self.config_prefix}_DOCUMENT_SIZE_MULTIPLIER", "1024")),
            "ru_per_document": int(os.getenv(f"{self.config_prefix}_RU_PER_DOCUMENT", "6")),
            "ru_check_interval_seconds": int(os.getenv(f"{self.config_prefix}_RU_CHECK_INTERVAL_SECONDS", "10")),
            "monitoring_batch_interval": int(os.getenv(f"{self.config_prefix}_MONITORING_BATCH_INTERVAL", "10")),
            "ru_throttle_threshold": int(os.getenv(f"{self.config_prefix}_RU_THROTTLE_THRESHOLD", "40000")),
            "ru_throttle_percentage": int(os.getenv(f"{self.config_prefix}_RU_THROTTLE_PERCENTAGE", "90")),
            "disable_throttling": os.getenv(f"{self.config_prefix}_DISABLE_THROTTLING", "true").lower() == "true"
        }
        
        # Migration
        config["migration"] = {
            "resume_from_checkpoint": os.getenv(f"{self.config_prefix}_RESUME_FROM_CHECKPOINT", "true").lower() == "true",
            "parallel_cursors": int(os.getenv(f"{self.config_prefix}_PARALLEL_CURSORS", "4")),
            "max_insert_workers": int(os.getenv(f"{self.config_prefix}_MAX_INSERT_WORKERS", "20")),
            "read_ahead_batches": int(os.getenv(f"{self.config_prefix}_READ_AHEAD_BATCHES", "100")),
            "read_ahead_workers": int(os.getenv(f"{self.config_prefix}_READ_AHEAD_WORKERS", "1")),
            "max_concurrent_batches": int(os.getenv(f"{self.config_prefix}_MAX_CONCURRENT_BATCHES", "20")),
            "fast_start": os.getenv(f"{self.config_prefix}_FAST_START", "false").lower() == "true"
        }
        
        return config
    
    def _create_config_object(self, config_data: Dict[str, Any]) -> FrameworkConfig:
        """Create FrameworkConfig object from dictionary"""
        return FrameworkConfig(
            environment=config_data.get("environment", Environment.DEVELOPMENT),
            log_level=config_data.get("log_level", "INFO"),
            source_database=DatabaseSettings(**config_data.get("source_database", {})),
            target_database=DatabaseSettings(**config_data.get("target_database", {})),
            workers=WorkerSettings(**config_data.get("workers", {})),
            performance=PerformanceSettings(**config_data.get("performance", {})),
            monitoring=MonitoringSettings(**config_data.get("monitoring", {})),
            data_generation=DataGenerationConfig(**config_data.get("data_generation", {})),
            migration=MigrationConfig(**config_data.get("migration", {})),
            custom_settings=config_data.get("custom_settings", {})
        )
    
    def _validate_config(self, config: FrameworkConfig):
        """Validate configuration"""
        errors = []
        
        # Validate required fields
        if not config.source_database.connection_string:
            errors.append("Source database connection string is required")
        
        if not config.target_database.connection_string:
            errors.append("Target database connection string is required")
        
        # Validate numeric ranges
        if config.workers.max_workers < config.workers.min_workers:
            errors.append("Max workers must be >= min workers")
        
        if config.data_generation.total_documents <= 0:
            errors.append("Total documents must be > 0")
        
        if errors:
            raise ValueError(f"Configuration validation failed: {'; '.join(errors)}")
    
    def get_config(self) -> FrameworkConfig:
        """Get current configuration"""
        if self.config is None:
            raise RuntimeError("Configuration not loaded. Call load_config() first.")
        return self.config
    
    def save_config(self, config: FrameworkConfig, file_path: str):
        """Save configuration to file"""
        config_dict = {
            "environment": config.environment.value,
            "log_level": config.log_level,
            "source_database": config.source_database.__dict__,
            "target_database": config.target_database.__dict__,
            "workers": config.workers.__dict__,
            "performance": config.performance.__dict__,
            "monitoring": config.monitoring.__dict__,
            "data_generation": config.data_generation.__dict__,
            "migration": config.migration.__dict__,
            "custom_settings": config.custom_settings
        }
        
        file_path_obj = Path(file_path)
        with open(file_path_obj, 'w') as f:
            if file_path_obj.suffix.lower() == '.json':
                json.dump(config_dict, f, indent=2)
            elif file_path_obj.suffix.lower() in ['.yml', '.yaml']:
                yaml.dump(config_dict, f, default_flow_style=False)
            else:
                raise ValueError(f"Unsupported file format: {file_path_obj.suffix}")

# Global configuration manager instance
config_manager = ConfigManager()
