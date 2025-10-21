"""
Profile-based Configuration System

This module defines pre-configured profiles for different use cases:
- data-ingest: High-speed data generation
- data-migration: High-speed m"igration with stability  
- dev: Development and testing with minimal resources
"""

from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

# Profile definitions with optimized settings for each use case
PROFILES: Dict[str, Dict[str, Any]] = {
    "data-ingest": {
        "description": "High-speed data generation - optimized for maximum throughput",
        "use_case": "Data generation with focus on speed and no data loss",
        "settings": {
            # Data Generation Settings (GEN_ prefix)
            "GEN_BATCH_SIZE": 40000,
            "GEN_WORKERS": 16,
            "GEN_WRITE_WORKERS": 24,
            "GEN_MAX_WORKERS": 20,
            "GEN_MIN_WORKERS": 8,
            "GEN_MAX_WRITE_WORKERS": 30,
            "GEN_MIN_WRITE_WORKERS": 12,
            
            # Connection Pool Settings
            "GEN_POOL_SIZE": 600,
            "GEN_MAX_POOL_SIZE": 800,
            "GEN_MIN_POOL_SIZE": 200,
            "GEN_MAX_IDLE_TIME_MS": 300000,
            "GEN_WARMUP_CONNECTIONS": 50,
            
            # Timeout Settings
            "GEN_SOCKET_TIMEOUT_MS": 600000,
            "GEN_CONNECT_TIMEOUT_MS": 300000,
            "GEN_SERVER_SELECTION_TIMEOUT_MS": 300000,
            
            # Retry Logic Settings
            "GEN_MAX_RETRIES": 5,
            "GEN_BACKOFF_BASE_SECONDS": 0.1,
            
            # Performance Monitoring Settings
            "GEN_PERFORMANCE_WINDOW_SIZE": 20,
            "GEN_PERFORMANCE_CHECK_INTERVAL_SECONDS": 30,
            "GEN_THROTTLING_THRESHOLD_PERCENTAGE": 10,
            "GEN_SCALING_COOLDOWN_SECONDS": 60,
            "GEN_SCALE_UP_COOLDOWN_SECONDS": 30,
            
            # Progress Bar Settings
            "GEN_PROGRESS_SMOOTHING": 0.1,
            "GEN_PROGRESS_MINITERS": 1000,
            
            # Document Size Estimation
            "GEN_DOCUMENT_SIZE_KB": 2.5,
            "GEN_DOCUMENT_SIZE_MULTIPLIER": 1024,
        }
    },
    
    "data-migration": {
        "description": "High-speed migration with stability - optimized for reliable migration",
        "use_case": "Data migration with focus on speed and stability",
        "settings": {
            # Basic Migration Settings (MIG_ prefix)
            "MIG_BATCH_SIZE": 15000,
            "MIG_WORKERS": 4,
            "MIG_MAX_WORKERS": 8,
            "MIG_MIN_WORKERS": 2,
            "MIG_PARALLEL_CURSORS": 4,
            "MIG_RESUME_FROM_CHECKPOINT": False,
            
            # RU-Optimized Migration Settings
            "MIG_MAX_INSERT_WORKERS": 8,
            "MIG_READ_AHEAD_BATCHES": 5000,
            "MIG_READ_AHEAD_WORKERS": 1,
            "MIG_MAX_CONCURRENT_BATCHES": 10,
            
            # RU Management Settings
            "MIG_RU_THROTTLE_THRESHOLD": 35000,
            "MIG_RU_THROTTLE_PERCENTAGE": 85,
            "MIG_RU_CHECK_INTERVAL_SECONDS": 5,
            "MIG_RETRY_DELAY_MS": 1000,
            "MIG_MAX_RETRIES": 5,
            
            # Connection and Timeout Settings
            "MIG_SOCKET_TIMEOUT_MS": 300000,
            "MIG_CONNECT_TIMEOUT_MS": 120000,
            "MIG_SERVER_SELECTION_TIMEOUT_MS": 60000,
            "MIG_WARMUP_CONNECTIONS": 10,
            "MIG_PROGRESS_UPDATE_INTERVAL_MS": 25,
            
            # Conservative Connection Pool Settings (to prevent broken pipe errors)
            "MIG_TARGET_DB_MAX_POOL_SIZE": 100,
            "MIG_TARGET_DB_MIN_POOL_SIZE": 10,
            "MIG_TARGET_DB_MAX_IDLE_TIME_MS": 300000,
            "MIG_TARGET_DB_SOCKET_TIMEOUT_MS": 300000,
            "MIG_TARGET_DB_CONNECT_TIMEOUT_MS": 120000,
            
            # Fast Start Settings
            "MIG_FAST_START": False,
        }
    },
    
    "dev": {
        "description": "Development and testing - optimized for minimal resource usage",
        "use_case": "Testing framework functionality without high resource requirements",
        "settings": {
            # Data Generation Settings (smaller batches for testing)
            "GEN_BATCH_SIZE": 500000,
            "GEN_WORKERS": 2,
            "GEN_WRITE_WORKERS": 4,
            "GEN_MAX_WORKERS": 4,
            "GEN_MIN_WORKERS": 1,
            "GEN_MAX_WRITE_WORKERS": 6,
            "GEN_MIN_WRITE_WORKERS": 2,
            
            # Connection Pool Settings (minimal)
            "GEN_POOL_SIZE": 10,
            "GEN_MAX_POOL_SIZE": 12,
            "GEN_MIN_POOL_SIZE": 4,
            "GEN_MAX_IDLE_TIME_MS": 500000,
            "GEN_WARMUP_CONNECTIONS": 5,
            
            # Timeout Settings (faster for testing)
            "GEN_SOCKET_TIMEOUT_MS": 300000,
            "GEN_CONNECT_TIMEOUT_MS": 120000,
            "GEN_SERVER_SELECTION_TIMEOUT_MS": 30000,
            
            # Retry Logic Settings
            "GEN_MAX_RETRIES": 3,
            "GEN_BACKOFF_BASE_SECONDS": 0.1,
            
            # Performance Monitoring Settings
            "GEN_PERFORMANCE_WINDOW_SIZE": 10,
            "GEN_PERFORMANCE_CHECK_INTERVAL_SECONDS": 60,
            "GEN_THROTTLING_THRESHOLD_PERCENTAGE": 5,
            "GEN_SCALING_COOLDOWN_SECONDS": 120,
            "GEN_SCALE_UP_COOLDOWN_SECONDS": 60,
            
            # Progress Bar Settings
            "GEN_PROGRESS_SMOOTHING": 0.2,
            "GEN_PROGRESS_MINITERS": 100,
            
            # Document Size Estimation
            "GEN_DOCUMENT_SIZE_KB": 2.5,
            "GEN_DOCUMENT_SIZE_MULTIPLIER": 1024,
            
            # Migration Settings (smaller batches for testing)
            "MIG_BATCH_SIZE": 100,
            "MIG_WORKERS": 2,
            "MIG_MAX_WORKERS": 4,
            "MIG_MIN_WORKERS": 1,
            "MIG_PARALLEL_CURSORS": 2,
            "MIG_RESUME_FROM_CHECKPOINT": False,
            
            # RU-Optimized Migration Settings (conservative for dev)
            "MIG_MAX_INSERT_WORKERS": 4,
            "MIG_READ_AHEAD_BATCHES": 100,
            "MIG_READ_AHEAD_WORKERS": 1,
            "MIG_MAX_CONCURRENT_BATCHES": 5,
            
            # RU Management Settings (low thresholds for dev)
            "MIG_RU_THROTTLE_THRESHOLD": 1000,
            "MIG_RU_THROTTLE_PERCENTAGE": 80,
            "MIG_RU_CHECK_INTERVAL_SECONDS": 10,
            "MIG_RETRY_DELAY_MS": 500,
            "MIG_MAX_RETRIES": 3,
            
            # Connection and Timeout Settings (faster for dev)
            "MIG_SOCKET_TIMEOUT_MS": 120000,
            "MIG_CONNECT_TIMEOUT_MS": 60000,
            "MIG_SERVER_SELECTION_TIMEOUT_MS": 30000,
            "MIG_WARMUP_CONNECTIONS": 5,
            "MIG_PROGRESS_UPDATE_INTERVAL_MS": 100,
            
            # Conservative Connection Pool Settings (minimal for dev)
            "MIG_TARGET_DB_MAX_POOL_SIZE": 20,
            "MIG_TARGET_DB_MIN_POOL_SIZE": 5,
            "MIG_TARGET_DB_MAX_IDLE_TIME_MS": 300000,
            "MIG_TARGET_DB_SOCKET_TIMEOUT_MS": 120000,
            "MIG_TARGET_DB_CONNECT_TIMEOUT_MS": 60000,
            
            # Fast Start Settings
            "MIG_FAST_START": False,
        }
    }
}

def get_profile(profile_name: str) -> Optional[Dict[str, Any]]:
    """
    Get profile configuration by name.
    
    Args:
        profile_name: Name of the profile to retrieve
        
    Returns:
        Profile configuration dict or None if not found
    """
    return PROFILES.get(profile_name)

def list_profiles() -> Dict[str, str]:
    """
    List all available profiles with their descriptions.
    
    Returns:
        Dict mapping profile names to descriptions
    """
    return {name: profile["description"] for name, profile in PROFILES.items()}

def validate_profile_settings(profile_name: str, user_settings: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate user settings against profile recommendations.
    
    Args:
        profile_name: Name of the profile being used
        user_settings: User-provided settings
        
    Returns:
        Dict of validation warnings/errors
    """
    warnings = {}
    profile = get_profile(profile_name)
    
    if not profile:
        return {"error": f"Unknown profile: {profile_name}"}
    
    profile_settings = profile["settings"]
    
    # Check for potentially problematic overrides
    if profile_name == "dev":
        # Warn about large batch sizes in dev mode
        if user_settings.get("GEN_BATCH_SIZE", 0) > 10000:
            warnings["GEN_BATCH_SIZE"] = "Large batch size in dev profile may cause issues"
        if user_settings.get("MIG_BATCH_SIZE", 0) > 5000:
            warnings["MIG_BATCH_SIZE"] = "Large batch size in dev profile may cause issues"
    
    elif profile_name == "data-ingest":
        # Warn about small batch sizes in high-performance mode
        if user_settings.get("GEN_BATCH_SIZE", 0) < 20000:
            warnings["GEN_BATCH_SIZE"] = "Small batch size may limit performance in data-ingest profile"
    
    elif profile_name == "data-migration":
        # Warn about very large batch sizes that might cause stability issues
        if user_settings.get("MIG_BATCH_SIZE", 0) > 25000:
            warnings["MIG_BATCH_SIZE"] = "Very large batch size may cause stability issues"
    
    return warnings

def merge_profile_settings(profile_name: str, user_settings: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge profile settings with user overrides.
    
    Args:
        profile_name: Name of the profile to use
        user_settings: User-provided settings (overrides)
        
    Returns:
        Merged settings dict
    """
    profile = get_profile(profile_name)
    if not profile:
        logger.warning(f"Unknown profile: {profile_name}, using user settings only")
        return user_settings
    
    # Start with profile defaults
    merged_settings = profile["settings"].copy()
    
    # Apply user overrides
    merged_settings.update(user_settings)
    
    # Log validation warnings
    warnings = validate_profile_settings(profile_name, user_settings)
    for key, warning in warnings.items():
        if key != "error":
            logger.warning(f"Profile validation warning: {warning}")
    
    return merged_settings

def show_profile_info(profile_name: str) -> str:
    """
    Show detailed information about a profile.
    
    Args:
        profile_name: Name of the profile
        
    Returns:
        Formatted string with profile information
    """
    profile = get_profile(profile_name)
    if not profile:
        return f"Profile '{profile_name}' not found"
    
    info = f"""
Profile: {profile_name}
Description: {profile['description']}
Use Case: {profile['use_case']}

Key Settings:
"""
    
    # Show key settings grouped by category
    settings = profile["settings"]
    
    # Data Generation Settings
    gen_settings = {k: v for k, v in settings.items() if k.startswith("GEN_")}
    if gen_settings:
        info += "\nData Generation:\n"
        for key, value in sorted(gen_settings.items()):
            info += f"  {key}: {value}\n"
    
    # Migration Settings
    mig_settings = {k: v for k, v in settings.items() if k.startswith("MIG_")}
    if mig_settings:
        info += "\nMigration:\n"
        for key, value in sorted(mig_settings.items()):
            info += f"  {key}: {value}\n"
    
    return info

if __name__ == "__main__":
    # CLI interface for profile management
    import sys
    
    if len(sys.argv) < 2:
        print("Available profiles:")
        for name, desc in list_profiles().items():
            print(f"  {name}: {desc}")
        sys.exit(0)
    
    command = sys.argv[1]
    
    if command == "list":
        print("Available profiles:")
        for name, desc in list_profiles().items():
            print(f"  {name}: {desc}")
    
    elif command == "info" and len(sys.argv) > 2:
        profile_name = sys.argv[2]
        print(show_profile_info(profile_name))
    
    elif command == "validate" and len(sys.argv) > 3:
        profile_name = sys.argv[2]
        # This would need to be implemented with actual user settings
        print(f"Validation for profile '{profile_name}' would go here")
    
    else:
        print("Usage:")
        print("  python profiles.py list")
        print("  python profiles.py info <profile_name>")
        print("  python profiles.py validate <profile_name> <settings_file>")
