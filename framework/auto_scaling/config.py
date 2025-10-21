"""
Auto-Scaling Configuration
Configuration management for intelligent auto-scaling system
"""
import os
from dataclasses import dataclass
from typing import Dict, Any, Optional
from enum import Enum

class AutoScalingMode(Enum):
    """Auto-scaling modes"""
    DISABLED = "disabled"
    CONSERVATIVE = "conservative"  # Slow, safe scaling
    BALANCED = "balanced"         # Moderate scaling
    AGGRESSIVE = "aggressive"     # Fast, aggressive scaling

@dataclass
class AutoScalingProfile:
    """Auto-scaling profile configuration"""
    name: str
    mode: AutoScalingMode
    
    # Performance targets
    target_docs_per_second: float
    target_cpu_usage: float
    target_memory_usage: float
    target_error_rate: float
    
    # Scaling thresholds
    cpu_scale_up_threshold: float
    cpu_scale_down_threshold: float
    memory_scale_up_threshold: float
    memory_scale_down_threshold: float
    
    # Scaling limits
    min_workers: int
    max_workers: int
    min_batch_size: int
    max_batch_size: int
    
    # Scaling intervals
    scaling_check_interval: float
    scaling_cooldown: float
    
    # Performance windows
    performance_window_size: int
    stability_window_size: int

class AutoScalingConfigManager:
    """Manages auto-scaling configuration"""
    
    def __init__(self):
        self.profiles = self._create_default_profiles()
        self.current_profile = "balanced"
    
    def _create_default_profiles(self) -> Dict[str, AutoScalingProfile]:
        """Create default auto-scaling profiles"""
        return {
            "conservative": AutoScalingProfile(
                name="conservative",
                mode=AutoScalingMode.CONSERVATIVE,
                target_docs_per_second=5000.0,
                target_cpu_usage=60.0,
                target_memory_usage=70.0,
                target_error_rate=0.005,  # 0.5%
                cpu_scale_up_threshold=75.0,
                cpu_scale_down_threshold=45.0,
                memory_scale_up_threshold=80.0,
                memory_scale_down_threshold=55.0,
                min_workers=2,
                max_workers=20,
                min_batch_size=1000,
                max_batch_size=50000,
                scaling_check_interval=15.0,
                scaling_cooldown=60.0,
                performance_window_size=15,
                stability_window_size=3
            ),
            
            "balanced": AutoScalingProfile(
                name="balanced",
                mode=AutoScalingMode.BALANCED,
                target_docs_per_second=10000.0,
                target_cpu_usage=70.0,
                target_memory_usage=75.0,
                target_error_rate=0.01,  # 1%
                cpu_scale_up_threshold=80.0,
                cpu_scale_down_threshold=50.0,
                memory_scale_up_threshold=85.0,
                memory_scale_down_threshold=60.0,
                min_workers=4,
                max_workers=30,
                min_batch_size=2000,
                max_batch_size=75000,
                scaling_check_interval=10.0,
                scaling_cooldown=30.0,
                performance_window_size=20,
                stability_window_size=5
            ),
            
            "aggressive": AutoScalingProfile(
                name="aggressive",
                mode=AutoScalingMode.AGGRESSIVE,
                target_docs_per_second=20000.0,
                target_cpu_usage=80.0,
                target_memory_usage=85.0,
                target_error_rate=0.02,  # 2%
                cpu_scale_up_threshold=85.0,
                cpu_scale_down_threshold=55.0,
                memory_scale_up_threshold=90.0,
                memory_scale_down_threshold=65.0,
                min_workers=8,
                max_workers=50,
                min_batch_size=5000,
                max_batch_size=100000,
                scaling_check_interval=5.0,
                scaling_cooldown=15.0,
                performance_window_size=10,
                stability_window_size=3
            )
        }
    
    def get_profile(self, name: str) -> Optional[AutoScalingProfile]:
        """Get auto-scaling profile by name"""
        return self.profiles.get(name)
    
    def get_current_profile(self) -> AutoScalingProfile:
        """Get current auto-scaling profile"""
        return self.profiles[self.current_profile]
    
    def set_profile(self, name: str):
        """Set current auto-scaling profile"""
        if name in self.profiles:
            self.current_profile = name
        else:
            raise ValueError(f"Unknown auto-scaling profile: {name}")
    
    def load_from_environment(self) -> AutoScalingProfile:
        """Load auto-scaling configuration from environment variables"""
        # Get profile name from environment
        profile_name = os.getenv("AUTO_SCALING_PROFILE", "balanced")
        
        # Get base profile
        profile = self.get_profile(profile_name)
        if not profile:
            profile = self.get_profile("balanced")
        
        # Override with environment variables
        profile.target_docs_per_second = float(os.getenv("AUTO_SCALING_TARGET_DOCS_PER_SECOND", profile.target_docs_per_second))
        profile.target_cpu_usage = float(os.getenv("AUTO_SCALING_TARGET_CPU_USAGE", profile.target_cpu_usage))
        profile.target_memory_usage = float(os.getenv("AUTO_SCALING_TARGET_MEMORY_USAGE", profile.target_memory_usage))
        profile.target_error_rate = float(os.getenv("AUTO_SCALING_TARGET_ERROR_RATE", profile.target_error_rate))
        
        profile.cpu_scale_up_threshold = float(os.getenv("AUTO_SCALING_CPU_SCALE_UP_THRESHOLD", profile.cpu_scale_up_threshold))
        profile.cpu_scale_down_threshold = float(os.getenv("AUTO_SCALING_CPU_SCALE_DOWN_THRESHOLD", profile.cpu_scale_down_threshold))
        profile.memory_scale_up_threshold = float(os.getenv("AUTO_SCALING_MEMORY_SCALE_UP_THRESHOLD", profile.memory_scale_up_threshold))
        profile.memory_scale_down_threshold = float(os.getenv("AUTO_SCALING_MEMORY_SCALE_DOWN_THRESHOLD", profile.memory_scale_down_threshold))
        
        # Use GEN_ prefixed environment variables for worker limits
        profile.min_workers = int(os.getenv("GEN_MIN_WORKERS", os.getenv("AUTO_SCALING_MIN_WORKERS", profile.min_workers)))
        profile.max_workers = int(os.getenv("GEN_MAX_WORKERS", os.getenv("AUTO_SCALING_MAX_WORKERS", profile.max_workers)))
        profile.min_batch_size = int(os.getenv("GEN_BATCH_SIZE", os.getenv("AUTO_SCALING_MIN_BATCH_SIZE", profile.min_batch_size)))
        profile.max_batch_size = int(os.getenv("GEN_BATCH_SIZE", os.getenv("AUTO_SCALING_MAX_BATCH_SIZE", profile.max_batch_size))) * 2  # Allow up to 2x batch size
        
        profile.scaling_check_interval = float(os.getenv("AUTO_SCALING_CHECK_INTERVAL", profile.scaling_check_interval))
        profile.scaling_cooldown = float(os.getenv("AUTO_SCALING_COOLDOWN", profile.scaling_cooldown))
        
        profile.performance_window_size = int(os.getenv("AUTO_SCALING_PERFORMANCE_WINDOW_SIZE", profile.performance_window_size))
        profile.stability_window_size = int(os.getenv("AUTO_SCALING_STABILITY_WINDOW_SIZE", profile.stability_window_size))
        
        return profile
    
    def list_profiles(self) -> Dict[str, Dict[str, Any]]:
        """List all available profiles"""
        return {
            name: {
                "mode": profile.mode.value,
                "target_docs_per_second": profile.target_docs_per_second,
                "target_cpu_usage": profile.target_cpu_usage,
                "min_workers": profile.min_workers,
                "max_workers": profile.max_workers,
                "scaling_check_interval": profile.scaling_check_interval
            }
            for name, profile in self.profiles.items()
        }
