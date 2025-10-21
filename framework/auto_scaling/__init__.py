"""
Intelligent Auto-Scaling Framework
Adaptive performance optimization for migration and generation operations
"""

from .engine import IntelligentAutoScaler, AutoScalingConfig, PerformanceMetrics, ScalingDecision, ScalingDirection, OperationType
from .metrics_collector import AutoScalingMetricsCollector, SystemMetrics
from .config import AutoScalingConfigManager, AutoScalingProfile, AutoScalingMode

__all__ = [
    'IntelligentAutoScaler',
    'AutoScalingConfig', 
    'PerformanceMetrics',
    'ScalingDecision',
    'ScalingDirection',
    'OperationType',
    'AutoScalingMetricsCollector',
    'SystemMetrics',
    'AutoScalingConfigManager',
    'AutoScalingProfile',
    'AutoScalingMode'
]
