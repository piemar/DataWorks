"""
Auto-Scaling Metrics Collector
Collects real-time performance metrics for intelligent auto-scaling decisions
"""
import asyncio
import logging
import time
import psutil
from typing import Dict, Any, Optional, Callable
from dataclasses import dataclass
from collections import deque

from .engine import PerformanceMetrics, OperationType

logger = logging.getLogger(__name__)

@dataclass
class SystemMetrics:
    """System-level performance metrics"""
    timestamp: float
    cpu_usage_percent: float
    memory_usage_mb: float
    network_io_mbps: float
    disk_io_mbps: float

class AutoScalingMetricsCollector:
    """
    Collects real-time performance metrics for auto-scaling decisions
    """
    
    def __init__(self, operation_type: OperationType):
        self.operation_type = operation_type
        self.metrics_callbacks: Dict[str, Callable] = {}
        self.system_metrics_history: deque = deque(maxlen=10)
        
        # Performance tracking
        self.last_docs_count = 0
        self.last_ops_count = 0
        self.last_timestamp = time.time()
        
        # Database metrics
        self.last_ru_consumption = 0
        self.last_connection_count = 0
        
    def register_metrics_callback(self, name: str, callback: Callable):
        """Register a callback for collecting specific metrics"""
        self.metrics_callbacks[name] = callback
        logger.debug(f"Registered metrics callback: {name}")
    
    async def collect_metrics(self) -> Optional[PerformanceMetrics]:
        """Collect current performance metrics"""
        try:
            current_time = time.time()
            
            # Collect system metrics
            system_metrics = await self._collect_system_metrics()
            
            # Collect operation-specific metrics
            operation_metrics = await self._collect_operation_metrics()
            
            # Combine metrics
            metrics = PerformanceMetrics(
                timestamp=current_time,
                operation_type=self.operation_type,
                documents_per_second=operation_metrics.get('documents_per_second', 0.0),
                operations_per_second=operation_metrics.get('operations_per_second', 0.0),
                cpu_usage_percent=system_metrics.cpu_usage_percent,
                memory_usage_mb=system_metrics.memory_usage_mb,
                network_io_mbps=system_metrics.network_io_mbps,
                ru_consumption=operation_metrics.get('ru_consumption', 0.0),
                connection_pool_utilization=operation_metrics.get('connection_pool_utilization', 0.0),
                queue_depth=operation_metrics.get('queue_depth', 0),
                error_rate=operation_metrics.get('error_rate', 0.0),
                retry_rate=operation_metrics.get('retry_rate', 0.0),
                active_workers=operation_metrics.get('active_workers', 0),
                idle_workers=operation_metrics.get('idle_workers', 0),
                batch_size=operation_metrics.get('batch_size', 0),
                batch_processing_time_ms=operation_metrics.get('batch_processing_time_ms', 0.0)
            )
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error collecting metrics: {e}")
            return None
    
    async def _collect_system_metrics(self) -> SystemMetrics:
        """Collect system-level performance metrics"""
        try:
            # CPU usage
            cpu_usage = psutil.cpu_percent(interval=0.1)
            
            # Memory usage
            memory = psutil.virtual_memory()
            memory_usage_mb = memory.used / 1024 / 1024
            
            # Network I/O
            network_io = psutil.net_io_counters()
            network_io_mbps = (network_io.bytes_sent + network_io.bytes_recv) / 1024 / 1024 / 1024
            
            # Disk I/O
            disk_io = psutil.disk_io_counters()
            disk_io_mbps = (disk_io.read_bytes + disk_io.write_bytes) / 1024 / 1024 / 1024
            
            return SystemMetrics(
                timestamp=time.time(),
                cpu_usage_percent=cpu_usage,
                memory_usage_mb=memory_usage_mb,
                network_io_mbps=network_io_mbps,
                disk_io_mbps=disk_io_mbps
            )
            
        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")
            return SystemMetrics(
                timestamp=time.time(),
                cpu_usage_percent=0.0,
                memory_usage_mb=0.0,
                network_io_mbps=0.0,
                disk_io_mbps=0.0
            )
    
    async def _collect_operation_metrics(self) -> Dict[str, Any]:
        """Collect operation-specific metrics"""
        metrics = {}
        
        # Collect metrics from registered callbacks
        for name, callback in self.metrics_callbacks.items():
            try:
                callback_metrics = await callback()
                if callback_metrics:
                    metrics.update(callback_metrics)
            except Exception as e:
                logger.error(f"Error collecting metrics from {name}: {e}")
        
        return metrics
    
    def update_document_count(self, count: int):
        """Update document count for rate calculation"""
        current_time = time.time()
        time_diff = current_time - self.last_timestamp
        
        if time_diff > 0:
            docs_per_second = (count - self.last_docs_count) / time_diff
            self.last_docs_count = count
            self.last_timestamp = current_time
            return docs_per_second
        
        return 0.0
    
    def update_operation_count(self, count: int):
        """Update operation count for rate calculation"""
        current_time = time.time()
        time_diff = current_time - self.last_timestamp
        
        if time_diff > 0:
            ops_per_second = (count - self.last_ops_count) / time_diff
            self.last_ops_count = count
            return ops_per_second
        
        return 0.0
