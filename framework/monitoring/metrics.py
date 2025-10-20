"""
Monitoring and Metrics Framework
Enterprise-ready monitoring with real-time metrics, performance tracking, and alerting
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Callable
from enum import Enum
from collections import deque
import json

logger = logging.getLogger(__name__)

class OperationType(Enum):
    """Types of operations to monitor"""
    READ = "read"
    WRITE = "write"
    GENERATE = "generate"
    MIGRATE = "migrate"
    CUSTOM = "custom"

class MetricType(Enum):
    """Types of metrics"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    RATE = "rate"

@dataclass
class Metric:
    """A single metric measurement"""
    name: str
    value: float
    timestamp: float
    metric_type: MetricType
    tags: Dict[str, str] = field(default_factory=dict)
    unit: Optional[str] = None

@dataclass
class OperationMetrics:
    """Metrics for a single operation"""
    operation_id: str
    operation_name: str
    operation_type: OperationType
    start_time: float
    end_time: Optional[float] = None
    documents_processed: int = 0
    success: bool = False
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration(self) -> float:
        if self.end_time is None:
            return time.time() - self.start_time
        return self.end_time - self.start_time
    
    @property
    def rate(self) -> float:
        return self.documents_processed / self.duration if self.duration > 0 else 0

@dataclass
class PerformanceSnapshot:
    """A snapshot of performance metrics at a point in time"""
    timestamp: float
    operations_per_second: float
    documents_per_second: float
    error_rate: float
    active_operations: int
    queue_size: int
    memory_usage_mb: float
    cpu_usage_percent: float

class MetricsCollector:
    """
    Enterprise metrics collector with:
    - Real-time metrics collection
    - Performance monitoring
    - Alerting capabilities
    - Export functionality
    """
    
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.metrics: deque = deque(maxlen=window_size)
        self.operations: Dict[str, OperationMetrics] = {}
        self.performance_history: deque = deque(maxlen=window_size)
        self.alerts: List[Dict[str, Any]] = []
        self.alert_callbacks: List[Callable] = []
        
        # Performance tracking
        self.total_operations = 0
        self.total_documents = 0
        self.total_errors = 0
        self.start_time = time.time()
    
    def start_operation(self, operation_id: str, operation_name: str, operation_type: OperationType, 
                       documents_count: int = 0) -> OperationMetrics:
        """Start tracking an operation"""
        operation = OperationMetrics(
            operation_id=operation_id,
            operation_name=operation_name,
            operation_type=operation_type,
            start_time=time.time(),
            documents_processed=documents_count
        )
        
        self.operations[operation_id] = operation
        self.total_operations += 1
        
        logger.debug(f"Started operation: {operation_name} ({operation_id})")
        return operation
    
    def end_operation(self, operation: OperationMetrics, documents_processed: int, 
                    success: bool, error_message: Optional[str] = None):
        """End tracking an operation"""
        operation.end_time = time.time()
        operation.documents_processed = documents_processed
        operation.success = success
        operation.error_message = error_message
        
        if not success:
            self.total_errors += 1
        
        self.total_documents += documents_processed
        
        # Remove from active operations
        if operation.operation_id in self.operations:
            del self.operations[operation.operation_id]
        
        logger.debug(f"Ended operation: {operation.operation_name} - {documents_processed} docs in {operation.duration:.2f}s")
    
    def record_metric(self, name: str, value: float, metric_type: MetricType, 
                     tags: Optional[Dict[str, str]] = None, unit: Optional[str] = None):
        """Record a custom metric"""
        metric = Metric(
            name=name,
            value=value,
            timestamp=time.time(),
            metric_type=metric_type,
            tags=tags or {},
            unit=unit
        )
        
        self.metrics.append(metric)
        logger.debug(f"Recorded metric: {name}={value} {unit or ''}")
    
    def get_current_performance(self) -> PerformanceSnapshot:
        """Get current performance snapshot"""
        current_time = time.time()
        elapsed_time = current_time - self.start_time
        
        # Calculate rates
        ops_per_second = self.total_operations / elapsed_time if elapsed_time > 0 else 0
        docs_per_second = self.total_documents / elapsed_time if elapsed_time > 0 else 0
        error_rate = self.total_errors / self.total_operations if self.total_operations > 0 else 0
        
        # Get system metrics
        memory_usage = self._get_memory_usage()
        cpu_usage = self._get_cpu_usage()
        
        snapshot = PerformanceSnapshot(
            timestamp=current_time,
            operations_per_second=ops_per_second,
            documents_per_second=docs_per_second,
            error_rate=error_rate,
            active_operations=len(self.operations),
            queue_size=0,  # Will be updated by specific implementations
            memory_usage_mb=memory_usage,
            cpu_usage_percent=cpu_usage
        )
        
        self.performance_history.append(snapshot)
        return snapshot
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except ImportError:
            return 0.0
    
    def _get_cpu_usage(self) -> float:
        """Get current CPU usage percentage"""
        try:
            import psutil
            return psutil.cpu_percent()
        except ImportError:
            return 0.0
    
    def get_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary"""
        current_performance = self.get_current_performance()
        
        # Calculate averages from recent history
        recent_snapshots = list(self.performance_history)[-10:] if self.performance_history else []
        
        avg_ops_per_second = sum(s.operations_per_second for s in recent_snapshots) / len(recent_snapshots) if recent_snapshots else 0
        avg_docs_per_second = sum(s.documents_per_second for s in recent_snapshots) / len(recent_snapshots) if recent_snapshots else 0
        avg_error_rate = sum(s.error_rate for s in recent_snapshots) / len(recent_snapshots) if recent_snapshots else 0
        
        return {
            "total_operations": self.total_operations,
            "total_documents": self.total_documents,
            "total_errors": self.total_errors,
            "current_performance": {
                "operations_per_second": current_performance.operations_per_second,
                "documents_per_second": current_performance.documents_per_second,
                "error_rate": current_performance.error_rate,
                "active_operations": current_performance.active_operations,
                "memory_usage_mb": current_performance.memory_usage_mb,
                "cpu_usage_percent": current_performance.cpu_usage_percent
            },
            "average_performance": {
                "operations_per_second": avg_ops_per_second,
                "documents_per_second": avg_docs_per_second,
                "error_rate": avg_error_rate
            },
            "uptime_seconds": time.time() - self.start_time,
            "metrics_count": len(self.metrics),
            "alerts_count": len(self.alerts)
        }
    
    def add_alert_callback(self, callback: Callable):
        """Add an alert callback function"""
        self.alert_callbacks.append(callback)
    
    def trigger_alert(self, alert_type: str, message: str, severity: str = "warning", 
                     metadata: Optional[Dict[str, Any]] = None):
        """Trigger an alert"""
        alert = {
            "timestamp": time.time(),
            "type": alert_type,
            "message": message,
            "severity": severity,
            "metadata": metadata or {}
        }
        
        self.alerts.append(alert)
        
        # Call alert callbacks
        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                logger.error(f"Error in alert callback: {e}")
        
        logger.warning(f"ALERT [{severity.upper()}]: {message}")
    
    def export_metrics(self, format: str = "json") -> str:
        """Export metrics in specified format"""
        summary = self.get_summary()
        
        if format.lower() == "json":
            return json.dumps(summary, indent=2)
        elif format.lower() == "csv":
            # Convert to CSV format
            lines = ["metric,value"]
            for key, value in summary.items():
                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        lines.append(f"{key}.{sub_key},{sub_value}")
                else:
                    lines.append(f"{key},{value}")
            return "\n".join(lines)
        else:
            raise ValueError(f"Unsupported export format: {format}")
    
    def clear_metrics(self):
        """Clear all metrics and reset counters"""
        self.metrics.clear()
        self.operations.clear()
        self.performance_history.clear()
        self.alerts.clear()
        self.total_operations = 0
        self.total_documents = 0
        self.total_errors = 0
        self.start_time = time.time()
        logger.info("Metrics cleared and reset")

class PerformanceMonitor:
    """
    Real-time performance monitor with alerting
    """
    
    def __init__(self, metrics_collector: MetricsCollector, check_interval: float = 5.0):
        self.metrics_collector = metrics_collector
        self.check_interval = check_interval
        self.is_monitoring = False
        self.monitor_task = None
        
        # Alert thresholds
        self.error_rate_threshold = 0.1  # 10%
        self.memory_threshold_mb = 1000  # 1GB
        self.cpu_threshold_percent = 80  # 80%
        self.performance_drop_threshold = 0.5  # 50% drop
    
    async def start_monitoring(self):
        """Start performance monitoring"""
        if self.is_monitoring:
            return
        
        self.is_monitoring = True
        self.monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("Performance monitoring started")
    
    async def stop_monitoring(self):
        """Stop performance monitoring"""
        self.is_monitoring = False
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("Performance monitoring stopped")
    
    async def _monitor_loop(self):
        """Main monitoring loop"""
        while self.is_monitoring:
            try:
                await self._check_performance()
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in performance monitoring: {e}")
                await asyncio.sleep(self.check_interval)
    
    async def _check_performance(self):
        """Check performance and trigger alerts if needed"""
        snapshot = self.metrics_collector.get_current_performance()
        
        # Check error rate
        if snapshot.error_rate > self.error_rate_threshold:
            self.metrics_collector.trigger_alert(
                "high_error_rate",
                f"Error rate is {snapshot.error_rate:.1%} (threshold: {self.error_rate_threshold:.1%})",
                "error"
            )
        
        # Check memory usage
        if snapshot.memory_usage_mb > self.memory_threshold_mb:
            self.metrics_collector.trigger_alert(
                "high_memory_usage",
                f"Memory usage is {snapshot.memory_usage_mb:.1f}MB (threshold: {self.memory_threshold_mb}MB)",
                "warning"
            )
        
        # Check CPU usage
        if snapshot.cpu_usage_percent > self.cpu_threshold_percent:
            self.metrics_collector.trigger_alert(
                "high_cpu_usage",
                f"CPU usage is {snapshot.cpu_usage_percent:.1f}% (threshold: {self.cpu_threshold_percent}%)",
                "warning"
            )
        
        # Check performance drop
        if len(self.metrics_collector.performance_history) >= 2:
            recent_snapshots = list(self.metrics_collector.performance_history)[-2:]
            if len(recent_snapshots) == 2:
                current_rate = recent_snapshots[1].documents_per_second
                previous_rate = recent_snapshots[0].documents_per_second
                
                if previous_rate > 0 and current_rate < previous_rate * (1 - self.performance_drop_threshold):
                    self.metrics_collector.trigger_alert(
                        "performance_drop",
                        f"Performance dropped from {previous_rate:.0f} to {current_rate:.0f} docs/s",
                        "warning"
                    )
