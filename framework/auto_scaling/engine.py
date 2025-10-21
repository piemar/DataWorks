"""
Intelligent Auto-Scaling Engine
Adaptive performance optimization for migration and generation operations
"""
import asyncio
import logging
import time
import statistics
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
from collections import deque

logger = logging.getLogger(__name__)

class OperationType(Enum):
    """Types of operations that can be auto-scaled"""
    MIGRATION = "migration"
    GENERATION = "generation"

class ScalingDirection(Enum):
    """Scaling direction"""
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    MAINTAIN = "maintain"

@dataclass
class PerformanceMetrics:
    """Performance metrics for auto-scaling decisions"""
    timestamp: float
    operation_type: OperationType
    
    # Throughput metrics
    documents_per_second: float
    operations_per_second: float
    
    # Resource utilization
    cpu_usage_percent: float
    memory_usage_mb: float
    network_io_mbps: float
    
    # Database metrics
    ru_consumption: float = 0.0
    connection_pool_utilization: float = 0.0
    queue_depth: int = 0
    
    # Error metrics
    error_rate: float = 0.0
    retry_rate: float = 0.0
    
    # Worker metrics
    active_workers: int = 0
    idle_workers: int = 0
    
    # Batch metrics
    batch_size: int = 0
    batch_processing_time_ms: float = 0.0

@dataclass
class ScalingDecision:
    """Auto-scaling decision"""
    direction: ScalingDirection
    confidence: float  # 0.0 to 1.0
    reason: str
    
    # Scaling parameters
    worker_adjustment: int = 0
    batch_size_adjustment: int = 0
    queue_size_adjustment: int = 0
    
    # Performance targets
    target_docs_per_second: float = 0.0
    target_cpu_usage: float = 0.0
    target_memory_usage: float = 0.0

@dataclass
class AutoScalingConfig:
    """Configuration for auto-scaling behavior"""
    # Performance targets
    target_docs_per_second: float = 10000.0
    target_cpu_usage: float = 70.0  # 70% CPU target
    target_memory_usage: float = 80.0  # 80% memory target
    target_error_rate: float = 0.01  # 1% error rate max
    
    # Scaling thresholds
    cpu_scale_up_threshold: float = 80.0
    cpu_scale_down_threshold: float = 50.0
    memory_scale_up_threshold: float = 85.0
    memory_scale_down_threshold: float = 60.0
    
    # Scaling limits
    min_workers: int = 2
    max_workers: int = 50
    min_batch_size: int = 1000
    max_batch_size: int = 100000
    
    # Scaling intervals
    scaling_check_interval: float = 10.0  # Check every 10 seconds
    scaling_cooldown: float = 30.0  # 30 seconds between scaling actions
    
    # Performance windows
    performance_window_size: int = 20  # Last 20 metrics for analysis
    stability_window_size: int = 5  # Last 5 metrics for stability check

class IntelligentAutoScaler:
    """
    Intelligent auto-scaling engine that adapts worker count, batch sizes,
    and other parameters based on real-time performance metrics
    """
    
    def __init__(self, config: AutoScalingConfig, operation_type: OperationType):
        self.config = config
        self.operation_type = operation_type
        
        # Performance tracking
        self.metrics_history: deque = deque(maxlen=config.performance_window_size)
        self.scaling_history: List[ScalingDecision] = []
        self.last_scaling_time = 0.0
        
        # Current configuration
        self.current_workers = config.min_workers
        self.current_batch_size = config.min_batch_size
        self.current_queue_size = config.min_workers * 2
        
        # Performance baselines
        self.baseline_performance = None
        self.performance_trend = "stable"
        
        # Scaling state
        self.is_scaling_enabled = True
        self.scaling_task = None
        
    async def start_monitoring(self, metrics_callback: callable):
        """Start the auto-scaling monitoring loop"""
        logger.info(f"🚀 Starting intelligent auto-scaling for {self.operation_type.value}")
        
        self.scaling_task = asyncio.create_task(
            self._monitoring_loop(metrics_callback)
        )
        
    async def stop_monitoring(self):
        """Stop the auto-scaling monitoring"""
        if self.scaling_task:
            self.scaling_task.cancel()
            try:
                await self.scaling_task
            except asyncio.CancelledError:
                pass
        
        logger.info(f"🛑 Stopped intelligent auto-scaling for {self.operation_type.value}")
    
    async def _monitoring_loop(self, metrics_callback: callable):
        """Main monitoring loop for auto-scaling decisions"""
        while True:
            try:
                # Collect current metrics
                metrics = await metrics_callback()
                if metrics:
                    await self._process_metrics(metrics)
                
                # Wait for next check
                await asyncio.sleep(self.config.scaling_check_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in auto-scaling monitoring loop: {e}")
                await asyncio.sleep(5)  # Wait 5 seconds before retrying
    
    async def _process_metrics(self, metrics: PerformanceMetrics):
        """Process performance metrics and make scaling decisions"""
        # Add to history
        self.metrics_history.append(metrics)
        
        # Skip if not enough data
        if len(self.metrics_history) < 3:
            return
        
        # Check if we're in cooldown period
        if time.time() - self.last_scaling_time < self.config.scaling_cooldown:
            return
        
        # Analyze performance and make scaling decision
        decision = await self._analyze_performance_and_scale()
        
        if decision and decision.direction != ScalingDirection.MAINTAIN:
            await self._execute_scaling_decision(decision)
    
    async def _analyze_performance_and_scale(self) -> Optional[ScalingDecision]:
        """Analyze performance metrics and determine scaling action"""
        if len(self.metrics_history) < 3:
            return None
        
        # Get recent metrics
        recent_metrics = list(self.metrics_history)[-5:]  # Last 5 metrics
        
        # Calculate performance indicators
        avg_docs_per_second = statistics.mean([m.documents_per_second for m in recent_metrics])
        avg_cpu_usage = statistics.mean([m.cpu_usage_percent for m in recent_metrics])
        avg_memory_usage = statistics.mean([m.memory_usage_mb for m in recent_metrics])
        avg_error_rate = statistics.mean([m.error_rate for m in recent_metrics])
        
        # Calculate performance trend
        docs_trend = self._calculate_trend([m.documents_per_second for m in recent_metrics])
        cpu_trend = self._calculate_trend([m.cpu_usage_percent for m in recent_metrics])
        
        # Determine scaling direction
        decision = self._determine_scaling_direction(
            avg_docs_per_second, avg_cpu_usage, avg_memory_usage, 
            avg_error_rate, docs_trend, cpu_trend
        )
        
        return decision
    
    def _calculate_trend(self, values: List[float]) -> str:
        """Calculate trend direction from a list of values"""
        if len(values) < 2:
            return "stable"
        
        # Simple linear trend calculation
        first_half = values[:len(values)//2]
        second_half = values[len(values)//2:]
        
        first_avg = statistics.mean(first_half)
        second_avg = statistics.mean(second_half)
        
        change_percent = (second_avg - first_avg) / first_avg * 100
        
        if change_percent > 5:
            return "increasing"
        elif change_percent < -5:
            return "decreasing"
        else:
            return "stable"
    
    def _determine_scaling_direction(self, docs_per_second: float, cpu_usage: float, 
                                   memory_usage: float, error_rate: float,
                                   docs_trend: str, cpu_trend: str) -> ScalingDecision:
        """Determine the appropriate scaling direction based on metrics"""
        
        # Check for error rate issues first
        if error_rate > self.config.target_error_rate:
            return ScalingDecision(
                direction=ScalingDirection.SCALE_DOWN,
                confidence=0.9,
                reason=f"High error rate: {error_rate:.2%} > {self.config.target_error_rate:.2%}",
                worker_adjustment=-2,
                batch_size_adjustment=-1000
            )
        
        # Check CPU usage
        if cpu_usage > self.config.cpu_scale_up_threshold:
            if docs_trend == "increasing":
                return ScalingDecision(
                    direction=ScalingDirection.SCALE_UP,
                    confidence=0.8,
                    reason=f"High CPU usage ({cpu_usage:.1f}%) with increasing throughput",
                    worker_adjustment=2,
                    batch_size_adjustment=2000
                )
            else:
                return ScalingDecision(
                    direction=ScalingDirection.SCALE_DOWN,
                    confidence=0.7,
                    reason=f"High CPU usage ({cpu_usage:.1f}%) without throughput increase",
                    worker_adjustment=-1,
                    batch_size_adjustment=-1000
                )
        
        # Check memory usage
        if memory_usage > self.config.memory_scale_up_threshold:
            return ScalingDecision(
                direction=ScalingDirection.SCALE_DOWN,
                confidence=0.8,
                reason=f"High memory usage: {memory_usage:.1f}MB",
                worker_adjustment=-1,
                batch_size_adjustment=-2000
            )
        
        # Check if we can scale up for better performance
        if (cpu_usage < self.config.cpu_scale_down_threshold and 
            memory_usage < self.config.memory_scale_down_threshold and
            docs_per_second < self.config.target_docs_per_second * 0.8):
            
            return ScalingDecision(
                direction=ScalingDirection.SCALE_UP,
                confidence=0.6,
                reason=f"Low resource usage, can increase throughput (current: {docs_per_second:.0f} docs/s)",
                worker_adjustment=1,
                batch_size_adjustment=1000
            )
        
        # Check if we can scale down to optimize resources
        if (cpu_usage < self.config.cpu_scale_down_threshold and 
            memory_usage < self.config.memory_scale_down_threshold and
            docs_per_second > self.config.target_docs_per_second * 1.2):
            
            return ScalingDecision(
                direction=ScalingDirection.SCALE_DOWN,
                confidence=0.5,
                reason=f"Low resource usage with good throughput, optimizing resources",
                worker_adjustment=-1,
                batch_size_adjustment=0
            )
        
        # No scaling needed
        return ScalingDecision(
            direction=ScalingDirection.MAINTAIN,
            confidence=1.0,
            reason="Performance within target ranges"
        )
    
    async def _execute_scaling_decision(self, decision: ScalingDecision):
        """Execute the scaling decision"""
        logger.info(f"🔄 Auto-scaling decision: {decision.direction.value} - {decision.reason}")
        logger.info(f"   Confidence: {decision.confidence:.2f}")
        logger.info(f"   Worker adjustment: {decision.worker_adjustment}")
        logger.info(f"   Batch size adjustment: {decision.batch_size_adjustment}")
        
        # Apply scaling adjustments
        old_workers = self.current_workers
        old_batch_size = self.current_batch_size
        
        # Update worker count
        self.current_workers = max(
            self.config.min_workers,
            min(self.config.max_workers, self.current_workers + decision.worker_adjustment)
        )
        
        # Update batch size
        self.current_batch_size = max(
            self.config.min_batch_size,
            min(self.config.max_batch_size, self.current_batch_size + decision.batch_size_adjustment)
        )
        
        # Update queue size
        self.current_queue_size = self.current_workers * 2
        
        # Record scaling action
        self.scaling_history.append(decision)
        self.last_scaling_time = time.time()
        
        # Log the changes
        logger.info(f"📊 Scaling applied:")
        logger.info(f"   Workers: {old_workers} → {self.current_workers}")
        logger.info(f"   Batch size: {old_batch_size:,} → {self.current_batch_size:,}")
        logger.info(f"   Queue size: {self.current_queue_size}")
        
        # Notify about scaling (this would be implemented by the calling engine)
        await self._notify_scaling_change(decision)
    
    async def _notify_scaling_change(self, decision: ScalingDecision):
        """Notify the engine about scaling changes (to be implemented by caller)"""
        # This method will be overridden by the migration/generation engines
        pass
    
    def get_current_config(self) -> Dict[str, Any]:
        """Get current auto-scaling configuration"""
        return {
            "workers": self.current_workers,
            "batch_size": self.current_batch_size,
            "queue_size": self.current_queue_size,
            "is_scaling_enabled": self.is_scaling_enabled,
            "last_scaling_time": self.last_scaling_time,
            "scaling_history_count": len(self.scaling_history)
        }
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary for monitoring"""
        if not self.metrics_history:
            return {"status": "no_data"}
        
        recent_metrics = list(self.metrics_history)[-5:]
        
        return {
            "avg_docs_per_second": statistics.mean([m.documents_per_second for m in recent_metrics]),
            "avg_cpu_usage": statistics.mean([m.cpu_usage_percent for m in recent_metrics]),
            "avg_memory_usage": statistics.mean([m.memory_usage_mb for m in recent_metrics]),
            "avg_error_rate": statistics.mean([m.error_rate for m in recent_metrics]),
            "current_workers": self.current_workers,
            "current_batch_size": self.current_batch_size,
            "scaling_decisions_count": len(self.scaling_history),
            "last_scaling_reason": self.scaling_history[-1].reason if self.scaling_history else "none"
        }
