"""
Enhanced Progress Monitoring System
Multi-progress bar system with real-time metrics and system monitoring
"""
import asyncio
import logging
import time
import sys
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum
import threading

try:
    from tqdm import tqdm
    from tqdm.asyncio import tqdm as atqdm
except ImportError:
    tqdm = None
    atqdm = None

logger = logging.getLogger(__name__)

class ProgressBarType(Enum):
    """Types of progress bars"""
    MAIN_MIGRATION = "main_migration"
    WORKER_SCALING = "worker_scaling"
    CHECKPOINT_SAVE = "checkpoint_save"
    SYSTEM_METRICS = "system_metrics"

@dataclass
class SystemMetrics:
    """System performance metrics"""
    cpu_usage: float
    memory_usage: float
    disk_io: float
    network_io: float
    timestamp: float

@dataclass
class WorkerStats:
    """Worker statistics"""
    active_read_workers: int
    active_write_workers: int
    total_workers: int
    scaling_events: int
    last_scale_time: float

class EnhancedProgressMonitor:
    """
    Enhanced progress monitoring with multiple progress bars and system metrics
    
    Features:
    - Main migration progress bar
    - Worker scaling progress bar
    - Checkpoint save progress bar
    - System metrics display
    - Real-time statistics
    - Summary reports
    """
    
    def __init__(self, config):
        self.config = config
        self.is_running = False
        
        # Progress bars
        self.main_pbar = None
        self.worker_pbar = None
        self.checkpoint_pbar = None
        self.system_pbar = None
        
        # Statistics
        self.stats = {
            'start_time': time.time(),
            'peak_rate': 0,
            'total_errors': 0,
            'total_retries': 0,
            'ru_consumption': 0,
            'memory_usage': 0,
            'checkpoints_saved': 0,
            'worker_scales': 0,
            'last_checkpoint_time': 0
        }
        
        # Worker statistics
        self.worker_stats = WorkerStats(
            active_read_workers=0,
            active_write_workers=0,
            total_workers=0,
            scaling_events=0,
            last_scale_time=0
        )
        
        # System metrics
        self.system_metrics = []
        self.metrics_lock = threading.Lock()
        
    def initialize_progress_bars(self, total_docs: int, migrated_so_far: int = 0):
        """Initialize all progress bars"""
        if not tqdm:
            logger.warning("tqdm not available, using basic logging")
            return
            
        # Main migration progress bar
        self.main_pbar = tqdm(
            total=total_docs,
            desc="🚀 Migrating data",
            unit="docs",
            unit_scale=True,
            ncols=140,
            bar_format='{desc}: {percentage:3.0f}%|{bar:25}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}',
            colour='blue',
            smoothing=self.config.performance.progress_smoothing,
            miniters=self.config.performance.progress_miniters,
            dynamic_ncols=True,
            leave=True,
            initial=migrated_so_far,
            position=0,
            file=sys.stdout
        )
        
        # Worker scaling progress bar
        self.worker_pbar = tqdm(
            total=100,
            desc="👥 Worker Scaling",
            unit="%",
            ncols=120,
            bar_format='{desc}: {percentage:3.0f}%|{bar:15}| Workers: {postfix}',
            colour='green',
            position=1,
            file=sys.stdout
        )
        
        # Checkpoint save progress bar
        self.checkpoint_pbar = tqdm(
            total=100,
            desc="💾 Checkpoint Save",
            unit="%",
            ncols=120,
            bar_format='{desc}: {percentage:3.0f}%|{bar:15}| {postfix}',
            colour='yellow',
            position=2,
            file=sys.stdout
        )
        
        # Database performance progress bar (replaces system metrics)
        self.db_performance_pbar = tqdm(
            total=100,
            desc="📊 Database Performance",
            unit="%",
            ncols=120,
            bar_format='{desc}: {percentage:3.0f}%|{bar:15}| {postfix}',
            colour='cyan',
            position=3,
            file=sys.stdout
        )
        
        self.is_running = True
        
    def update_main_progress(self, documents_migrated: int, current_rate: float):
        """Update main migration progress"""
        if self.main_pbar:
            # Update peak rate
            if current_rate > self.stats['peak_rate']:
                self.stats['peak_rate'] = current_rate
            
            # Get memory usage
            try:
                import psutil
                memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
                self.stats['memory_usage'] = memory_mb
            except ImportError:
                memory_mb = 0
            
            # Update postfix with enhanced statistics
            stats_str = f"Peak: {self.stats['peak_rate']:.0f}docs/s | Mem: {memory_mb:.0f}MB | Errors: {self.stats['total_errors']}"
            self.main_pbar.set_postfix_str(stats_str)
            self.main_pbar.update(documents_migrated - self.main_pbar.n)
    
    def update_worker_scaling(self, read_workers: int, write_workers: int, scaling_event: bool = False):
        """Update worker scaling progress"""
        if self.worker_pbar:
            self.worker_stats.active_read_workers = read_workers
            self.worker_stats.active_write_workers = write_workers
            self.worker_stats.total_workers = read_workers + write_workers
            
            if scaling_event:
                self.worker_stats.scaling_events += 1
                self.worker_stats.last_scale_time = time.time()
                self.stats['worker_scales'] += 1
            
            # Calculate worker utilization percentage
            max_workers = self.config.performance.max_workers + self.config.performance.max_write_workers
            utilization = (self.worker_stats.total_workers / max_workers) * 100
            
            worker_info = f"R:{read_workers} W:{write_workers} Scales:{self.worker_stats.scaling_events}"
            self.worker_pbar.set_postfix_str(worker_info)
            self.worker_pbar.n = utilization
            self.worker_pbar.refresh()
    
    def update_checkpoint_progress(self, checkpoint_progress: float, checkpoint_info: str = ""):
        """Update checkpoint save progress"""
        if self.checkpoint_pbar:
            self.checkpoint_pbar.n = checkpoint_progress
            self.checkpoint_pbar.set_postfix_str(checkpoint_info)
            self.checkpoint_pbar.refresh()
            
            if checkpoint_progress >= 100:
                self.stats['checkpoints_saved'] += 1
                self.stats['last_checkpoint_time'] = time.time()
    
    def update_database_performance(self, ru_consumption: float = 0, throttling_detected: bool = False, 
                                  connection_pool_size: int = 0, queue_size: int = 0):
        """Update database performance metrics"""
        if self.db_performance_pbar:
            # Calculate performance percentage based on RU consumption and throttling
            if ru_consumption > 0:
                # Normalize RU consumption to percentage (assuming 1000 RU/s is 100%)
                performance_percent = min(100, (ru_consumption / 1000) * 100)
            else:
                performance_percent = 50  # Default middle value
            
            # Create performance string with useful metrics
            perf_parts = []
            if ru_consumption > 0:
                perf_parts.append(f"RU:{ru_consumption:.0f}/s")
            if connection_pool_size > 0:
                perf_parts.append(f"Pool:{connection_pool_size}")
            if queue_size > 0:
                perf_parts.append(f"Queue:{queue_size}")
            if throttling_detected:
                perf_parts.append("⚠️THROTTLED")
            
            perf_str = " ".join(perf_parts) if perf_parts else "Initializing..."
            
            self.db_performance_pbar.n = performance_percent
            self.db_performance_pbar.set_postfix_str(perf_str)
            self.db_performance_pbar.refresh()
    
    def show_snapshot_complete(self, snapshot_info: str):
        """Show snapshot/checkpoint completion"""
        logger.info(f"✅ Snapshot complete: {snapshot_info}")
        if self.checkpoint_pbar:
            self.checkpoint_pbar.set_postfix_str(f"✅ {snapshot_info}")
            self.checkpoint_pbar.refresh()
    
    def show_worker_scale_event(self, event_type: str, old_count: int, new_count: int):
        """Show worker scaling event"""
        logger.info(f"🔄 Worker scaling: {event_type} from {old_count} to {new_count} workers")
        self.update_worker_scaling(new_count, self.worker_stats.active_write_workers, scaling_event=True)
    
    def generate_summary_report(self) -> Dict[str, Any]:
        """Generate comprehensive summary report"""
        total_time = time.time() - self.stats['start_time']
        
        # Calculate averages
        avg_cpu = sum(m.cpu_usage for m in self.system_metrics) / len(self.system_metrics) if self.system_metrics else 0
        avg_memory = sum(m.memory_usage for m in self.system_metrics) / len(self.system_metrics) if self.system_metrics else 0
        peak_cpu = max(m.cpu_usage for m in self.system_metrics) if self.system_metrics else 0
        peak_memory = max(m.memory_usage for m in self.system_metrics) if self.system_metrics else 0
        
        return {
            "migration_summary": {
                "total_time_seconds": total_time,
                "total_time_formatted": f"{total_time//3600:.0f}h {(total_time%3600)//60:.0f}m {total_time%60:.0f}s",
                "peak_migration_rate": self.stats['peak_rate'],
                "total_errors": self.stats['total_errors'],
                "total_retries": self.stats['total_retries'],
                "checkpoints_saved": self.stats['checkpoints_saved'],
                "worker_scales": self.stats['worker_scales']
            },
            "worker_statistics": {
                "final_read_workers": self.worker_stats.active_read_workers,
                "final_write_workers": self.worker_stats.active_write_workers,
                "total_workers": self.worker_stats.total_workers,
                "scaling_events": self.worker_stats.scaling_events,
                "last_scale_time": self.worker_stats.last_scale_time
            },
            "system_performance": {
                "average_cpu_usage": avg_cpu,
                "average_memory_usage": avg_memory,
                "peak_cpu_usage": peak_cpu,
                "peak_memory_usage": peak_memory,
                "final_memory_usage_mb": self.stats['memory_usage']
            },
            "performance_metrics": {
                "documents_per_second": self.stats['peak_rate'],
                "memory_efficiency": f"{self.stats['memory_usage']:.1f}MB",
                "error_rate": (self.stats['total_errors'] / max(1, self.stats['peak_rate'] * total_time)) * 100,
                "checkpoint_frequency": self.stats['checkpoints_saved'] / max(1, total_time / 60)  # per minute
            }
        }
    
    def print_summary_report(self):
        """Print formatted summary report"""
        report = self.generate_summary_report()
        
        print("\n" + "="*80)
        print("📊 DATAWORKS MIGRATION SUMMARY REPORT")
        print("="*80)
        
        # Migration Summary
        print(f"\n🚀 MIGRATION SUMMARY:")
        print(f"   • Total Time: {report['migration_summary']['total_time_formatted']}")
        print(f"   • Peak Rate: {report['migration_summary']['peak_migration_rate']:.0f} docs/s")
        print(f"   • Total Errors: {report['migration_summary']['total_errors']}")
        print(f"   • Checkpoints Saved: {report['migration_summary']['checkpoints_saved']}")
        print(f"   • Worker Scales: {report['migration_summary']['worker_scales']}")
        
        # Worker Statistics
        print(f"\n👥 WORKER STATISTICS:")
        print(f"   • Final Read Workers: {report['worker_statistics']['final_read_workers']}")
        print(f"   • Final Write Workers: {report['worker_statistics']['final_write_workers']}")
        print(f"   • Total Workers: {report['worker_statistics']['total_workers']}")
        print(f"   • Scaling Events: {report['worker_statistics']['scaling_events']}")
        
        # System Performance
        print(f"\n📊 SYSTEM PERFORMANCE:")
        print(f"   • Average CPU: {report['system_performance']['average_cpu_usage']:.1f}%")
        print(f"   • Average Memory: {report['system_performance']['average_memory_usage']:.1f}%")
        print(f"   • Peak CPU: {report['system_performance']['peak_cpu_usage']:.1f}%")
        print(f"   • Peak Memory: {report['system_performance']['peak_memory_usage']:.1f}%")
        
        # Performance Metrics
        print(f"\n⚡ PERFORMANCE METRICS:")
        print(f"   • Documents/Second: {report['performance_metrics']['documents_per_second']:.0f}")
        print(f"   • Memory Efficiency: {report['performance_metrics']['memory_efficiency']}")
        print(f"   • Error Rate: {report['performance_metrics']['error_rate']:.2f}%")
        print(f"   • Checkpoint Frequency: {report['performance_metrics']['checkpoint_frequency']:.1f}/min")
        
        print("\n" + "="*80)
        print("✅ MIGRATION COMPLETED SUCCESSFULLY!")
        print("="*80)
    
    def close_all(self):
        """Close all progress bars"""
        if self.main_pbar:
            self.main_pbar.close()
        if self.worker_pbar:
            self.worker_pbar.close()
        if self.checkpoint_pbar:
            self.checkpoint_pbar.close()
        if self.db_performance_pbar:
            self.db_performance_pbar.close()
        
        self.is_running = False
