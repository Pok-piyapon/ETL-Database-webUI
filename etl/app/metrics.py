"""
Metrics & Monitoring for ETL Pipeline
Tracks performance, errors, and system health
"""

import time
import psutil
import logging
from typing import Dict, Optional
from datetime import datetime
from functools import wraps
from collections import defaultdict

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collect and track ETL metrics"""
    
    def __init__(self):
        self.metrics = {
            'tables': defaultdict(lambda: {
                'rows_processed': 0,
                'rows_failed': 0,
                'duration_seconds': 0,
                'attempts': 0,
                'last_run': None,
                'status': 'pending'
            }),
            'workers': defaultdict(lambda: {
                'rows_processed': 0,
                'chunks_processed': 0,
                'errors': 0,
                'start_time': None,
                'end_time': None
            }),
            'system': {
                'cpu_percent': 0,
                'memory_percent': 0,
                'memory_used_mb': 0,
                'disk_usage_percent': 0
            },
            'etl': {
                'total_tables': 0,
                'completed_tables': 0,
                'failed_tables': 0,
                'total_rows_processed': 0,
                'total_duration_seconds': 0,
                'start_time': None,
                'end_time': None
            },
            'errors': []
        }
        
        self.start_time = time.time()
    
    def record_table_start(self, table_name: str):
        """Record table processing start"""
        self.metrics['tables'][table_name]['start_time'] = time.time()
        self.metrics['tables'][table_name]['status'] = 'processing'
        self.metrics['tables'][table_name]['attempts'] += 1
        logger.info(f"📊 Metrics: Started tracking table {table_name}")
    
    def record_table_complete(self, table_name: str, rows_processed: int, duration: float):
        """Record table processing completion"""
        self.metrics['tables'][table_name]['rows_processed'] = rows_processed
        self.metrics['tables'][table_name]['duration_seconds'] = duration
        self.metrics['tables'][table_name]['status'] = 'completed'
        self.metrics['tables'][table_name]['last_run'] = datetime.now().isoformat()
        
        self.metrics['etl']['completed_tables'] += 1
        self.metrics['etl']['total_rows_processed'] += rows_processed
        
        logger.info(f"📊 Metrics: Completed table {table_name} - {rows_processed:,} rows in {duration:.2f}s")
    
    def record_table_failed(self, table_name: str, error: str):
        """Record table processing failure"""
        self.metrics['tables'][table_name]['status'] = 'failed'
        self.metrics['tables'][table_name]['last_error'] = error
        self.metrics['tables'][table_name]['last_run'] = datetime.now().isoformat()
        
        self.metrics['etl']['failed_tables'] += 1
        
        self.metrics['errors'].append({
            'table': table_name,
            'error': error,
            'timestamp': datetime.now().isoformat()
        })
        
        logger.error(f"📊 Metrics: Failed table {table_name} - {error}")
    
    def record_worker_progress(self, worker_id: str, rows: int, chunks: int = 1):
        """Record worker progress"""
        if self.metrics['workers'][worker_id]['start_time'] is None:
            self.metrics['workers'][worker_id]['start_time'] = time.time()
        
        self.metrics['workers'][worker_id]['rows_processed'] += rows
        self.metrics['workers'][worker_id]['chunks_processed'] += chunks
    
    def record_worker_error(self, worker_id: str, error: str):
        """Record worker error"""
        self.metrics['workers'][worker_id]['errors'] += 1
        self.metrics['workers'][worker_id]['last_error'] = error
    
    def record_worker_complete(self, worker_id: str):
        """Record worker completion"""
        self.metrics['workers'][worker_id]['end_time'] = time.time()
        start = self.metrics['workers'][worker_id]['start_time']
        if start:
            duration = time.time() - start
            self.metrics['workers'][worker_id]['duration_seconds'] = duration
    
    def update_system_metrics(self):
        """Update system resource metrics"""
        try:
            self.metrics['system']['cpu_percent'] = psutil.cpu_percent(interval=0.1)
            
            memory = psutil.virtual_memory()
            self.metrics['system']['memory_percent'] = memory.percent
            self.metrics['system']['memory_used_mb'] = memory.used / 1024 / 1024
            
            disk = psutil.disk_usage('/')
            self.metrics['system']['disk_usage_percent'] = disk.percent
            
        except Exception as e:
            logger.warning(f"Failed to update system metrics: {e}")
    
    def get_metrics(self) -> Dict:
        """Get all metrics"""
        self.update_system_metrics()
        return self.metrics
    
    def get_table_metrics(self, table_name: str) -> Dict:
        """Get metrics for specific table"""
        return dict(self.metrics['tables'].get(table_name, {}))
    
    def get_worker_metrics(self, worker_id: str) -> Dict:
        """Get metrics for specific worker"""
        return dict(self.metrics['workers'].get(worker_id, {}))
    
    def get_summary(self) -> Dict:
        """Get summary metrics"""
        total_duration = time.time() - self.start_time
        
        return {
            'total_tables': self.metrics['etl']['total_tables'],
            'completed_tables': self.metrics['etl']['completed_tables'],
            'failed_tables': self.metrics['etl']['failed_tables'],
            'total_rows_processed': self.metrics['etl']['total_rows_processed'],
            'total_duration_seconds': total_duration,
            'avg_rows_per_second': (
                self.metrics['etl']['total_rows_processed'] / total_duration 
                if total_duration > 0 else 0
            ),
            'success_rate': (
                (self.metrics['etl']['completed_tables'] / self.metrics['etl']['total_tables'] * 100)
                if self.metrics['etl']['total_tables'] > 0 else 0
            ),
            'system': self.metrics['system'],
            'errors_count': len(self.metrics['errors'])
        }
    
    def print_summary(self):
        """Print formatted summary"""
        summary = self.get_summary()
        
        print("\n" + "=" * 80)
        print("📊 ETL METRICS SUMMARY")
        print("=" * 80)
        print(f"Tables Processed: {summary['completed_tables']}/{summary['total_tables']}")
        print(f"Success Rate: {summary['success_rate']:.1f}%")
        print(f"Total Rows: {summary['total_rows_processed']:,}")
        print(f"Duration: {summary['total_duration_seconds']:.2f}s")
        print(f"Throughput: {summary['avg_rows_per_second']:.0f} rows/sec")
        print(f"\nSystem Resources:")
        print(f"  CPU: {summary['system']['cpu_percent']:.1f}%")
        print(f"  Memory: {summary['system']['memory_percent']:.1f}% ({summary['system']['memory_used_mb']:.0f} MB)")
        print(f"  Disk: {summary['system']['disk_usage_percent']:.1f}%")
        
        if summary['errors_count'] > 0:
            print(f"\n⚠️  Errors: {summary['errors_count']}")
        
        print("=" * 80 + "\n")
    
    def reset(self):
        """Reset all metrics"""
        self.__init__()


# Singleton instance
metrics_collector = MetricsCollector()


def measure_time(func):
    """Decorator to measure function execution time"""
    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        start = time.time()
        try:
            result = await func(*args, **kwargs)
            duration = time.time() - start
            logger.info(f"⏱️  {func.__name__} completed in {duration:.2f}s")
            return result
        except Exception as e:
            duration = time.time() - start
            logger.error(f"⏱️  {func.__name__} failed after {duration:.2f}s: {e}")
            raise
    
    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        start = time.time()
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start
            logger.info(f"⏱️  {func.__name__} completed in {duration:.2f}s")
            return result
        except Exception as e:
            duration = time.time() - start
            logger.error(f"⏱️  {func.__name__} failed after {duration:.2f}s: {e}")
            raise
    
    # Return appropriate wrapper based on function type
    import asyncio
    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return sync_wrapper


class PerformanceMonitor:
    """Monitor performance and detect bottlenecks"""
    
    def __init__(self):
        self.checkpoints = {}
    
    def checkpoint(self, name: str):
        """Record a checkpoint"""
        self.checkpoints[name] = time.time()
    
    def get_duration(self, start_checkpoint: str, end_checkpoint: str) -> float:
        """Get duration between two checkpoints"""
        if start_checkpoint not in self.checkpoints or end_checkpoint not in self.checkpoints:
            return 0.0
        return self.checkpoints[end_checkpoint] - self.checkpoints[start_checkpoint]
    
    def print_checkpoints(self):
        """Print all checkpoints"""
        if not self.checkpoints:
            return
        
        print("\n📍 Performance Checkpoints:")
        sorted_checkpoints = sorted(self.checkpoints.items(), key=lambda x: x[1])
        
        for i, (name, timestamp) in enumerate(sorted_checkpoints):
            if i > 0:
                prev_time = sorted_checkpoints[i-1][1]
                duration = timestamp - prev_time
                print(f"  {name}: +{duration:.2f}s")
            else:
                print(f"  {name}: START")


# Singleton instance
performance_monitor = PerformanceMonitor()


def get_system_health() -> Dict:
    """Get current system health status"""
    try:
        cpu = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        # Determine health status
        health_status = 'healthy'
        warnings = []
        
        if cpu > 90:
            health_status = 'warning'
            warnings.append(f"High CPU usage: {cpu:.1f}%")
        
        if memory.percent > 90:
            health_status = 'critical'
            warnings.append(f"High memory usage: {memory.percent:.1f}%")
        
        if disk.percent > 90:
            health_status = 'warning'
            warnings.append(f"High disk usage: {disk.percent:.1f}%")
        
        return {
            'status': health_status,
            'cpu_percent': cpu,
            'memory_percent': memory.percent,
            'memory_available_mb': memory.available / 1024 / 1024,
            'disk_percent': disk.percent,
            'disk_free_gb': disk.free / 1024 / 1024 / 1024,
            'warnings': warnings,
            'timestamp': datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"Failed to get system health: {e}")
        return {
            'status': 'unknown',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }
