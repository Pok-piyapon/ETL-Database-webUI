"""
Cache Storage System for ETL Pipeline
High-performance buffer between Producers and Consumers
"""

import asyncio
import logging
import time
import psutil
from typing import Dict, Optional, Any
from collections import deque
from dataclasses import dataclass
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class CacheItem:
    """Cache item with metadata"""
    chunk_id: int
    table_name: str
    data: pd.DataFrame
    timestamp: float
    size_mb: float
    producer_id: int


class CacheStorage:
    """
    High-performance cache storage for ETL data chunks
    
    Features:
    - Async queue-based storage
    - Memory-aware caching
    - LRU eviction policy
    - Statistics tracking
    """
    
    def __init__(self, max_size_mb: int = 1000, max_items: int = 100):
        """
        Initialize cache storage
        
        Args:
            max_size_mb: Maximum cache size in MB
            max_items: Maximum number of items in cache
        """
        self.max_size_mb = max_size_mb
        self.max_items = max_items
        
        # Storage
        self.cache: Dict[str, CacheItem] = {}  # key: f"{table}_{chunk_id}"
        self.queue = asyncio.Queue()
        self.access_order = deque()  # LRU tracking
        
        # Statistics
        self.stats = {
            'total_items_stored': 0,
            'total_items_retrieved': 0,
            'total_items_evicted': 0,
            'current_size_mb': 0,
            'current_items': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'peak_size_mb': 0,
            'peak_items': 0
        }
        
        # Locks
        self.lock = asyncio.Lock()
        
        logger.info(f"📦 Cache Storage initialized: max_size={max_size_mb}MB, max_items={max_items}")
    
    async def put(self, table_name: str, chunk_id: int, data: pd.DataFrame, producer_id: int) -> bool:
        """
        Store data chunk in cache
        
        Args:
            table_name: Table name
            chunk_id: Chunk ID
            data: DataFrame to cache
            producer_id: Producer worker ID
            
        Returns:
            True if stored successfully
        """
        async with self.lock:
            try:
                # Calculate size
                size_mb = data.memory_usage(deep=True).sum() / 1024 / 1024
                
                # Check if we need to evict
                while (self.stats['current_size_mb'] + size_mb > self.max_size_mb or 
                       self.stats['current_items'] >= self.max_items):
                    if not await self._evict_lru():
                        logger.warning("⚠️  Cache full, cannot evict more items")
                        return False
                
                # Create cache item
                cache_key = f"{table_name}_{chunk_id}"
                item = CacheItem(
                    chunk_id=chunk_id,
                    table_name=table_name,
                    data=data,
                    timestamp=time.time(),
                    size_mb=size_mb,
                    producer_id=producer_id
                )
                
                # Store in cache
                self.cache[cache_key] = item
                self.access_order.append(cache_key)
                
                # Update stats
                self.stats['total_items_stored'] += 1
                self.stats['current_items'] += 1
                self.stats['current_size_mb'] += size_mb
                
                # Track peaks
                if self.stats['current_size_mb'] > self.stats['peak_size_mb']:
                    self.stats['peak_size_mb'] = self.stats['current_size_mb']
                if self.stats['current_items'] > self.stats['peak_items']:
                    self.stats['peak_items'] = self.stats['current_items']
                
                # Put in queue for consumers
                await self.queue.put(cache_key)
                
                logger.debug(f"📦 Cached: {cache_key} ({size_mb:.2f}MB) - Cache: {self.stats['current_items']} items, {self.stats['current_size_mb']:.1f}MB")
                
                return True
                
            except Exception as e:
                logger.error(f"❌ Cache put error: {e}")
                return False
    
    async def get(self, timeout: float = 5.0) -> Optional[CacheItem]:
        """
        Retrieve data chunk from cache
        
        Args:
            timeout: Timeout in seconds
            
        Returns:
            CacheItem or None if timeout
        """
        try:
            # Get from queue (blocks until available)
            cache_key = await asyncio.wait_for(self.queue.get(), timeout=timeout)
            
            async with self.lock:
                # Get from cache
                if cache_key in self.cache:
                    item = self.cache[cache_key]
                    
                    # Remove from cache
                    del self.cache[cache_key]
                    if cache_key in self.access_order:
                        self.access_order.remove(cache_key)
                    
                    # Update stats
                    self.stats['total_items_retrieved'] += 1
                    self.stats['current_items'] -= 1
                    self.stats['current_size_mb'] -= item.size_mb
                    self.stats['cache_hits'] += 1
                    
                    logger.debug(f"📤 Retrieved: {cache_key} ({item.size_mb:.2f}MB) - Cache: {self.stats['current_items']} items")
                    
                    return item
                else:
                    self.stats['cache_misses'] += 1
                    logger.warning(f"⚠️  Cache miss: {cache_key}")
                    return None
                    
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            logger.error(f"❌ Cache get error: {e}")
            return None
    
    async def _evict_lru(self) -> bool:
        """
        Evict least recently used item
        
        Returns:
            True if evicted successfully
        """
        if not self.access_order:
            return False
        
        # Get LRU item
        cache_key = self.access_order.popleft()
        
        if cache_key in self.cache:
            item = self.cache[cache_key]
            
            # Remove from cache
            del self.cache[cache_key]
            
            # Update stats
            self.stats['total_items_evicted'] += 1
            self.stats['current_items'] -= 1
            self.stats['current_size_mb'] -= item.size_mb
            
            logger.debug(f"🗑️  Evicted LRU: {cache_key} ({item.size_mb:.2f}MB)")
            
            return True
        
        return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        
        return {
            **self.stats,
            'process_memory_mb': memory_mb,
            'cache_hit_rate': (
                self.stats['cache_hits'] / (self.stats['cache_hits'] + self.stats['cache_misses']) * 100
                if (self.stats['cache_hits'] + self.stats['cache_misses']) > 0 else 0
            ),
            'avg_item_size_mb': (
                self.stats['current_size_mb'] / self.stats['current_items']
                if self.stats['current_items'] > 0 else 0
            )
        }
    
    def print_stats(self):
        """Print cache statistics"""
        stats = self.get_stats()
        
        print("\n" + "=" * 80)
        print("📦 CACHE STORAGE STATISTICS")
        print("=" * 80)
        print(f"Current Items: {stats['current_items']} / {self.max_items}")
        print(f"Current Size: {stats['current_size_mb']:.1f}MB / {self.max_size_mb}MB")
        print(f"Peak Items: {stats['peak_items']}")
        print(f"Peak Size: {stats['peak_size_mb']:.1f}MB")
        print(f"\nTotal Stored: {stats['total_items_stored']:,}")
        print(f"Total Retrieved: {stats['total_items_retrieved']:,}")
        print(f"Total Evicted: {stats['total_items_evicted']:,}")
        print(f"\nCache Hit Rate: {stats['cache_hit_rate']:.1f}%")
        print(f"Avg Item Size: {stats['avg_item_size_mb']:.2f}MB")
        print(f"Process Memory: {stats['process_memory_mb']:.1f}MB")
        print("=" * 80 + "\n")
    
    async def clear(self):
        """Clear all cache"""
        async with self.lock:
            self.cache.clear()
            self.access_order.clear()
            
            # Clear queue
            while not self.queue.empty():
                try:
                    self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            
            self.stats['current_items'] = 0
            self.stats['current_size_mb'] = 0
            
            logger.info("🗑️  Cache cleared")
    
    def is_empty(self) -> bool:
        """Check if cache is empty"""
        return self.stats['current_items'] == 0
    
    def is_full(self) -> bool:
        """Check if cache is full"""
        return (self.stats['current_size_mb'] >= self.max_size_mb or 
                self.stats['current_items'] >= self.max_items)


# Singleton instance - 3GB cache for high performance
cache_storage = CacheStorage(max_size_mb=3000, max_items=300)


async def monitor_cache(interval: int = 10):
    """
    Monitor cache storage periodically
    
    Args:
        interval: Monitoring interval in seconds
    """
    while True:
        await asyncio.sleep(interval)
        
        stats = cache_storage.get_stats()
        
        logger.info(
            f"📦 Cache: {stats['current_items']} items, "
            f"{stats['current_size_mb']:.1f}MB, "
            f"Hit rate: {stats['cache_hit_rate']:.1f}%"
        )
        
        # Warning if cache is getting full
        if stats['current_size_mb'] > cache_storage.max_size_mb * 0.8:
            logger.warning(f"⚠️  Cache usage high: {stats['current_size_mb']:.1f}MB / {cache_storage.max_size_mb}MB")
