from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Optional, TypeVar
from collections import OrderedDict
import asyncio
import logging
import inspect

T = TypeVar('T')

@dataclass
class CacheConfig:
    """Configuration for cache behavior"""
    ttl: int = 300  # seconds
    max_size: int = 1000
    strategy: str = "lru"  # lru or fifo

@dataclass
class CacheMetrics:
    """Metrics for cache monitoring"""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    size: int = 0
    
    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return round(self.hits / total, 4) if total > 0 else 0.0

class Cache:
    """Advanced caching decorator"""
    def __init__(
        self,
        ttl: int = 300,
        max_size: int = 1000,
        strategy: str = "lru",
        logger: Optional[logging.Logger] = None
    ) -> None:
        self.config = CacheConfig(ttl=ttl, max_size=max_size, strategy=strategy)
        self.logger = logger or logging.getLogger(__name__)
        self.metrics = CacheMetrics()
        self.cache: OrderedDict = OrderedDict()
        self.lock = asyncio.Lock()
        
    def __call__(self, func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def async_wrapper(*args, **kwargs) -> T:
            key = self._make_key(func, args, kwargs)
            
            async with self.lock:
                if self._get_cached_value(key):
                    self.metrics.hits += 1
                    return self.cache[key]['value']
                
                self.metrics.misses += 1
                result = await func(*args, **kwargs)
                self._cache_value(key, result)
                return result
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs) -> T:
            key = self._make_key(func, args, kwargs)
            
            if self._get_cached_value(key):
                self.metrics.hits += 1
                return self.cache[key]['value']
            
            self.metrics.misses += 1
            result = func(*args, **kwargs)
            self._cache_value(key, result)
            return result
        
        return async_wrapper if inspect.iscoroutinefunction(func) else sync_wrapper
    
    def _make_key(self, func: Callable, args: tuple, kwargs: dict) -> str:
        key_parts = [
            func.__module__,
            func.__name__,
            str(args),
            str(sorted(kwargs.items()))
        ]
        return ":".join(key_parts)
    
    def _get_cached_value(self, key: str) -> bool:
        if key not in self.cache:
            return False
            
        entry = self.cache[key]
        if entry['expires_at'] < datetime.now():
            del self.cache[key]
            return False
            
        if self.config.strategy == "lru":
            self.cache.move_to_end(key)
        return True
    
    def _cache_value(self, key: str, value: Any) -> None:
        while len(self.cache) >= self.config.max_size:
            self.cache.popitem(last=False)
            self.metrics.evictions += 1
        
        self.cache[key] = {
            'value': value,
            'expires_at': datetime.now() + timedelta(seconds=self.config.ttl)
        }
        self.metrics.size = len(self.cache)
        
        if self.config.strategy == "lru":
            self.cache.move_to_end(key)