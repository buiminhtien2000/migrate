from dataclasses import dataclass
from functools import wraps
from typing import Optional, Any, Dict, Callable, Tuple, TypeVar
from collections import deque
import asyncio
import time
import math

T = TypeVar('T')

@dataclass
class RateLimitConfig:
    """Configuration for rate limiting"""
    max_calls: int  # Maximum number of calls allowed
    time_window: float  # Time window in seconds
    strategy: str = "fixed"  # 'fixed' or 'sliding'
    burst_size: Optional[int] = None  # Maximum burst size (None = max_calls)
    tolerance: float = 0.05  # Tolerance for timing precision (5%)

class RateLimitExceededError(Exception):
    """Raised when rate limit is exceeded"""
    def __init__(self, wait_time: float):
        self.wait_time = wait_time
        super().__init__(f"Rate limit exceeded. Please wait {wait_time:.2f} seconds")

class RateLimit:
    """
    Decorator class for rate limiting with multiple strategies
    
    Features:
    - Fixed and sliding window rate limiting
    - Burst handling
    - Distributed rate limiting support (via Redis - optional)
    - Custom exception handling
    - Rate limit metrics
    """
    
    def __init__(
        self,
        config: RateLimitConfig,
        redis_client: Any = None,
        key_func: Optional[Callable[..., str]] = None,
        logger: 'CustomLogger' = None, # type: ignore
        on_limit: Optional[Callable[[float], None]] = None
    ):
        self.config = config
        self.redis = redis_client
        self.key_func = key_func or (lambda *args, **kwargs: "default")
        self.logger = logger
        self.on_limit = on_limit
        
        # Initialize rate limit tracking
        self.calls: deque = deque(maxlen=self.config.max_calls)
        self.burst_size = config.burst_size or config.max_calls
        self._lock = asyncio.Lock()

    def _clean_old_calls(self, now: float) -> None:
        """Remove calls outside the time window"""
        while self.calls and now - self.calls[0] >= self.config.time_window:
            self.calls.popleft()

    async def _check_redis_limit(self, key: str) -> Tuple[bool, float]:
        """Check rate limit using Redis"""
        if not self.redis:
            return True, 0
            
        now = time.time()
        pipe = self.redis.pipeline()
        
        # Remove old entries and add new one
        pipe.zremrangebyscore(key, 0, now - self.config.time_window)
        pipe.zadd(key, {str(now): now})
        pipe.zcard(key)
        pipe.expire(key, int(self.config.time_window * 2))
        
        _, _, count, _ = await pipe.execute()
        
        if count <= self.config.max_calls:
            return True, 0
            
        # Calculate wait time
        oldest = float(await self.redis.zrange(key, 0, 0, withscores=True)[0][1])
        wait_time = oldest + self.config.time_window - now
        return False, max(0, wait_time)

    def _calculate_wait_time(self, now: float) -> float:
        """Calculate wait time before next allowed call"""
        if len(self.calls) < self.config.max_calls:
            return 0
            
        if self.config.strategy == "fixed":
            window_start = math.floor(now / self.config.time_window) * self.config.time_window
            next_window = window_start + self.config.time_window
            return next_window - now
        else:  # sliding window
            oldest_call = self.calls[0]
            return oldest_call + self.config.time_window - now

    def __call__(self, func: Callable[..., T]) -> Callable[..., T]:
        """Decorator for synchronous functions"""
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            now = time.time()
            key = self.key_func(*args, **kwargs)
            
            # Clean old calls
            self._clean_old_calls(now)
            
            # Check rate limit
            if len(self.calls) >= self.config.max_calls:
                wait_time = self._calculate_wait_time(now)
                if wait_time > self.config.tolerance:
                    if self.on_limit:
                        self.on_limit(wait_time)
                    raise RateLimitExceededError(wait_time)
            
            # Add current call
            self.calls.append(now)
            return func(*args, **kwargs)

        return wrapper

    def async_limit(self, func: Callable[..., T]) -> Callable[..., T]:
        """Decorator for asynchronous functions"""
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            async with self._lock:
                now = time.time()
                key = self.key_func(*args, **kwargs)
                
                # Check Redis rate limit if configured
                if self.redis:
                    allowed, wait_time = await self._check_redis_limit(f"ratelimit:{key}")
                    if not allowed:
                        if self.on_limit:
                            self.on_limit(wait_time)
                        raise RateLimitExceededError(wait_time)
                
                # Clean old calls
                self._clean_old_calls(now)
                
                # Check rate limit
                if len(self.calls) >= self.config.max_calls:
                    wait_time = self._calculate_wait_time(now)
                    if wait_time > self.config.tolerance:
                        if self.on_limit:
                            self.on_limit(wait_time)
                        raise RateLimitExceededError(wait_time)
                
                # Add current call
                self.calls.append(now)
                return await func(*args, **kwargs)

        return wrapper

    def get_metrics(self) -> Dict[str, Any]:
        """Get current rate limit metrics"""
        now = time.time()
        self._clean_old_calls(now)
        
        return {
            "total_calls": len(self.calls),
            "remaining_calls": self.config.max_calls - len(self.calls),
            "window_size": self.config.time_window,
            "burst_size": self.burst_size,
            "is_limited": len(self.calls) >= self.config.max_calls,
            "wait_time": self._calculate_wait_time(now) if self.calls else 0,
        }