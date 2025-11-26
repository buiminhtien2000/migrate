from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Any, Dict, Callable, Type, Tuple, Generic, TypeVar, List
from enum import Enum
import asyncio
import random
import json
import logging

T = TypeVar('T')  # Generic type for return value

@dataclass
class RetryConfig:
    """Configuration for retry behavior"""
    max_retries: int = 3
    initial_delay: float = 1.0  # seconds
    max_delay: float = 60.0  # seconds
    exponential_base: float = 2
    jitter: bool = True

    def get_delay(self, attempt: int) -> float:
        """Calculate delay with exponential backoff and optional jitter"""
        delay = min(
            self.initial_delay * (self.exponential_base ** attempt),
            self.max_delay
        )
        if self.jitter:
            delay *= (0.5 + random.random())
        return delay

@dataclass
class HealthCheck:
    """Health check configuration and status"""
    callback: Callable[[], bool]
    interval: int  # seconds
    timeout: int  # seconds
    last_check: Optional[datetime] = None
    is_healthy: bool = False

@dataclass
class CircuitBreakerMetrics:
    """Enhanced metrics for circuit breaker monitoring"""
    total_failures: int = 0
    consecutive_successes: int = 0
    total_requests: int = 0
    success_rate: float = 0.0
    last_success_time: Optional[datetime] = None
    retry_attempts: int = 0
    average_response_time: float = 0.0
    response_times: List[float] = field(default_factory=list)
    error_counts: Dict[str, int] = field(default_factory=dict)
    
    def add_response_time(self, response_time: float) -> None:
        """Add response time and update average"""
        self.response_times.append(response_time)
        if len(self.response_times) > 100:  # Keep last 100 responses
            self.response_times.pop(0)
        self.average_response_time = sum(self.response_times) / len(self.response_times)

    def record_error(self, error_type: str) -> None:
        """Record error type for monitoring"""
        self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1

    def reset(self) -> None:
        """Reset all metrics"""
        self.__init__()

class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

class CircuitBreakerError(Exception):
    """Base exception for circuit breaker errors"""
    pass

class CircuitBreakerOpenError(CircuitBreakerError):
    """Error when circuit is open"""
    pass

class CircuitBreakerHalfOpenError(CircuitBreakerError):
    """Error when circuit is half-open and processing request"""
    pass

class CircuitBreaker(Generic[T]):
    """
    Enhanced circuit breaker implementation with advanced features
    
    Features:
    - Exponential backoff with jitter
    - Health checks
    - Advanced metrics collection
    - Structured logging
    - Configurable half-open state
    """
    
    def __init__(
        self,
        name: str,
        logger: Optional[logging.Logger] = None,
        failure_threshold: int = 5,
        reset_timeout: int = 60,
        excluded_exceptions: Tuple[Type[Exception], ...] = tuple(),
        retry_config: Optional[RetryConfig] = None,
        health_check: Optional[HealthCheck] = None,
        half_open_max_tries: int = 3
    ) -> None:
        self._validate_inputs(failure_threshold, reset_timeout)
        
        self.name = name
        self.logger = logger or logging.getLogger(__name__)
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.excluded_exceptions = excluded_exceptions
        self.retry_config = retry_config or RetryConfig()
        self.health_check = health_check
        self.half_open_max_tries = half_open_max_tries

        self._state = CircuitBreakerState.CLOSED
        self._failure_count: int = 0
        self._half_open_tries: int = 0
        self._last_failure_time: Optional[datetime] = None
        self._lock = asyncio.Lock()
        self._half_open_lock = asyncio.Lock()
        self._metrics = CircuitBreakerMetrics()
        
        # Start health check task if configured
        if self.health_check:
            asyncio.create_task(self._health_check_loop())

    @staticmethod
    def _validate_inputs(failure_threshold: int, reset_timeout: int) -> None:
        """Validate initialization parameters"""
        if failure_threshold < 1:
            raise ValueError("Failure threshold must be at least 1")
        if reset_timeout < 1:
            raise ValueError("Reset timeout must be at least 1 second")

    async def _health_check_loop(self) -> None:
        """Continuous health check loop"""
        while True:
            try:
                if self.health_check:
                    self.health_check.is_healthy = await asyncio.wait_for(
                        asyncio.to_thread(self.health_check.callback),
                        timeout=self.health_check.timeout
                    )
                    self.health_check.last_check = datetime.now()
                    
                    if self.health_check.is_healthy and self._state == CircuitBreakerState.OPEN:
                        await self._attempt_reset()
                    
                    self._log_event("health_check", {
                        "is_healthy": self.health_check.is_healthy,
                        "last_check": self.health_check.last_check.isoformat()
                    })
                    
                await asyncio.sleep(self.health_check.interval)
            except Exception as e:
                self._log_event("health_check_error", {"error": str(e)})
                await asyncio.sleep(1)  # Prevent tight loop on persistent errors

    async def _attempt_reset(self) -> None:
        """Attempt to reset circuit breaker state"""
        async with self._lock:
            if self._state == CircuitBreakerState.OPEN and await self._should_reset():
                self._state = CircuitBreakerState.HALF_OPEN
                self._half_open_tries = 0
                self._log_event("state_change", {"new_state": "HALF_OPEN"})

    async def call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute function with circuit breaker protection and retry logic"""
        self._metrics.total_requests += 1
        start_time = datetime.now()
        
        for attempt in range(self.retry_config.max_retries):
            try:
                async with self._lock:
                    await self._check_state()
                
                async with self:
                    result = await func(*args, **kwargs)
                    response_time = (datetime.now() - start_time).total_seconds()
                    self._metrics.add_response_time(response_time)
                    await self._on_success()
                    return result
                    
            except CircuitBreakerError:
                raise
            except Exception as e:
                if isinstance(e, self.excluded_exceptions):
                    raise
                
                self._metrics.record_error(type(e).__name__)
                await self._on_failure(e)
                
                if attempt < self.retry_config.max_retries - 1:
                    delay = self.retry_config.get_delay(attempt)
                    self._log_event("retry", {
                        "attempt": attempt + 1,
                        "delay": delay,
                        "error": str(e)
                    })
                    await asyncio.sleep(delay)
                else:
                    raise

    def _log_event(self, event_type: str, data: Dict[str, Any]) -> None:
        """Structured logging of circuit breaker events"""
        log_data = {
            "timestamp": datetime.now().isoformat(),
            "circuit": self.name,
            "state": self._state.value,
            "event": event_type,
            "metrics": self.metrics,
            **data
        }
        self.logger.info(json.dumps(log_data))

    async def _check_state(self) -> None:
        """Check and validate current state with half-open management"""
        if self._state == CircuitBreakerState.OPEN:
            if await self._should_reset():
                self._state = CircuitBreakerState.HALF_OPEN
                self._half_open_tries = 0
                self._log_event("state_change", {"new_state": "HALF_OPEN"})
            else:
                remaining = self._get_remaining_timeout()
                raise CircuitBreakerOpenError(
                    f"Circuit {self.name} is OPEN. Next retry in {remaining:.1f}s"
                )

        if self._state == CircuitBreakerState.HALF_OPEN:
            if self._half_open_tries >= self.half_open_max_tries:
                self._state = CircuitBreakerState.OPEN
                self._log_event("state_change", {
                    "new_state": "OPEN",
                    "reason": "max_half_open_tries_exceeded"
                })
                raise CircuitBreakerOpenError(
                    f"Circuit {self.name} exceeded maximum half-open tries"
                )
            
            if not await self._half_open_lock.acquire(False):
                raise CircuitBreakerHalfOpenError(
                    f"Circuit {self.name} is HALF-OPEN and already processing a request"
                )
            
            self._half_open_tries += 1

    async def _on_success(self) -> None:
        """Handle successful execution with state transitions"""
        async with self._lock:
            await self._update_metrics_on_success()
            
            if self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.CLOSED
                self._failure_count = 0
                self._half_open_tries = 0
                self._log_event("state_change", {
                    "new_state": "CLOSED",
                    "reason": "success_in_half_open"
                })
                
            if self._half_open_lock.locked():
                self._half_open_lock.release()

    async def _on_failure(self, exception: Exception) -> None:
        """Handle failure with proper state management"""
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = datetime.now()
            await self._update_metrics_on_failure()

            if self._state == CircuitBreakerState.CLOSED and self._failure_count >= self.failure_threshold:
                self._state = CircuitBreakerState.OPEN
                self._log_event("state_change", {
                    "new_state": "OPEN",
                    "reason": "failure_threshold_reached"
                })
            
            if self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.OPEN
                self._log_event("state_change", {
                    "new_state": "OPEN",
                    "reason": "failure_in_half_open"
                })
                
            if self._half_open_lock.locked():
                self._half_open_lock.release()

    @property
    def metrics(self) -> Dict[str, Any]:
        """Enhanced metrics with additional monitoring data"""
        return {
            'total_failures': self._metrics.total_failures,
            'consecutive_successes': self._metrics.consecutive_successes,
            'total_requests': self._metrics.total_requests,
            'success_rate': round(self._metrics.success_rate, 4),
            'last_success_time': (
                self._metrics.last_success_time.isoformat()
                if self._metrics.last_success_time
                else None
            ),
            'average_response_time': round(self._metrics.average_response_time, 3),
            'retry_attempts': self._metrics.retry_attempts,
            'error_distribution': self._metrics.error_counts
        }

    async def _update_metrics_on_success(self) -> None:
        """Update metrics after successful execution"""
        self._metrics.consecutive_successes += 1
        self._metrics.last_success_time = datetime.now()
        self._metrics.success_rate = (
            (self._metrics.total_requests - self._metrics.total_failures) /
            self._metrics.total_requests if self._metrics.total_requests > 0 else 0.0
        )

    async def _update_metrics_on_failure(self) -> None:
        """Update metrics after failed execution"""
        self._metrics.total_failures += 1
        self._metrics.consecutive_successes = 0
        self._metrics.success_rate = (
            (self._metrics.total_requests - self._metrics.total_failures) /
            self._metrics.total_requests if self._metrics.total_requests > 0 else 0.0
        )
        
    def get_stats(self) -> Dict[str, Any]:
        """Get detailed circuit breaker statistics"""
        stats = {
            'state': self._state.value,
            'failure_count': self._failure_count,
            'half_open_tries': self._half_open_tries,
            'last_failure': (
                self._last_failure_time.isoformat()
                if self._last_failure_time
                else None
            ),
            'metrics': self.metrics,
            'remaining_timeout': round(self._get_remaining_timeout(), 2)
        }
        
        if self.health_check:
            stats['health_check'] = {
                'is_healthy': self.health_check.is_healthy,
                'last_check': (
                    self.health_check.last_check.isoformat()
                    if self.health_check.last_check
                    else None
                )
            }
            
        return stats

    async def _should_reset(self) -> bool:
        """
        Check if circuit breaker should reset from OPEN to HALF-OPEN state
        """
        if self._last_failure_time is None:
            return True
            
        elapsed = (datetime.now() - self._last_failure_time).total_seconds()
        should_reset = elapsed >= self.reset_timeout
        
        if should_reset:
            self._log_event("reset_check", {
                "should_reset": True,
                "elapsed_time": elapsed,
                "reset_timeout": self.reset_timeout
            })
        
        return should_reset

    def _get_remaining_timeout(self) -> float:
        """
        Calculate remaining time until circuit can be reset
        """
        if self._last_failure_time is None:
            return 0.0
            
        elapsed = (datetime.now() - self._last_failure_time).total_seconds()
        remaining = max(0.0, self.reset_timeout - elapsed)
        
        return remaining

    async def __aenter__(self) -> 'CircuitBreaker':
        await self._check_state()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[Exception]],
        exc_val: Optional[Exception],
        exc_tb: Any
    ) -> None:
        if exc_type is None:
            await self._on_success()
        elif not isinstance(exc_val, self.excluded_exceptions):
            await self._on_failure(exc_val)  