from dataclasses import dataclass
from typing import Optional, Any, Dict, Callable, Type, Tuple, Generic, TypeVar
from enum import Enum
import asyncio
import random
import time
import logging
from functools import wraps

T = TypeVar('T')

class RetryStrategy(Enum):
    """Available retry strategies"""
    CONSTANT = "constant"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"

@dataclass
class RetryConfig:
    """Configuration for retry behavior"""
    max_retries: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    multiplier: float = 2.0  # Used for linear and exponential backoff
    jitter: bool = True
    jitter_factor: float = 0.1  # Percentage of delay to use for jitter
    exponential_base: float = 2
    
    def __post_init__(self):
        if self.initial_delay <= 0:
            raise ValueError("Initial delay must be positive")
        if self.max_delay < self.initial_delay:
            raise ValueError("Max delay must be greater than initial delay")
        if self.multiplier <= 0:
            raise ValueError("Multiplier must be positive")

class RetryError(Exception):
    """Raised when all retry attempts fail"""
    def __init__(self, attempts: int, last_exception: Exception):
        self.attempts = attempts
        self.last_exception = last_exception
        super().__init__(
            f"Failed after {attempts} attempts. Last error: {str(last_exception)}"
        )

class Retry(Generic[T]):
    """
    Decorator class for retry logic with multiple strategies
    
    Features:
    - Multiple retry strategies (constant, linear, exponential, fibonacci)
    - Configurable jitter
    - Exception filtering
    - Retry event logging
    - Success/failure callbacks
    """
    
    def __init__(
        self,
        retry_config: Optional[RetryConfig] = None,
        retry_on_exceptions: Tuple[Type[Exception], ...] = (Exception,),
        exclude_exceptions: Tuple[Type[Exception], ...] = (),
        logger: Optional[logging.Logger] = None,
        on_retry: Optional[Callable[[int, Exception], None]] = None,
        on_success: Optional[Callable[[int], None]] = None,
        on_failure: Optional[Callable[[int, Exception], None]] = None
    ):
        self.config = retry_config or RetryConfig()
        self.retry_on_exceptions = retry_on_exceptions
        self.exclude_exceptions = exclude_exceptions
        self.logger = logger or logging.getLogger(__name__)
        self.on_retry = on_retry
        self.on_success = on_success
        self.on_failure = on_failure
        
        # Initialize fibonacci sequence cache for fibonacci backoff
        self._fib_cache: Dict[int, int] = {0: 0, 1: 1}

    def _log_retry(self, attempt: int, exception: Exception, delay: float) -> None:
        """Default retry logging handler"""
        error_dict = exception.to_dict() if hasattr(exception, 'to_dict') else str(exception)
        self.logger.warning(
            f"API error. Retry {attempt}/{self.config.max_retries} after {delay:.2f}s",
            extra={'error': error_dict}
        )

        # Call custom retry callback if provided
        if self.on_retry:
            self.on_retry(attempt, exception)

    def _log_failure(self, attempts: int, exception: Exception) -> None:
        """Default failure logging handler"""
        error_dict = exception.to_dict() if hasattr(exception, 'to_dict') else str(exception)
        
        if hasattr(exception, '__name__') and exception.__name__ == 'AuthError':
            self.logger.error("Authentication failed", extra={'error': error_dict})
        elif hasattr(exception, '__name__') and exception.__name__ == 'BusinessError':
            self.logger.error("Business logic error", extra={'error': error_dict})
        else:
            self.logger.error(
                f"API error after {attempts} retries",
                extra={'error': error_dict}
            )

        # Call custom failure callback if provided
        if self.on_failure:
            self.on_failure(attempts, exception)

    def _log_success(self, attempts: int) -> None:
        """Default success logging handler"""
        if attempts > 0:
            self.logger.info(f"Operation succeeded after {attempts} retries")

        # Call custom success callback if provided
        if self.on_success:
            self.on_success(attempts)

    def _fibonacci(self, n: int) -> int:
        """Calculate fibonacci number with caching"""
        if n in self._fib_cache:
            return self._fib_cache[n]
        self._fib_cache[n] = self._fibonacci(n - 1) + self._fibonacci(n - 2)
        return self._fib_cache[n]

    def _get_delay(self, attempt: int) -> float:
        """Calculate delay based on retry strategy"""
        base_delay = self.config.initial_delay
        
        if self.config.strategy == RetryStrategy.CONSTANT:
            delay = base_delay
        elif self.config.strategy == RetryStrategy.LINEAR:
            delay = base_delay + (attempt * self.config.multiplier)
        elif self.config.strategy == RetryStrategy.EXPONENTIAL:
            delay = base_delay * (self.config.multiplier ** attempt)
        elif self.config.strategy == RetryStrategy.FIBONACCI:
            delay = base_delay * self._fibonacci(attempt + 1)
        else:
            delay = base_delay

        # Apply jitter if configured
        if self.config.jitter:
            jitter_range = delay * self.config.jitter_factor
            delay += random.uniform(-jitter_range, jitter_range)

        return min(delay, self.config.max_delay)

    def _should_retry(self, exception: Exception) -> bool:
        """Determine if retry should be attempted based on exception"""
        return (
            isinstance(exception, self.retry_on_exceptions) and
            not isinstance(exception, self.exclude_exceptions)
        )

    def __call__(self, func: Callable[..., T]) -> Callable[..., T]:
        """Decorator for synchronous functions"""
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            attempts = 0
            last_exception = None

            while attempts <= self.config.max_retries:
                try:
                    result = func(*args, **kwargs)
                    self._log_success(attempts)
                    return result
                except Exception as e:
                    last_exception = e
                    attempts += 1

                    if not self._should_retry(e) or attempts > self.config.max_retries:
                        self._log_failure(attempts, e)
                        raise RetryError(attempts, e) from e

                    delay = self._get_delay(attempts)
                    self._log_retry(attempts, e, delay)
                    time.sleep(delay)

            raise RetryError(attempts, last_exception)  # type: ignore

        return wrapper

    def async_retry(self, func: Callable[..., T]) -> Callable[..., T]:
        """Decorator for asynchronous functions"""
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            attempts = 0
            last_exception = None

            while attempts <= self.config.max_retries:
                try:
                    result = await func(*args, **kwargs)
                    self._log_success(attempts)
                    return result
                except Exception as e:
                    last_exception = e
                    attempts += 1

                    if not self._should_retry(e) or attempts > self.config.max_retries:
                        self._log_failure(attempts, e)
                        raise RetryError(attempts, e) from e

                    delay = self._get_delay(attempts)
                    self._log_retry(attempts, e, delay)
                    await asyncio.sleep(delay)

            raise RetryError(attempts, last_exception)  # type: ignore

        return wrapper