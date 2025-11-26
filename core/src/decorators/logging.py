from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar
from functools import wraps
import logging
import time
import inspect

T = TypeVar('T')

@dataclass
class LogConfig:
    """Configuration for logging behavior"""
    level: int = logging.INFO
    include_args: bool = True
    include_result: bool = True
    include_time: bool = True
    include_metrics: bool = True

@dataclass
class LogMetrics:
    """Metrics for function execution monitoring"""
    call_count: int = 0
    error_count: int = 0
    total_time: float = 0.0
    average_time: float = 0.0
    last_error: Optional[str] = None
    error_types: Dict[str, int] = field(default_factory=dict)

def Logging(
    logger: Optional[logging.Logger] = None,
    level: int = logging.INFO,
    include_args: bool = True,
    include_result: bool = True,
    include_time: bool = True,  
    include_metrics: bool = True
):
    """Advanced logging decorator"""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        metrics = LogMetrics()
        log = logger or logging.getLogger(func.__module__)
        config = LogConfig(level, include_args, include_result, include_time, include_metrics)
        
        def log_entry(args: tuple, kwargs: dict) -> None:
            if config.include_args:
                log.log(config.level, f"Calling {func.__name__} with args={args}, kwargs={kwargs}")
            else:
                log.log(config.level, f"Calling {func.__name__}")
        
        def log_exit(result: Any, duration: float) -> None:
            message_parts = [f"Finished {func.__name__}"]
            
            if config.include_time:
                message_parts.append(f"in {duration:.3f}s")
            
            if config.include_result:
                message_parts.append(f"with result={result}")
            
            if config.include_metrics:
                message_parts.append(f"metrics={metrics.__dict__}")
            
            log.log(config.level, " ".join(message_parts))
        
        def log_error(error: Exception, duration: float) -> None:
            error_type = type(error).__name__
            metrics.error_types[error_type] = metrics.error_types.get(error_type, 0) + 1
            metrics.last_error = str(error)
            
            log.exception(
                f"Error in {func.__name__} after {duration:.3f}s: {str(error)}"
                f" (metrics={metrics.__dict__})"
            )
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs) -> T:
            metrics.call_count += 1
            start_time = time.time()
            
            try:
                log_entry(args, kwargs)
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                
                metrics.total_time += duration
                metrics.average_time = metrics.total_time / metrics.call_count
                
                log_exit(result, duration)
                return result
                
            except Exception as e:
                metrics.error_count += 1
                duration = time.time() - start_time
                log_error(e, duration)
                raise
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs) -> T:
            metrics.call_count += 1
            start_time = time.time()
            
            try:
                log_entry(args, kwargs)
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                
                metrics.total_time += duration
                metrics.average_time = metrics.total_time / metrics.call_count
                
                log_exit(result, duration)
                return result
                
            except Exception as e:
                metrics.error_count += 1
                duration = time.time() - start_time
                log_error(e, duration)
                raise
        
        return async_wrapper if inspect.iscoroutinefunction(func) else sync_wrapper
    
    return decorator