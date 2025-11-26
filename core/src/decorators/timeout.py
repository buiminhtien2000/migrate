from dataclasses import dataclass
from typing import Any, Callable, TypeVar
from functools import wraps
import asyncio
import inspect

T = TypeVar('T')

class TimeoutError(Exception):
    """Custom exception for timeout errors"""
    pass

@dataclass
class TimeoutConfig:
    """Configuration for timeout behavior"""
    seconds: float
    cancel_task: bool = True
    cleanup_timeout: float = 1.0

def Timeout(
    seconds: float,
    cancel_task: bool = True,
    cleanup_timeout: float = 1.0
):
    """Advanced timeout decorator"""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        config = TimeoutConfig(seconds, cancel_task, cleanup_timeout)
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs) -> T:
            try:
                return await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=config.seconds
                )
            except asyncio.TimeoutError:
                if config.cancel_task:
                    # Add cleanup handling here if needed
                    pass
                raise TimeoutError(
                    f"Function {func.__name__} timed out after {config.seconds} seconds"
                )
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs) -> T:
            async def run_with_timeout():
                loop = asyncio.get_event_loop()
                return await asyncio.wait_for(
                    loop.run_in_executor(None, func, *args, **kwargs),
                    timeout=config.seconds
                )
            
            try:
                return asyncio.run(run_with_timeout())
            except asyncio.TimeoutError:
                if config.cancel_task:
                    # Add cleanup handling here if needed
                    pass
                raise TimeoutError(
                    f"Function {func.__name__} timed out after {config.seconds} seconds"
                )
        
        return async_wrapper if inspect.iscoroutinefunction(func) else sync_wrapper
    
    return decorator