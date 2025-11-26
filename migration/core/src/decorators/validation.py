from dataclasses import dataclass
from typing import Any, Callable, TypeVar
from functools import wraps
import inspect

T = TypeVar('T')

@dataclass
class ValidationRule:
    """Validation rule configuration"""
    field: str 
    validator: Callable[[Any], bool]
    error_message: str

class ValidationError(Exception):
    """Custom exception for validation errors"""
    def __init__(self, errors: dict[str, str]) -> None:
        self.errors = errors
        super().__init__(f"Validation failed: {errors}")

def Validation(*rules: ValidationRule):
    """
    Advanced validation decorator that applies multiple validation rules
    to function arguments.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        sig = inspect.signature(func)
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs) -> T:
            bound_args = sig.bind(*args, **kwargs)
            errors = {}
            
            for rule in rules:
                if rule.field in bound_args.arguments:
                    value = bound_args.arguments[rule.field]
                    try:
                        if not rule.validator(value):
                            errors[rule.field] = rule.error_message
                    except Exception as e:
                        errors[rule.field] = f"Validation error: {str(e)}"
            
            if errors:
                raise ValidationError(errors)
            
            return await func(*args, **kwargs) if inspect.iscoroutinefunction(func) else func(*args, **kwargs)

        @wraps(func)
        def sync_wrapper(*args, **kwargs) -> T:
            bound_args = sig.bind(*args, **kwargs)
            errors = {}
            
            for rule in rules:
                if rule.field in bound_args.arguments:
                    value = bound_args.arguments[rule.field]
                    try:
                        if not rule.validator(value):
                            errors[rule.field] = rule.error_message
                    except Exception as e:
                        errors[rule.field] = f"Validation error: {str(e)}"
            
            if errors:
                raise ValidationError(errors)
            
            return func(*args, **kwargs)
        
        return async_wrapper if inspect.iscoroutinefunction(func) else sync_wrapper
    
    return decorator