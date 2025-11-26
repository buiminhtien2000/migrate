# utils/logger.py

import os
import logging
import inspect
from typing import Optional, Any, Dict
from logging.handlers import RotatingFileHandler
from pathlib import Path
from functools import wraps
from config import config

class CustomLogger:
    """Enhanced logger with caller information and environment configuration support"""
    
    LOG_LEVELS = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    
    def __init__(
        self,
        log_file: Optional[str] = None,
        level: Optional[str] = None,
        format: str = '%(asctime)s - [%(levelname)s] - %(caller_info)s - %(message)s'
    ):
        self.log_level = level or config['LOG_LEVEL']
        self.log_file = log_file or config['LOG_FILE']
        
        # Ensure log directory exists
        log_dir = os.path.dirname(self.log_file)
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        
        # Setup logger
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(self.LOG_LEVELS.get(self.log_level.upper(), logging.INFO))
        
        # Clear existing handlers
        if self.logger.handlers:
            self.logger.handlers.clear()
        
        # Custom formatter with caller info
        formatter = logging.Formatter(format)
        
        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File Handler with rotation
        file_handler = RotatingFileHandler(
            self.log_file,
            maxBytes=1024 * 1024,  # 1MB
            backupCount=5
        )
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def _get_caller_info(self, stack_info: Optional[int] = None) -> str:
        """Get caller information including class name, function name and line number"""
        stack = inspect.stack()
        
        # Get the frame at the specified stack level or default to the caller's caller
        frame = stack[stack_info if stack_info is not None else 2]
        
        filename = os.path.basename(frame.filename)
        line_number = frame.lineno
        function_name = frame.function
        
        # Try to get class name if method is called from a class
        try:
            if 'self' in frame.frame.f_locals:
                class_name = frame.frame.f_locals['self'].__class__.__name__
                return f"{filename}:{class_name}.{function_name}:{line_number}"
        except:
            pass
            
        return f"{filename}:{function_name}:{line_number}"

    def _log(self, level: int, message: str, stack_info: Optional[int] = None, 
             exc_info: Optional[Exception] = None, extra: Optional[Dict[str, Any]] = None, 
             *args, **kwargs) -> None:
        """Internal logging method with caller information"""
        extra = extra or {}
        extra['caller_info'] = self._get_caller_info(stack_info)
        
        self.logger.log(
            level, 
            message, 
            exc_info=exc_info,
            extra=extra,
            *args, 
            **kwargs
        )

    def debug(self, message: str, extra: Optional[Dict[str, Any]] = None, *args, **kwargs):
        self._log(logging.DEBUG, message, extra=extra, stack_info=3, *args, **kwargs)
        
    def info(self, message: str, extra: Optional[Dict[str, Any]] = None, *args, **kwargs):
        self._log(logging.INFO, message, extra=extra, stack_info=3, *args, **kwargs)
        
    def warning(self, message: str, extra: Optional[Dict[str, Any]] = None, *args, **kwargs):
        self._log(logging.WARNING, message, extra=extra, stack_info=3, *args, **kwargs)
        
    def error(self, message: str, extra: Optional[Dict[str, Any]] = None, exc_info: Optional[Exception] = None, *args, **kwargs):
        self._log(logging.ERROR, message, extra=extra, stack_info=3, exc_info=exc_info, *args, **kwargs)
        
    def critical(self, message: str, extra: Optional[Dict[str, Any]] = None, *args, **kwargs):
        self._log(logging.CRITICAL, message, extra=extra, stack_info=3, *args, **kwargs)

    @staticmethod
    def log_execution(logger_instance=None):
        """Decorator to log function execution"""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                _logger = logger_instance or logger
                func_name = func.__name__
                class_name = args[0].__class__.__name__ if args and hasattr(args[0], '__class__') else None
                
                # Create full function name
                full_name = f"{class_name}.{func_name}" if class_name else func_name
                
                try:
                    _logger.debug(f"Executing {full_name}")
                    result = func(*args, **kwargs)
                    _logger.debug(f"Completed {full_name}")
                    return result
                except Exception as e:
                    _logger.error(f"Error in {full_name}: {str(e)}", exc_info=e)
                    raise
            return wrapper
        return decorator

# Create default logger instance
logger = CustomLogger()