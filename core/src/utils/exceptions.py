# utils/exceptions.py

from typing import Optional, Dict, Any
from http import HTTPStatus
from .logger import logger

class AppException(Exception):
    """Base exception class for all application exceptions"""
    
    status_code = HTTPStatus.INTERNAL_SERVER_ERROR
    error_code = "INTERNAL_ERROR"
    
    def __init__(
        self, 
        message: str, 
        details: Optional[str] = None,
        status_code: Optional[int] = None,
        error_code: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        self.message = message
        self.details = details
        self.context = context or {}
        
        if status_code:
            self.status_code = status_code
        if error_code:
            self.error_code = error_code
            
        # Log the exception
        self._log_exception()
        super().__init__(self.message)
    
    def _log_exception(self):
        """Log exception with context"""
        log_message = f"{self.__class__.__name__}: {self.message}"
        if self.details:
            log_message += f" | Details: {self.details}"
        
        logger.error(
            log_message,
            extra={
                'error_code': self.error_code,
                'status_code': self.status_code,
                'context': self.context
            }
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary format"""
        return {
            'error': self.__class__.__name__,
            'error_code': self.error_code,
            'message': self.message,
            'details': self.details,
            'status_code': self.status_code,
            'context': self.context
        }

# API Exceptions
class APIError(AppException):
    """Base class for API related errors"""
    status_code = HTTPStatus.BAD_REQUEST
    error_code = "API_ERROR"

class RateLimitException(APIError):
    """Exception raised when API rate limit is exceeded."""
    pass
  
class BitrixAPIError(APIError):
    """Bitrix API specific errors"""
    error_code = "BITRIX_API_ERROR"

class ExternalAPIError(APIError):
    """External API communication errors"""
    error_code = "EXTERNAL_API_ERROR"

class APITimeoutError(APIError):
    """API timeout errors"""
    status_code = HTTPStatus.GATEWAY_TIMEOUT
    error_code = "API_TIMEOUT"

# Authentication Exceptions
class AuthError(AppException):
    """Base class for authentication errors"""
    status_code = HTTPStatus.UNAUTHORIZED
    error_code = "AUTH_ERROR"

class TokenError(AuthError):
    """Token related errors"""
    error_code = "TOKEN_ERROR"

class PermissionError(AuthError):
    """Permission related errors"""
    status_code = HTTPStatus.FORBIDDEN
    error_code = "PERMISSION_ERROR"

# Validation Exceptions
class ValidationError(AppException):
    """Data validation errors"""
    status_code = HTTPStatus.BAD_REQUEST
    error_code = "VALIDATION_ERROR"

class SchemaError(ValidationError):
    """Schema validation errors"""
    error_code = "SCHEMA_ERROR"

# Database Exceptions
class DatabaseError(AppException):
    """Database related errors"""
    status_code = HTTPStatus.INTERNAL_SERVER_ERROR
    error_code = "DB_ERROR"

class ConnectionError(DatabaseError):
    """Database connection errors"""
    error_code = "DB_CONNECTION_ERROR"

class QueryError(DatabaseError):
    """Database query errors"""
    error_code = "DB_QUERY_ERROR"

# Business Logic Exceptions
class BusinessError(AppException):
    """Business logic related errors"""
    status_code = HTTPStatus.UNPROCESSABLE_ENTITY
    error_code = "BUSINESS_ERROR"

class ResourceNotFound(BusinessError):
    """Resource not found errors"""
    status_code = HTTPStatus.NOT_FOUND
    error_code = "RESOURCE_NOT_FOUND"

class DuplicateError(BusinessError):
    """Duplicate resource errors"""
    status_code = HTTPStatus.CONFLICT
    error_code = "DUPLICATE_ERROR"

# Configuration Exceptions
class ConfigError(AppException):
    """Configuration related errors"""
    status_code = HTTPStatus.INTERNAL_SERVER_ERROR
    error_code = "CONFIG_ERROR"

class EnvironmentError(ConfigError):
    """Environment configuration errors"""
    error_code = "ENV_ERROR"

# Export all exceptions
__all__ = [
    'AppException',
    'APIError',
    'BitrixAPIError',
    'ExternalAPIError',
    'APITimeoutError',
    'AuthError',
    'TokenError',
    'PermissionError',
    'ValidationError',
    'SchemaError',
    'DatabaseError',
    'ConnectionError',
    'QueryError',
    'BusinessError',
    'ResourceNotFound',
    'DuplicateError',
    'ConfigError',
    'EnvironmentError'
]