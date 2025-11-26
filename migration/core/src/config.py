import os
import json
import logging
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict
from dotenv import load_dotenv

# Get the project root directory (parent of src)
PROJECT_ROOT = Path('/app')
ENV_PATH = PROJECT_ROOT / '.env'

# Load environment variables from the correct path
load_dotenv(ENV_PATH)

logger = logging.getLogger(__name__)

def parse_bool(value: Any, default: bool = False) -> bool:
    """Parse string to boolean with various formats."""
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    
    true_values = ('true', 't', 'yes', 'y', 'on', '1')
    false_values = ('false', 'f', 'no', 'n', 'off', '0')
    
    try:
        value_lower = str(value).lower().strip()
        if value_lower in true_values:
            return True
        if value_lower in false_values:
            return False
        return default
    except (ValueError, AttributeError):
        return default

@dataclass
class Config:
    """Configuration class for the Bitrix Migration application."""

    DOMAIN: str = str(os.getenv('DOMAIN', ''))
    CORS_ORIGINS: str = str(os.getenv('CORS_ORIGINS', '*'))
    API_TOKEN: str = str(os.getenv('API_TOKEN', '74310e2192452d0df9c311c786b80ef4'))

    # API Configuration
    API_TIMEOUT: int = int(os.getenv('API_TIMEOUT', '30'))
    API_RATE_LIMIT: float = float(os.getenv('API_RATE_LIMIT', '0.5'))
    MAX_CONCURRENT_REQUESTS: int = int(os.getenv('MAX_CONCURRENT_REQUESTS', '5'))

    # Debug and Development Settings
    DEBUG_MODE: bool = parse_bool(os.getenv('DEBUG_MODE'), False)
    DEBUG_SAMPLE_SIZE: int = int(os.getenv('DEBUG_SAMPLE_SIZE', '500'))

    # Logging Configuration
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE: str = str(PROJECT_ROOT / os.getenv('LOG_FILE', 'logs/migration.log'))
    LOG_MAX_SIZE: str = os.getenv('LOG_MAX_SIZE', '3m')
    LOG_MAX_FILE: int = int(os.getenv('LOG_MAX_FILE', '3'))

    # Migration Process Configuration
    BATCH_SIZE: int = int(os.getenv('BATCH_SIZE', '50'))
    MAX_RETRIES: int = int(os.getenv('MAX_RETRIES', '3'))
    RETRY_DELAY: int = int(os.getenv('RETRY_DELAY', '5'))
    TIMEOUT: int = int(os.getenv('TIMEOUT', '30'))
    CONTINUE_ON_ERROR: bool = parse_bool(os.getenv('CONTINUE_ON_ERROR'), False)

    # Directory Configuration
    DATA_DIR: str = str(PROJECT_ROOT / '.data') 
    PROGRESS_PATH: str = os.path.join(DATA_DIR, os.getenv('PROGRESS_PATH', '.progress'))
    BACKUP_DIR: str = os.path.join(DATA_DIR, os.getenv('BACKUP_DIR', '.backups'))
    AUTH_DIR: str = os.path.join(DATA_DIR, os.getenv('AUTH_DIR', '.auths'))
    DOWNLOAD_DIR: str = os.path.join(DATA_DIR, os.getenv('DOWNLOAD_DIR', '.downloads'))
    TOKENS_FILE: str = 'tokens.json'

    # Circuit Breaker Configuration
    CIRCUIT_BREAKER_THRESHOLD: int = int(os.getenv('CIRCUIT_BREAKER_THRESHOLD', '5'))
    CIRCUIT_BREAKER_RECOVERY_TIME: int = int(os.getenv('CIRCUIT_BREAKER_RECOVERY_TIME', '60'))

    # Database Configuration
    POSTGRES_USER: str = os.getenv('DB_USERNAME', 'migration')
    POSTGRES_PASSWORD: str = os.getenv('DB_PASSWORD')
    POSTGRES_DB: str = os.getenv('DB_DATABASE', 'migration_db')
    POSTGRES_HOST: str = os.getenv('POSTGRES_HOST', 'postgres')
    POSTGRES_PORT: int = int(os.getenv('POSTGRES_PORT', '5432'))

    # Resume Configuration
    ENABLE_RESUME_TIMEOUT: bool = parse_bool(os.getenv('ENABLE_RESUME_TIMEOUT'), False)
    RESUME_TIMEOUT_HOURS: int = int(os.getenv('RESUME_TIMEOUT_HOURS', '24'))
    MAX_BUFFER_SIZE: int = int(os.getenv('MAX_BUFFER_SIZE', '1000'))
    EXPONENTIAL_BACKOFF: bool = parse_bool(os.getenv('EXPONENTIAL_BACKOFF'), False)

    # Utils Settings
    NOTIFY_ERROR: bool = parse_bool(os.getenv('NOTIFY_ERROR'), False)
    CLEANUP_DAYS: int = int(os.getenv('CLEANUP_DAYS', '7'))
    PAGES_PER_BATCH: int = int(os.getenv('PAGES_PER_BATCH', '2'))
    PAGE_SIZE: int = int(os.getenv('PAGE_SIZE', '50'))

    @property 
    def NOTIFICATION_CHANNELS(self):
        channels = os.getenv('NOTIFICATION_CHANNELS', '')
        return [c.strip() for c in channels.split(',') if c.strip()]

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value safely with fallback to default.
        
        Args:
            key: Configuration key to retrieve
            default: Default value if key doesn't exist
            
        Returns:
            Configuration value or default if key doesn't exist
        """
        return self._config_dict.get(key, default)

    def __getattr__(self, name: str) -> Any:
        """Safe attribute access for dynamic fields."""
        return None
      
    def __post_init__(self):
        """Convert to dictionary after initialization"""
        self._config_dict = asdict(self)
        self._setup_database_url()

    def _setup_database_url(self):
        """Setup database URL from configuration"""
        self.DATABASE_URL = f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@" \
                          f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
    
    def _load_file_config(self) -> Dict[str, Any]:
        """Load authentication configuration from config.json"""
        try:
            config_path = os.path.join(PROJECT_ROOT, 'config.json')
            
            if not os.path.exists(config_path):
                logger.error(f"Config not found at: {config_path}")
                self.from_file = {}
                
            with open(config_path, 'r', encoding='utf-8') as f:
                self.from_file = json.load(f)
            
        except Exception as e:
            logger.error(f"Failed to load config.json: {str(e)}")
            self.from_file = {}

    def validate(self) -> bool:
        """Validate the configuration"""
        try:
            # Migration config validation
            assert self.BATCH_SIZE > 0, "Batch size must be positive"
            assert self.MAX_RETRIES >= 0, "Max retries must be non-negative"
            assert self.API_RATE_LIMIT > 0, "API rate limit must be positive"
            assert self.MAX_CONCURRENT_REQUESTS > 0, "Max concurrent requests must be positive"

            # Log level validation
            valid_log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
            assert self.LOG_LEVEL.upper() in valid_log_levels, \
                f"Invalid log level. Must be one of {valid_log_levels}"

            # Database validation
            required_db_fields = ['POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_DB', 'POSTGRES_HOST']
            missing_fields = [field for field in required_db_fields if not getattr(self, field)]
            if missing_fields:
                raise ValueError(f"Missing required database fields: {', '.join(missing_fields)}")

            # Directory validation
            directories = [
                self.BACKUP_DIR,
                self.PROGRESS_PATH,
                os.path.dirname(self.LOG_FILE)
            ]
            
            for directory in directories:
                os.makedirs(directory, exist_ok=True)

            logger.info("Configuration validation successful")
            return True

        except AssertionError as e:
            logger.error(f"Configuration validation failed: {str(e)}")
            raise ValueError(f"Configuration validation failed: {str(e)}")

    @classmethod
    def load(cls) -> 'Config':
        """Load and validate configuration"""
        config = cls()
        config.validate()
        config._load_file_config()
        return config

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return self._config_dict

    def __getitem__(self, key: str) -> Any:
        """Support dictionary-style access"""
        return self._config_dict[key]

    def __contains__(self, key: str) -> bool:
        """Support 'in' operator"""
        return key in self._config_dict

# Create global config instance
config = Config.load()