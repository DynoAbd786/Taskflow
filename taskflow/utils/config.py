"""
Configuration management for TaskFlow system.
Handles environment variables, configuration files, and settings validation.
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import Field, validator
from pydantic_settings import BaseSettings


class DatabaseConfig(BaseSettings):
    """Database configuration settings."""

    host: str = Field(default="localhost", env="DB_HOST")
    port: int = Field(default=5432, env="DB_PORT")
    name: str = Field(default="taskflow", env="DB_NAME")
    user: str = Field(default="taskflow", env="DB_USER")
    password: str = Field(default="taskflow", env="DB_PASSWORD")
    pool_size: int = Field(default=10, env="DB_POOL_SIZE")
    max_overflow: int = Field(default=20, env="DB_MAX_OVERFLOW")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "allow"

    @property
    def url(self) -> str:
        """Get database URL."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

    @property
    def async_url(self) -> str:
        """Get async database URL."""
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


class RedisConfig(BaseSettings):
    """Redis configuration settings."""

    host: str = Field(default="localhost", env="REDIS_HOST")
    port: int = Field(default=6379, env="REDIS_PORT")
    db: int = Field(default=0, env="REDIS_DB")
    password: Optional[str] = Field(default=None, env="REDIS_PASSWORD")
    max_connections: int = Field(default=20, env="REDIS_MAX_CONNECTIONS")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "allow"

    @property
    def url(self) -> str:
        """Get Redis URL."""
        auth = f":{self.password}@" if self.password else ""
        return f"redis://{auth}{self.host}:{self.port}/{self.db}"


class WorkerConfig(BaseSettings):
    """Worker configuration settings."""

    concurrency: int = Field(default=4, env="WORKER_CONCURRENCY")
    prefetch_multiplier: int = Field(default=1, env="WORKER_PREFETCH_MULTIPLIER")
    max_tasks_per_child: int = Field(default=1000, env="WORKER_MAX_TASKS_PER_CHILD")
    task_timeout: int = Field(default=300, env="WORKER_TASK_TIMEOUT")
    heartbeat_interval: int = Field(default=30, env="WORKER_HEARTBEAT_INTERVAL")
    max_retries: int = Field(default=100, env="WORKER_MAX_RETRIES")
    retry_delay: int = Field(default=2, env="WORKER_RETRY_DELAY")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "allow"


class APIConfig(BaseSettings):
    """API server configuration settings."""

    host: str = Field(default="0.0.0.0", env="API_HOST")
    port: int = Field(default=8000, env="API_PORT")
    workers: int = Field(default=1, env="API_WORKERS")
    reload: bool = Field(default=False, env="API_RELOAD")
    access_log: bool = Field(default=True, env="API_ACCESS_LOG")
    cors_origins: List[str] = Field(default=["*"], env="API_CORS_ORIGINS")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "allow"

    @validator("cors_origins", pre=True)
    def parse_cors_origins(cls, v):
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",")]
        return v


class SecurityConfig(BaseSettings):
    """Security configuration settings."""

    secret_key: str = Field(env="SECRET_KEY")
    algorithm: str = Field(default="HS256", env="JWT_ALGORITHM")
    access_token_expire_minutes: int = Field(
        default=30, env="ACCESS_TOKEN_EXPIRE_MINUTES"
    )
    api_key_header: str = Field(default="X-API-Key", env="API_KEY_HEADER")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "allow"


class LoggingConfig(BaseSettings):
    """Logging configuration settings."""

    level: str = Field(default="INFO", env="LOG_LEVEL")
    format: str = Field(default="json", env="LOG_FORMAT")
    file: Optional[str] = Field(default=None, env="LOG_FILE")
    max_size: int = Field(default=100, env="LOG_MAX_SIZE_MB")
    backup_count: int = Field(default=5, env="LOG_BACKUP_COUNT")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "allow"

    @validator("level")
    def validate_level(cls, v):
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid_levels:
            raise ValueError(f"Invalid log level: {v}")
        return v.upper()


class MonitoringConfig(BaseSettings):
    """Monitoring and metrics configuration."""

    enabled: bool = Field(default=True, env="MONITORING_ENABLED")
    prometheus_port: int = Field(default=8001, env="PROMETHEUS_PORT")
    metrics_prefix: str = Field(default="taskflow", env="METRICS_PREFIX")
    collect_system_metrics: bool = Field(default=True, env="COLLECT_SYSTEM_METRICS")
    health_check_interval: int = Field(default=60, env="HEALTH_CHECK_INTERVAL")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "allow"


class TaskFlowConfig(BaseSettings):
    """Main TaskFlow configuration."""

    environment: str = Field(default="development", env="ENVIRONMENT")
    debug: bool = Field(default=False, env="DEBUG")
    timezone: str = Field(default="UTC", env="TIMEZONE")

    database: DatabaseConfig = DatabaseConfig()
    redis: RedisConfig = RedisConfig()
    worker: WorkerConfig = WorkerConfig()
    api: APIConfig = APIConfig()
    security: SecurityConfig = SecurityConfig()
    logging: LoggingConfig = LoggingConfig()
    monitoring: MonitoringConfig = MonitoringConfig()

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "allow"

    @classmethod
    def load_from_file(cls, config_path: Optional[Path] = None) -> "TaskFlowConfig":
        """Load configuration from file."""
        if config_path and config_path.exists():
            return cls(_env_file=config_path)
        return cls()

    def validate_config(self) -> List[str]:
        """Validate configuration and return list of errors."""
        errors = []

        if not self.security.secret_key:
            errors.append("SECRET_KEY is required")

        if self.worker.concurrency < 1:
            errors.append("Worker concurrency must be at least 1")

        if self.api.port < 1 or self.api.port > 65535:
            errors.append("API port must be between 1 and 65535")

        return errors

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary."""
        return self.dict()


_config: Optional[TaskFlowConfig] = None


def get_config() -> TaskFlowConfig:
    """Get the global configuration instance."""
    global _config
    if _config is None:
        _config = TaskFlowConfig()
        errors = _config.validate_config()
        if errors:
            raise ValueError(f"Configuration validation failed: {'; '.join(errors)}")
    return _config


def set_config(config: TaskFlowConfig) -> None:
    """Set the global configuration instance."""
    global _config
    _config = config


def reload_config() -> TaskFlowConfig:
    """Reload configuration from environment/files."""
    global _config
    _config = None
    return get_config()
