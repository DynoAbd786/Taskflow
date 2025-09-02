"""
Error recovery and resilience mechanisms for TaskFlow.
Handles automatic recovery, circuit breakers, and system healing.
"""

import asyncio
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Union

from taskflow.utils.exceptions import (
    TaskFlowError,
    get_error_severity,
    is_permanent_error,
    is_retryable_error,
)
from taskflow.utils.logging import get_logger

T = TypeVar("T")
logger = get_logger(__name__)


class RetryStrategy(str, Enum):
    """Retry strategy types."""

    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"
    RANDOM = "random"


class CircuitState(str, Enum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class RetryConfig:
    """Retry configuration."""

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_multiplier: float = 2.0
    jitter: bool = True
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    retryable_exceptions: Tuple[type, ...] = (Exception,)
    permanent_exceptions: Tuple[type, ...] = ()


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""

    failure_threshold: int = 5
    timeout: float = 60.0
    expected_exception: type = Exception
    recovery_timeout: float = 30.0
    half_open_max_calls: int = 3


@dataclass
class RecoveryStats:
    """Recovery operation statistics."""

    total_attempts: int = 0
    successful_attempts: int = 0
    failed_attempts: int = 0
    last_attempt: Optional[datetime] = None
    last_success: Optional[datetime] = None
    last_failure: Optional[datetime] = None
    average_duration: float = 0.0


class RetryHandler:
    """Handles retry logic with different strategies."""

    def __init__(self, config: RetryConfig):
        self.config = config
        self.stats = RecoveryStats()

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt number."""
        if self.config.strategy == RetryStrategy.FIXED:
            delay = self.config.base_delay
        elif self.config.strategy == RetryStrategy.LINEAR:
            delay = self.config.base_delay * attempt
        elif self.config.strategy == RetryStrategy.EXPONENTIAL:
            delay = self.config.base_delay * (
                self.config.backoff_multiplier ** (attempt - 1)
            )
        elif self.config.strategy == RetryStrategy.FIBONACCI:
            delay = self.config.base_delay * self._fibonacci(attempt)
        elif self.config.strategy == RetryStrategy.RANDOM:
            delay = self.config.base_delay * random.uniform(0.5, 2.0)
        else:
            delay = self.config.base_delay

        # Apply jitter
        if self.config.jitter:
            jitter_amount = delay * 0.1
            delay += random.uniform(-jitter_amount, jitter_amount)

        return min(delay, self.config.max_delay)

    def _fibonacci(self, n: int) -> int:
        """Calculate Fibonacci number."""
        if n <= 0:
            return 0
        elif n == 1:
            return 1
        else:
            a, b = 0, 1
            for _ in range(2, n + 1):
                a, b = b, a + b
            return b

    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """Check if operation should be retried."""
        if attempt >= self.config.max_attempts:
            return False

        # Check permanent exceptions
        if any(isinstance(exception, exc) for exc in self.config.permanent_exceptions):
            return False

        # Check retryable exceptions
        if not any(
            isinstance(exception, exc) for exc in self.config.retryable_exceptions
        ):
            return False

        # Use TaskFlow error classification
        if isinstance(exception, TaskFlowError):
            return is_retryable_error(exception) and not is_permanent_error(exception)

        return True

    async def execute_with_retry(
        self, operation: Callable[..., T], *args, **kwargs
    ) -> T:
        """Execute operation with retry logic."""
        attempt = 0
        last_exception = None
        start_time = time.time()

        while attempt < self.config.max_attempts:
            attempt += 1
            self.stats.total_attempts += 1
            self.stats.last_attempt = datetime.now(timezone.utc)

            try:
                if asyncio.iscoroutinefunction(operation):
                    result = await operation(*args, **kwargs)
                else:
                    result = operation(*args, **kwargs)

                # Success
                self.stats.successful_attempts += 1
                self.stats.last_success = datetime.now(timezone.utc)

                duration = time.time() - start_time
                self.stats.average_duration = (
                    self.stats.average_duration * (self.stats.successful_attempts - 1)
                    + duration
                ) / self.stats.successful_attempts

                logger.info(
                    "Operation succeeded",
                    attempt=attempt,
                    duration=duration,
                    component="retry_handler",
                )

                return result

            except Exception as e:
                last_exception = e
                self.stats.failed_attempts += 1
                self.stats.last_failure = datetime.now(timezone.utc)

                logger.warning(
                    "Operation failed",
                    attempt=attempt,
                    error=str(e),
                    component="retry_handler",
                )

                if not self.should_retry(e, attempt):
                    break

                if attempt < self.config.max_attempts:
                    delay = self.calculate_delay(attempt)
                    logger.info(
                        "Retrying operation",
                        attempt=attempt,
                        delay=delay,
                        component="retry_handler",
                    )
                    await asyncio.sleep(delay)

        # All retries exhausted
        logger.error(
            "Operation failed after all retries",
            attempts=attempt,
            last_error=str(last_exception),
            component="retry_handler",
        )

        raise last_exception


class CircuitBreaker:
    """Circuit breaker for preventing cascading failures."""

    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.half_open_calls = 0
        self.stats = RecoveryStats()

    def _should_attempt_reset(self) -> bool:
        """Check if circuit should attempt to reset."""
        if self.state != CircuitState.OPEN:
            return False

        if not self.last_failure_time:
            return True

        time_since_failure = datetime.now(timezone.utc) - self.last_failure_time
        return time_since_failure.total_seconds() >= self.config.timeout

    def _on_success(self) -> None:
        """Handle successful operation."""
        self.failure_count = 0
        self.half_open_calls = 0
        self.state = CircuitState.CLOSED
        self.stats.successful_attempts += 1
        self.stats.last_success = datetime.now(timezone.utc)

        logger.info("Circuit breaker reset to closed", component="circuit_breaker")

    def _on_failure(self, exception: Exception) -> None:
        """Handle failed operation."""
        self.failure_count += 1
        self.last_failure_time = datetime.now(timezone.utc)
        self.stats.failed_attempts += 1
        self.stats.last_failure = datetime.now(timezone.utc)

        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
            logger.warning(
                "Circuit breaker opened from half-open",
                failure_count=self.failure_count,
                error=str(exception),
                component="circuit_breaker",
            )
        elif (
            self.state == CircuitState.CLOSED
            and self.failure_count >= self.config.failure_threshold
        ):
            self.state = CircuitState.OPEN
            logger.warning(
                "Circuit breaker opened",
                failure_count=self.failure_count,
                threshold=self.config.failure_threshold,
                error=str(exception),
                component="circuit_breaker",
            )

    async def call(self, operation: Callable[..., T], *args, **kwargs) -> T:
        """Execute operation through circuit breaker."""
        self.stats.total_attempts += 1
        self.stats.last_attempt = datetime.now(timezone.utc)

        # Check if circuit should attempt reset
        if self._should_attempt_reset():
            self.state = CircuitState.HALF_OPEN
            self.half_open_calls = 0
            logger.info(
                "Circuit breaker transitioning to half-open",
                component="circuit_breaker",
            )

        # Block calls if circuit is open
        if self.state == CircuitState.OPEN:
            logger.warning(
                "Circuit breaker is open, rejecting call", component="circuit_breaker"
            )
            raise TaskFlowError(
                "Circuit breaker is open",
                error_code="CIRCUIT_BREAKER_OPEN",
                details={
                    "failure_count": self.failure_count,
                    "last_failure": self.last_failure_time.isoformat()
                    if self.last_failure_time
                    else None,
                },
            )

        # Limit calls in half-open state
        if self.state == CircuitState.HALF_OPEN:
            if self.half_open_calls >= self.config.half_open_max_calls:
                logger.warning(
                    "Circuit breaker half-open call limit exceeded",
                    component="circuit_breaker",
                )
                raise TaskFlowError(
                    "Circuit breaker half-open call limit exceeded",
                    error_code="CIRCUIT_BREAKER_HALF_OPEN_LIMIT",
                )
            self.half_open_calls += 1

        try:
            if asyncio.iscoroutinefunction(operation):
                result = await operation(*args, **kwargs)
            else:
                result = operation(*args, **kwargs)

            self._on_success()
            return result

        except Exception as e:
            if isinstance(e, self.config.expected_exception):
                self._on_failure(e)
            raise


class HealthChecker:
    """Health checking and automatic recovery."""

    def __init__(self, check_interval: float = 60.0):
        self.check_interval = check_interval
        self.health_checks: Dict[str, Callable] = {}
        self.last_checks: Dict[str, datetime] = {}
        self.health_status: Dict[str, bool] = {}
        self.recovery_actions: Dict[str, Callable] = {}
        self._running = False
        self._shutdown_event = asyncio.Event()

    def register_health_check(
        self, name: str, check_func: Callable, recovery_func: Optional[Callable] = None
    ) -> None:
        """Register a health check."""
        self.health_checks[name] = check_func
        self.health_status[name] = True

        if recovery_func:
            self.recovery_actions[name] = recovery_func

        logger.info(
            "Health check registered",
            check_name=name,
            has_recovery=recovery_func is not None,
            component="health_checker",
        )

    async def perform_check(self, name: str) -> bool:
        """Perform a single health check."""
        check_func = self.health_checks.get(name)
        if not check_func:
            return False

        try:
            if asyncio.iscoroutinefunction(check_func):
                result = await check_func()
            else:
                result = check_func()

            self.health_status[name] = bool(result)
            self.last_checks[name] = datetime.now(timezone.utc)

            if result:
                logger.debug(
                    "Health check passed", check_name=name, component="health_checker"
                )
            else:
                logger.warning(
                    "Health check failed", check_name=name, component="health_checker"
                )

            return bool(result)

        except Exception as e:
            self.health_status[name] = False
            self.last_checks[name] = datetime.now(timezone.utc)

            logger.error(
                "Health check error",
                check_name=name,
                error=str(e),
                component="health_checker",
            )

            return False

    async def attempt_recovery(self, name: str) -> bool:
        """Attempt to recover from health check failure."""
        recovery_func = self.recovery_actions.get(name)
        if not recovery_func:
            return False

        try:
            logger.info(
                "Attempting recovery", check_name=name, component="health_checker"
            )

            if asyncio.iscoroutinefunction(recovery_func):
                await recovery_func()
            else:
                recovery_func()

            # Re-run health check after recovery
            success = await self.perform_check(name)

            if success:
                logger.info(
                    "Recovery successful", check_name=name, component="health_checker"
                )
            else:
                logger.warning(
                    "Recovery failed", check_name=name, component="health_checker"
                )

            return success

        except Exception as e:
            logger.error(
                "Recovery error",
                check_name=name,
                error=str(e),
                component="health_checker",
            )
            return False

    async def start(self) -> None:
        """Start health checking loop."""
        self._running = True

        logger.info(
            "Health checker started",
            interval=self.check_interval,
            checks=list(self.health_checks.keys()),
            component="health_checker",
        )

        while self._running and not self._shutdown_event.is_set():
            try:
                for name in self.health_checks:
                    healthy = await self.perform_check(name)

                    if not healthy and name in self.recovery_actions:
                        await self.attempt_recovery(name)

                await asyncio.sleep(self.check_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "Health checker loop error",
                    error=str(e),
                    component="health_checker",
                )
                await asyncio.sleep(self.check_interval)

    async def stop(self) -> None:
        """Stop health checking."""
        self._running = False
        self._shutdown_event.set()

        logger.info("Health checker stopped", component="health_checker")

    def get_status(self) -> Dict[str, Any]:
        """Get overall health status."""
        return {
            "checks": dict(self.health_status),
            "last_checks": {
                name: timestamp.isoformat()
                for name, timestamp in self.last_checks.items()
            },
            "overall_healthy": all(self.health_status.values()),
            "total_checks": len(self.health_checks),
            "healthy_checks": sum(self.health_status.values()),
        }


def with_retry(config: Optional[RetryConfig] = None):
    """Decorator for adding retry behavior to functions."""
    retry_config = config or RetryConfig()

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        retry_handler = RetryHandler(retry_config)

        if asyncio.iscoroutinefunction(func):

            async def async_wrapper(*args, **kwargs):
                return await retry_handler.execute_with_retry(func, *args, **kwargs)

            return async_wrapper
        else:

            def sync_wrapper(*args, **kwargs):
                return asyncio.run(
                    retry_handler.execute_with_retry(func, *args, **kwargs)
                )

            return sync_wrapper

    return decorator


def with_circuit_breaker(config: Optional[CircuitBreakerConfig] = None):
    """Decorator for adding circuit breaker behavior to functions."""
    breaker_config = config or CircuitBreakerConfig()
    circuit_breaker = CircuitBreaker(breaker_config)

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        if asyncio.iscoroutinefunction(func):

            async def async_wrapper(*args, **kwargs):
                return await circuit_breaker.call(func, *args, **kwargs)

            return async_wrapper
        else:

            def sync_wrapper(*args, **kwargs):
                return asyncio.run(circuit_breaker.call(func, *args, **kwargs))

            return sync_wrapper

    return decorator
