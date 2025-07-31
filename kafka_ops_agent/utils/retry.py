"""Retry utilities with exponential backoff and circuit breaker patterns."""

import asyncio
import logging
import time
import random
from typing import Callable, Any, Optional, Type, Union, List
from functools import wraps
from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timedelta

from kafka_ops_agent.exceptions import KafkaOpsError, ErrorCode

logger = logging.getLogger(__name__)


class CircuitState(str, Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Circuit is open, failing fast
    HALF_OPEN = "half_open"  # Testing if service is back


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    base_delay: float = 1.0  # Base delay in seconds
    max_delay: float = 60.0  # Maximum delay in seconds
    exponential_base: float = 2.0  # Exponential backoff multiplier
    jitter: bool = True  # Add random jitter to delays
    backoff_strategy: str = "exponential"  # exponential, linear, fixed


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""
    failure_threshold: int = 5  # Number of failures to open circuit
    recovery_timeout: float = 60.0  # Seconds to wait before trying half-open
    success_threshold: int = 3  # Successful calls needed to close circuit
    timeout: float = 30.0  # Operation timeout in seconds


class CircuitBreaker:
    """Circuit breaker implementation for external service calls."""
    
    def __init__(self, name: str, config: CircuitBreakerConfig):
        """Initialize circuit breaker.
        
        Args:
            name: Name of the circuit breaker for logging
            config: Circuit breaker configuration
        """
        self.name = name
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.last_success_time: Optional[datetime] = None
    
    def can_execute(self) -> bool:
        """Check if operation can be executed."""
        now = datetime.utcnow()
        
        if self.state == CircuitState.CLOSED:
            return True
        
        elif self.state == CircuitState.OPEN:
            if (self.last_failure_time and 
                now - self.last_failure_time >= timedelta(seconds=self.config.recovery_timeout)):
                self.state = CircuitState.HALF_OPEN
                self.success_count = 0
                logger.info(f"Circuit breaker '{self.name}' transitioning to HALF_OPEN")
                return True
            return False
        
        elif self.state == CircuitState.HALF_OPEN:
            return True
        
        return False
    
    def record_success(self):
        """Record a successful operation."""
        self.last_success_time = datetime.utcnow()
        
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                logger.info(f"Circuit breaker '{self.name}' closed after successful recovery")
        
        elif self.state == CircuitState.CLOSED:
            self.failure_count = 0  # Reset failure count on success
    
    def record_failure(self):
        """Record a failed operation."""
        self.last_failure_time = datetime.utcnow()
        self.failure_count += 1
        
        if self.state == CircuitState.CLOSED:
            if self.failure_count >= self.config.failure_threshold:
                self.state = CircuitState.OPEN
                logger.warning(f"Circuit breaker '{self.name}' opened after {self.failure_count} failures")
        
        elif self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
            logger.warning(f"Circuit breaker '{self.name}' reopened after failure during recovery")
    
    def get_stats(self) -> dict:
        """Get circuit breaker statistics."""
        return {
            'name': self.name,
            'state': self.state.value,
            'failure_count': self.failure_count,
            'success_count': self.success_count,
            'last_failure_time': self.last_failure_time.isoformat() if self.last_failure_time else None,
            'last_success_time': self.last_success_time.isoformat() if self.last_success_time else None
        }


class RetryManager:
    """Manages retry logic with exponential backoff."""
    
    def __init__(self, config: RetryConfig):
        """Initialize retry manager.
        
        Args:
            config: Retry configuration
        """
        self.config = config
    
    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for the given attempt number."""
        if self.config.backoff_strategy == "exponential":
            delay = self.config.base_delay * (self.config.exponential_base ** (attempt - 1))
        elif self.config.backoff_strategy == "linear":
            delay = self.config.base_delay * attempt
        else:  # fixed
            delay = self.config.base_delay
        
        # Apply maximum delay limit
        delay = min(delay, self.config.max_delay)
        
        # Add jitter if enabled
        if self.config.jitter:
            jitter_range = delay * 0.1  # 10% jitter
            delay += random.uniform(-jitter_range, jitter_range)
        
        return max(0, delay)
    
    def should_retry(self, attempt: int, exception: Exception) -> bool:
        """Determine if operation should be retried."""
        if attempt >= self.config.max_attempts:
            return False
        
        # Don't retry certain types of errors
        if isinstance(exception, KafkaOpsError):
            non_retryable_codes = {
                ErrorCode.VALIDATION_ERROR,
                ErrorCode.AUTHENTICATION_FAILED,
                ErrorCode.AUTHORIZATION_FAILED,
                ErrorCode.TOPIC_ALREADY_EXISTS,
                ErrorCode.INSTANCE_ALREADY_EXISTS,
                ErrorCode.TOPIC_NOT_FOUND,
                ErrorCode.INSTANCE_NOT_FOUND
            }
            
            if exception.error_code in non_retryable_codes:
                return False
        
        return True


def retry_with_backoff(
    config: Optional[RetryConfig] = None,
    exceptions: Union[Type[Exception], tuple] = Exception
):
    """Decorator for retrying functions with exponential backoff.
    
    Args:
        config: Retry configuration (uses defaults if None)
        exceptions: Exception types to retry on
    """
    if config is None:
        config = RetryConfig()
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            retry_manager = RetryManager(config)
            last_exception = None
            
            for attempt in range(1, config.max_attempts + 1):
                try:
                    if asyncio.iscoroutinefunction(func):
                        return await func(*args, **kwargs)
                    else:
                        return func(*args, **kwargs)
                
                except exceptions as e:
                    last_exception = e
                    
                    if not retry_manager.should_retry(attempt, e):
                        logger.warning(f"Not retrying {func.__name__} after attempt {attempt}: {e}")
                        break
                    
                    if attempt < config.max_attempts:
                        delay = retry_manager.calculate_delay(attempt)
                        logger.warning(
                            f"Attempt {attempt} of {func.__name__} failed: {e}. "
                            f"Retrying in {delay:.2f} seconds..."
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f"All {config.max_attempts} attempts of {func.__name__} failed")
            
            # If we get here, all retries failed
            raise last_exception
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            retry_manager = RetryManager(config)
            last_exception = None
            
            for attempt in range(1, config.max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                
                except exceptions as e:
                    last_exception = e
                    
                    if not retry_manager.should_retry(attempt, e):
                        logger.warning(f"Not retrying {func.__name__} after attempt {attempt}: {e}")
                        break
                    
                    if attempt < config.max_attempts:
                        delay = retry_manager.calculate_delay(attempt)
                        logger.warning(
                            f"Attempt {attempt} of {func.__name__} failed: {e}. "
                            f"Retrying in {delay:.2f} seconds..."
                        )
                        time.sleep(delay)
                    else:
                        logger.error(f"All {config.max_attempts} attempts of {func.__name__} failed")
            
            # If we get here, all retries failed
            raise last_exception
        
        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def circuit_breaker(
    name: str,
    config: Optional[CircuitBreakerConfig] = None,
    exceptions: Union[Type[Exception], tuple] = Exception
):
    """Decorator for circuit breaker pattern.
    
    Args:
        name: Name of the circuit breaker
        config: Circuit breaker configuration (uses defaults if None)
        exceptions: Exception types that trigger circuit breaker
    """
    if config is None:
        config = CircuitBreakerConfig()
    
    # Global registry of circuit breakers
    if not hasattr(circuit_breaker, '_breakers'):
        circuit_breaker._breakers = {}
    
    if name not in circuit_breaker._breakers:
        circuit_breaker._breakers[name] = CircuitBreaker(name, config)
    
    breaker = circuit_breaker._breakers[name]
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            if not breaker.can_execute():
                raise KafkaOpsError(
                    f"Circuit breaker '{name}' is open",
                    ErrorCode.INTERNAL_ERROR,
                    details={'circuit_state': breaker.state.value}
                )
            
            try:
                # Set timeout for operation
                if asyncio.iscoroutinefunction(func):
                    result = await asyncio.wait_for(
                        func(*args, **kwargs),
                        timeout=config.timeout
                    )
                else:
                    result = func(*args, **kwargs)
                
                breaker.record_success()
                return result
            
            except exceptions as e:
                breaker.record_failure()
                raise
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            if not breaker.can_execute():
                raise KafkaOpsError(
                    f"Circuit breaker '{name}' is open",
                    ErrorCode.INTERNAL_ERROR,
                    details={'circuit_state': breaker.state.value}
                )
            
            try:
                result = func(*args, **kwargs)
                breaker.record_success()
                return result
            
            except exceptions as e:
                breaker.record_failure()
                raise
        
        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def get_circuit_breaker_stats() -> List[dict]:
    """Get statistics for all circuit breakers."""
    if not hasattr(circuit_breaker, '_breakers'):
        return []
    
    return [breaker.get_stats() for breaker in circuit_breaker._breakers.values()]


def reset_circuit_breaker(name: str) -> bool:
    """Reset a circuit breaker to closed state."""
    if not hasattr(circuit_breaker, '_breakers'):
        return False
    
    if name in circuit_breaker._breakers:
        breaker = circuit_breaker._breakers[name]
        breaker.state = CircuitState.CLOSED
        breaker.failure_count = 0
        breaker.success_count = 0
        logger.info(f"Circuit breaker '{name}' manually reset to CLOSED state")
        return True
    
    return False


# Convenience decorators with common configurations

def retry_kafka_operation(max_attempts: int = 3):
    """Retry decorator specifically for Kafka operations."""
    config = RetryConfig(
        max_attempts=max_attempts,
        base_delay=1.0,
        max_delay=30.0,
        exponential_base=2.0,
        jitter=True
    )
    
    return retry_with_backoff(
        config=config,
        exceptions=(KafkaOpsError, ConnectionError, TimeoutError)
    )


def kafka_circuit_breaker(name: str):
    """Circuit breaker decorator specifically for Kafka operations."""
    config = CircuitBreakerConfig(
        failure_threshold=3,
        recovery_timeout=30.0,
        success_threshold=2,
        timeout=15.0
    )
    
    return circuit_breaker(
        name=name,
        config=config,
        exceptions=(KafkaOpsError, ConnectionError, TimeoutError)
    )


def storage_circuit_breaker(name: str):
    """Circuit breaker decorator specifically for storage operations."""
    config = CircuitBreakerConfig(
        failure_threshold=5,
        recovery_timeout=60.0,
        success_threshold=3,
        timeout=10.0
    )
    
    return circuit_breaker(
        name=name,
        config=config,
        exceptions=(Exception,)  # Catch all storage exceptions
    )