"""Tests for error handling functionality."""

import pytest
import asyncio
import time
from unittest.mock import Mock, patch

from kafka_ops_agent.exceptions import (
    KafkaOpsError, ErrorCode, ValidationError, AuthenticationError,
    TopicNotFoundError, ClusterProvisioningError, RateLimitError,
    format_error_response
)
from kafka_ops_agent.utils.retry import (
    RetryConfig, CircuitBreakerConfig, RetryManager, CircuitBreaker,
    retry_with_backoff, circuit_breaker, CircuitState
)
from kafka_ops_agent.utils.error_handlers import (
    ErrorResponseFormatter, create_error_response
)


class TestCustomExceptions:
    """Test custom exception classes."""
    
    def test_kafka_ops_error_basic(self):
        """Test basic KafkaOpsError functionality."""
        error = KafkaOpsError(
            message="Test error",
            error_code=ErrorCode.INTERNAL_ERROR,
            details={"key": "value"}
        )
        
        assert error.message == "Test error"
        assert error.error_code == ErrorCode.INTERNAL_ERROR
        assert error.details == {"key": "value"}
        assert error.cause is None
    
    def test_kafka_ops_error_with_cause(self):
        """Test KafkaOpsError with underlying cause."""
        cause = ValueError("Original error")
        error = KafkaOpsError(
            message="Wrapper error",
            error_code=ErrorCode.VALIDATION_ERROR,
            cause=cause
        )
        
        assert error.cause == cause
        assert "caused by: Original error" in str(error)
    
    def test_kafka_ops_error_to_dict(self):
        """Test converting KafkaOpsError to dictionary."""
        error = KafkaOpsError(
            message="Test error",
            error_code=ErrorCode.TOPIC_NOT_FOUND,
            details={"topic": "test-topic"}
        )
        
        error_dict = error.to_dict()
        
        assert error_dict['error'] == ErrorCode.TOPIC_NOT_FOUND.value
        assert error_dict['message'] == "Test error"
        assert error_dict['details'] == {"topic": "test-topic"}
        assert 'cause' not in error_dict
    
    def test_validation_error(self):
        """Test ValidationError specific functionality."""
        error = ValidationError(
            message="Invalid field value",
            field="topic_name",
            value="invalid-name"
        )
        
        assert error.error_code == ErrorCode.VALIDATION_ERROR
        assert error.details['field'] == "topic_name"
        assert error.details['value'] == "invalid-name"
    
    def test_topic_not_found_error(self):
        """Test TopicNotFoundError."""
        error = TopicNotFoundError("my-topic")
        
        assert error.error_code == ErrorCode.TOPIC_NOT_FOUND
        assert "my-topic" in error.message
        assert error.details['topic_name'] == "my-topic"
    
    def test_cluster_provisioning_error(self):
        """Test ClusterProvisioningError."""
        error = ClusterProvisioningError(
            message="Provisioning failed",
            cluster_id="test-cluster",
            provider="docker"
        )
        
        assert error.error_code == ErrorCode.CLUSTER_PROVISIONING_FAILED
        assert error.details['cluster_id'] == "test-cluster"
        assert error.details['provider'] == "docker"
    
    def test_rate_limit_error(self):
        """Test RateLimitError."""
        error = RateLimitError(retry_after=60)
        
        assert error.error_code == ErrorCode.RATE_LIMIT_EXCEEDED
        assert error.details['retry_after_seconds'] == 60


class TestRetryManager:
    """Test retry functionality."""
    
    def test_retry_config_defaults(self):
        """Test default retry configuration."""
        config = RetryConfig()
        
        assert config.max_attempts == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 60.0
        assert config.exponential_base == 2.0
        assert config.jitter is True
    
    def test_calculate_delay_exponential(self):
        """Test exponential backoff delay calculation."""
        config = RetryConfig(
            base_delay=1.0,
            exponential_base=2.0,
            max_delay=10.0,
            jitter=False
        )
        manager = RetryManager(config)
        
        assert manager.calculate_delay(1) == 1.0  # 1.0 * 2^0
        assert manager.calculate_delay(2) == 2.0  # 1.0 * 2^1
        assert manager.calculate_delay(3) == 4.0  # 1.0 * 2^2
        assert manager.calculate_delay(4) == 8.0  # 1.0 * 2^3
        assert manager.calculate_delay(5) == 10.0  # Capped at max_delay
    
    def test_calculate_delay_linear(self):
        """Test linear backoff delay calculation."""
        config = RetryConfig(
            base_delay=2.0,
            backoff_strategy="linear",
            jitter=False
        )
        manager = RetryManager(config)
        
        assert manager.calculate_delay(1) == 2.0  # 2.0 * 1
        assert manager.calculate_delay(2) == 4.0  # 2.0 * 2
        assert manager.calculate_delay(3) == 6.0  # 2.0 * 3
    
    def test_calculate_delay_fixed(self):
        """Test fixed delay calculation."""
        config = RetryConfig(
            base_delay=3.0,
            backoff_strategy="fixed",
            jitter=False
        )
        manager = RetryManager(config)
        
        assert manager.calculate_delay(1) == 3.0
        assert manager.calculate_delay(2) == 3.0
        assert manager.calculate_delay(3) == 3.0
    
    def test_should_retry_max_attempts(self):
        """Test retry limit enforcement."""
        config = RetryConfig(max_attempts=3)
        manager = RetryManager(config)
        
        assert manager.should_retry(1, Exception()) is True
        assert manager.should_retry(2, Exception()) is True
        assert manager.should_retry(3, Exception()) is False
    
    def test_should_retry_non_retryable_errors(self):
        """Test that certain errors are not retried."""
        config = RetryConfig(max_attempts=5)
        manager = RetryManager(config)
        
        # These should not be retried
        validation_error = ValidationError("Invalid input")
        auth_error = AuthenticationError()
        
        assert manager.should_retry(1, validation_error) is False
        assert manager.should_retry(1, auth_error) is False
        
        # This should be retried
        connection_error = KafkaOpsError("Connection failed", ErrorCode.KAFKA_CONNECTION_ERROR)
        assert manager.should_retry(1, connection_error) is True


class TestRetryDecorator:
    """Test retry decorator functionality."""
    
    @pytest.mark.asyncio
    async def test_retry_success_on_first_attempt(self):
        """Test successful operation on first attempt."""
        call_count = 0
        
        @retry_with_backoff(RetryConfig(max_attempts=3, base_delay=0.01))
        async def test_func():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = await test_func()
        
        assert result == "success"
        assert call_count == 1
    
    @pytest.mark.asyncio
    async def test_retry_success_after_failures(self):
        """Test successful operation after some failures."""
        call_count = 0
        
        @retry_with_backoff(RetryConfig(max_attempts=3, base_delay=0.01))
        async def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Temporary failure")
            return "success"
        
        result = await test_func()
        
        assert result == "success"
        assert call_count == 3
    
    @pytest.mark.asyncio
    async def test_retry_all_attempts_fail(self):
        """Test when all retry attempts fail."""
        call_count = 0
        
        @retry_with_backoff(RetryConfig(max_attempts=3, base_delay=0.01))
        async def test_func():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Persistent failure")
        
        with pytest.raises(ConnectionError, match="Persistent failure"):
            await test_func()
        
        assert call_count == 3
    
    @pytest.mark.asyncio
    async def test_retry_non_retryable_error(self):
        """Test that non-retryable errors are not retried."""
        call_count = 0
        
        @retry_with_backoff(RetryConfig(max_attempts=3, base_delay=0.01))
        async def test_func():
            nonlocal call_count
            call_count += 1
            raise ValidationError("Invalid input")
        
        with pytest.raises(ValidationError):
            await test_func()
        
        assert call_count == 1  # Should not retry
    
    def test_retry_sync_function(self):
        """Test retry decorator with synchronous function."""
        call_count = 0
        
        @retry_with_backoff(RetryConfig(max_attempts=3, base_delay=0.01))
        def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Temporary failure")
            return "success"
        
        result = test_func()
        
        assert result == "success"
        assert call_count == 2


class TestCircuitBreaker:
    """Test circuit breaker functionality."""
    
    def test_circuit_breaker_initial_state(self):
        """Test circuit breaker initial state."""
        config = CircuitBreakerConfig(failure_threshold=3)
        breaker = CircuitBreaker("test", config)
        
        assert breaker.state == CircuitState.CLOSED
        assert breaker.failure_count == 0
        assert breaker.can_execute() is True
    
    def test_circuit_breaker_opens_after_failures(self):
        """Test circuit breaker opens after threshold failures."""
        config = CircuitBreakerConfig(failure_threshold=3)
        breaker = CircuitBreaker("test", config)
        
        # Record failures
        for i in range(3):
            breaker.record_failure()
            if i < 2:
                assert breaker.state == CircuitState.CLOSED
            else:
                assert breaker.state == CircuitState.OPEN
        
        assert breaker.can_execute() is False
    
    def test_circuit_breaker_half_open_transition(self):
        """Test circuit breaker transitions to half-open after timeout."""
        config = CircuitBreakerConfig(failure_threshold=2, recovery_timeout=0.1)
        breaker = CircuitBreaker("test", config)
        
        # Open the circuit
        breaker.record_failure()
        breaker.record_failure()
        assert breaker.state == CircuitState.OPEN
        
        # Wait for recovery timeout
        time.sleep(0.2)
        
        # Should transition to half-open
        assert breaker.can_execute() is True
        assert breaker.state == CircuitState.HALF_OPEN
    
    def test_circuit_breaker_closes_after_success(self):
        """Test circuit breaker closes after successful operations in half-open."""
        config = CircuitBreakerConfig(failure_threshold=2, success_threshold=2, recovery_timeout=0.1)
        breaker = CircuitBreaker("test", config)
        
        # Open the circuit
        breaker.record_failure()
        breaker.record_failure()
        assert breaker.state == CircuitState.OPEN
        
        # Wait and transition to half-open
        time.sleep(0.2)
        breaker.can_execute()  # This transitions to half-open
        
        # Record successful operations
        breaker.record_success()
        assert breaker.state == CircuitState.HALF_OPEN
        
        breaker.record_success()
        assert breaker.state == CircuitState.CLOSED
    
    def test_circuit_breaker_reopens_on_failure_in_half_open(self):
        """Test circuit breaker reopens on failure during half-open."""
        config = CircuitBreakerConfig(failure_threshold=2, recovery_timeout=0.1)
        breaker = CircuitBreaker("test", config)
        
        # Open the circuit
        breaker.record_failure()
        breaker.record_failure()
        
        # Wait and transition to half-open
        time.sleep(0.2)
        breaker.can_execute()
        
        # Failure in half-open should reopen circuit
        breaker.record_failure()
        assert breaker.state == CircuitState.OPEN
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_decorator(self):
        """Test circuit breaker decorator."""
        call_count = 0
        
        @circuit_breaker("test_decorator", CircuitBreakerConfig(failure_threshold=2, timeout=1.0))
        async def test_func():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise ConnectionError("Failure")
            return "success"
        
        # First two calls should fail and open circuit
        with pytest.raises(ConnectionError):
            await test_func()
        
        with pytest.raises(ConnectionError):
            await test_func()
        
        # Third call should be blocked by open circuit
        with pytest.raises(KafkaOpsError, match="Circuit breaker.*is open"):
            await test_func()
        
        assert call_count == 2  # Third call was blocked


class TestErrorResponseFormatter:
    """Test error response formatting."""
    
    def test_format_api_error_custom_exception(self):
        """Test formatting custom KafkaOpsError."""
        error = TopicNotFoundError("test-topic")
        
        response = ErrorResponseFormatter.format_api_error(error)
        
        assert response['success'] is False
        assert response['error_code'] == ErrorCode.TOPIC_NOT_FOUND.value
        assert response['message'] == "Topic 'test-topic' not found"
        assert response['details']['topic_name'] == "test-topic"
        assert response['http_status'] == 404
        assert 'timestamp' in response
    
    def test_format_api_error_generic_exception(self):
        """Test formatting generic exception."""
        error = ValueError("Generic error")
        
        response = ErrorResponseFormatter.format_api_error(error)
        
        assert response['success'] is False
        assert response['error_code'] == ErrorCode.INTERNAL_ERROR.value
        assert response['message'] == "Generic error"
        assert response['http_status'] == 500
    
    def test_format_osb_error(self):
        """Test formatting error for OSB API."""
        error = TopicNotFoundError("test-topic")
        
        response, status_code = ErrorResponseFormatter.format_osb_error(error)
        
        assert response['error'] == ErrorCode.TOPIC_NOT_FOUND.value
        assert response['description'] == "Topic 'test-topic' not found"
        assert status_code == 404
    
    def test_format_validation_errors(self):
        """Test formatting validation errors."""
        errors = [
            {"field": "name", "message": "Required field"},
            {"field": "partitions", "message": "Must be positive integer"}
        ]
        
        response = ErrorResponseFormatter.format_validation_errors(errors)
        
        assert response['success'] is False
        assert response['error_code'] == ErrorCode.VALIDATION_ERROR.value
        assert response['validation_errors'] == errors
        assert response['http_status'] == 400
    
    def test_get_http_status_mapping(self):
        """Test HTTP status code mapping."""
        assert ErrorResponseFormatter._get_http_status(ErrorCode.VALIDATION_ERROR) == 400
        assert ErrorResponseFormatter._get_http_status(ErrorCode.AUTHENTICATION_FAILED) == 401
        assert ErrorResponseFormatter._get_http_status(ErrorCode.AUTHORIZATION_FAILED) == 403
        assert ErrorResponseFormatter._get_http_status(ErrorCode.TOPIC_NOT_FOUND) == 404
        assert ErrorResponseFormatter._get_http_status(ErrorCode.TOPIC_ALREADY_EXISTS) == 409
        assert ErrorResponseFormatter._get_http_status(ErrorCode.RATE_LIMIT_EXCEEDED) == 429
        assert ErrorResponseFormatter._get_http_status(ErrorCode.INTERNAL_ERROR) == 500
        assert ErrorResponseFormatter._get_http_status(ErrorCode.KAFKA_CONNECTION_ERROR) == 502


class TestErrorUtilities:
    """Test error utility functions."""
    
    def test_format_error_response(self):
        """Test format_error_response utility function."""
        error = ValidationError("Invalid input", field="name")
        
        response = format_error_response(error)
        
        assert response['error'] == ErrorCode.VALIDATION_ERROR.value
        assert response['message'] == "Invalid input"
        assert response['details']['field'] == "name"
    
    def test_create_error_response(self):
        """Test create_error_response utility function."""
        response, status_code = create_error_response(
            message="Test error",
            error_code=ErrorCode.TOPIC_NOT_FOUND,
            details={"topic": "test"}
        )
        
        assert response['success'] is False
        assert response['error_code'] == ErrorCode.TOPIC_NOT_FOUND.value
        assert response['message'] == "Test error"
        assert response['details']['topic'] == "test"
        assert status_code == 404


if __name__ == '__main__':
    pytest.main([__file__])