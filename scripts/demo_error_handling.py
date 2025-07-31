#!/usr/bin/env python3
"""Demo script showing comprehensive error handling features."""

import asyncio
import logging
import sys
import os
from typing import Dict, Any

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka_ops_agent.exceptions import (
    KafkaOpsError, ErrorCode, ValidationError, TopicNotFoundError,
    KafkaConnectionError, ClusterProvisioningError, RateLimitError
)
from kafka_ops_agent.utils.retry import (
    retry_with_backoff, RetryConfig, circuit_breaker, CircuitBreakerConfig,
    get_circuit_breaker_stats, reset_circuit_breaker
)
from kafka_ops_agent.utils.error_handlers import (
    ErrorResponseFormatter, create_error_response, log_error_context
)
from kafka_ops_agent.logging_config import setup_logging

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)


def demo_custom_exceptions():
    """Demonstrate custom exception usage."""
    print("🔥 Demo: Custom Exceptions")
    print("=" * 50)
    
    try:
        # Validation error
        raise ValidationError("Invalid topic name", field="name", value="invalid-name!")
    except ValidationError as e:
        print(f"ValidationError: {e}")
        print(f"Error dict: {e.to_dict()}")
        print()
    
    try:
        # Topic not found error
        raise TopicNotFoundError("my-missing-topic")
    except TopicNotFoundError as e:
        print(f"TopicNotFoundError: {e}")
        print(f"Details: {e.details}")
        print()
    
    try:
        # Cluster provisioning error with cause
        original_error = ConnectionError("Network unreachable")
        raise ClusterProvisioningError(
            "Failed to provision cluster",
            cluster_id="test-cluster",
            provider="docker"
        ) from original_error
    except ClusterProvisioningError as e:
        print(f"ClusterProvisioningError: {e}")
        print(f"Cause: {e.__cause__}")
        print()


async def demo_retry_mechanism():
    """Demonstrate retry mechanism with exponential backoff."""
    print("🔄 Demo: Retry Mechanism")
    print("=" * 50)
    
    # Simulate a function that fails a few times then succeeds
    attempt_count = 0
    
    @retry_with_backoff(RetryConfig(max_attempts=4, base_delay=0.1, jitter=False))
    async def flaky_operation():
        nonlocal attempt_count
        attempt_count += 1
        
        if attempt_count < 3:
            print(f"  Attempt {attempt_count}: Failing...")
            raise KafkaConnectionError("Connection failed", cluster_id="test-cluster")
        
        print(f"  Attempt {attempt_count}: Success!")
        return "Operation completed successfully"
    
    try:
        result = await flaky_operation()
        print(f"✅ Result: {result}")
        print(f"Total attempts: {attempt_count}")
    except Exception as e:
        print(f"❌ Final failure: {e}")
    
    print()


async def demo_circuit_breaker():
    """Demonstrate circuit breaker pattern."""
    print("⚡ Demo: Circuit Breaker")
    print("=" * 50)
    
    call_count = 0
    
    @circuit_breaker(
        "demo_service",
        CircuitBreakerConfig(failure_threshold=3, recovery_timeout=1.0, timeout=0.5)
    )
    async def unreliable_service():
        nonlocal call_count
        call_count += 1
        
        # Fail for first 5 calls
        if call_count <= 5:
            print(f"  Call {call_count}: Service failing...")
            raise KafkaConnectionError("Service unavailable")
        
        print(f"  Call {call_count}: Service working!")
        return "Service response"
    
    # Make calls to trigger circuit breaker
    for i in range(8):
        try:
            result = await unreliable_service()
            print(f"✅ Call {i+1} succeeded: {result}")
        except Exception as e:
            print(f"❌ Call {i+1} failed: {e}")
        
        # Show circuit breaker stats
        stats = get_circuit_breaker_stats()
        if stats:
            breaker_stats = stats[0]  # First (and only) circuit breaker
            print(f"   Circuit state: {breaker_stats['state']}, failures: {breaker_stats['failure_count']}")
        
        # Small delay between calls
        await asyncio.sleep(0.1)
    
    print("\n🔧 Resetting circuit breaker...")
    reset_circuit_breaker("demo_service")
    
    # Try again after reset
    try:
        result = await unreliable_service()
        print(f"✅ After reset: {result}")
    except Exception as e:
        print(f"❌ After reset: {e}")
    
    print()


def demo_error_response_formatting():
    """Demonstrate error response formatting for APIs."""
    print("📋 Demo: Error Response Formatting")
    print("=" * 50)
    
    # Test different error types
    errors = [
        ValidationError("Invalid input", field="topic_name", value=""),
        TopicNotFoundError("missing-topic"),
        RateLimitError("Too many requests", retry_after=60),
        KafkaConnectionError("Connection timeout", cluster_id="prod-cluster"),
        ValueError("Generic error")
    ]
    
    for error in errors:
        print(f"Error: {type(error).__name__}")
        
        # Format for API response
        api_response = ErrorResponseFormatter.format_api_error(error, request_id="req-123")
        print(f"  API Response: {api_response}")
        
        # Format for OSB API
        if isinstance(error, KafkaOpsError):
            osb_response, status_code = ErrorResponseFormatter.format_osb_error(error)
            print(f"  OSB Response: {osb_response} (HTTP {status_code})")
        
        print()


def demo_error_context_logging():
    """Demonstrate error context logging."""
    print("📝 Demo: Error Context Logging")
    print("=" * 50)
    
    # Simulate operation with context
    context = {
        "user_id": "user123",
        "cluster_id": "prod-cluster",
        "operation": "create_topic",
        "topic_name": "test-topic"
    }
    
    try:
        # Simulate an error during topic creation
        raise TopicAlreadyExistsError("test-topic")
    except Exception as e:
        log_error_context(e, context)
        print("✅ Error logged with context (check logs)")
    
    print()


def demo_error_utilities():
    """Demonstrate error utility functions."""
    print("🛠️  Demo: Error Utilities")
    print("=" * 50)
    
    # Create standardized error response
    response, status_code = create_error_response(
        message="Topic validation failed",
        error_code=ErrorCode.VALIDATION_ERROR,
        details={"field": "partitions", "value": -1}
    )
    
    print(f"Standardized response: {response}")
    print(f"HTTP status: {status_code}")
    print()
    
    # Format validation errors
    validation_errors = [
        {"field": "name", "message": "Required field"},
        {"field": "partitions", "message": "Must be positive"}
    ]
    
    validation_response = ErrorResponseFormatter.format_validation_errors(validation_errors)
    print(f"Validation errors response: {validation_response}")
    print()


async def main():
    """Run all error handling demos."""
    print("🚀 Kafka Ops Agent - Error Handling Demo")
    print("=" * 60)
    print()
    
    # Run all demos
    demo_custom_exceptions()
    await demo_retry_mechanism()
    await demo_circuit_breaker()
    demo_error_response_formatting()
    demo_error_context_logging()
    demo_error_utilities()
    
    print("🎉 Error handling demo completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())