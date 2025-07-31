#!/usr/bin/env python3
"""Test script for error handling functionality."""

import asyncio
import sys
import os
import time

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka_ops_agent.exceptions import *
from kafka_ops_agent.utils.retry import *
from kafka_ops_agent.utils.error_handlers import *


def test_custom_exceptions():
    """Test custom exception functionality."""
    print("🧪 Testing Custom Exceptions...")
    
    tests_passed = 0
    tests_total = 0
    
    # Test KafkaOpsError
    tests_total += 1
    try:
        error = KafkaOpsError("Test error", ErrorCode.INTERNAL_ERROR, {"key": "value"})
        assert error.message == "Test error"
        assert error.error_code == ErrorCode.INTERNAL_ERROR
        assert error.details == {"key": "value"}
        
        error_dict = error.to_dict()
        assert error_dict['error'] == ErrorCode.INTERNAL_ERROR.value
        assert error_dict['message'] == "Test error"
        
        print("  ✅ KafkaOpsError basic functionality")
        tests_passed += 1
    except Exception as e:
        print(f"  ❌ KafkaOpsError test failed: {e}")
    
    # Test ValidationError
    tests_total += 1
    try:
        error = ValidationError("Invalid field", field="name", value="test")
        assert error.error_code == ErrorCode.VALIDATION_ERROR
        assert error.details['field'] == "name"
        assert error.details['value'] == "test"
        
        print("  ✅ ValidationError functionality")
        tests_passed += 1
    except Exception as e:
        print(f"  ❌ ValidationError test failed: {e}")
    
    # Test TopicNotFoundError
    tests_total += 1
    try:
        error = TopicNotFoundError("test-topic")
        assert error.error_code == ErrorCode.TOPIC_NOT_FOUND
        assert "test-topic" in error.message
        assert error.details['topic_name'] == "test-topic"
        
        print("  ✅ TopicNotFoundError functionality")
        tests_passed += 1
    except Exception as e:
        print(f"  ❌ TopicNotFoundError test failed: {e}")
    
    return tests_passed, tests_total


async def test_retry_mechanism():
    """Test retry mechanism."""
    print("\n🧪 Testing Retry Mechanism...")
    
    tests_passed = 0
    tests_total = 0
    
    # Test successful retry
    tests_total += 1
    try:
        attempt_count = 0
        
        @retry_with_backoff(RetryConfig(max_attempts=3, base_delay=0.01))
        async def test_func():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise ConnectionError("Temporary failure")
            return "success"
        
        result = await test_func()
        assert result == "success"
        assert attempt_count == 3
        
        print("  ✅ Retry with eventual success")
        tests_passed += 1
    except Exception as e:
        print(f"  ❌ Retry success test failed: {e}")
    
    # Test retry exhaustion
    tests_total += 1
    try:
        attempt_count = 0
        
        @retry_with_backoff(RetryConfig(max_attempts=2, base_delay=0.01))
        async def failing_func():
            nonlocal attempt_count
            attempt_count += 1
            raise ConnectionError("Persistent failure")
        
        try:
            await failing_func()
            assert False, "Should have raised exception"
        except ConnectionError:
            assert attempt_count == 2
            print("  ✅ Retry exhaustion handling")
            tests_passed += 1
    except Exception as e:
        print(f"  ❌ Retry exhaustion test failed: {e}")
    
    # Test non-retryable errors
    tests_total += 1
    try:
        attempt_count = 0
        
        @retry_with_backoff(RetryConfig(max_attempts=3, base_delay=0.01))
        async def validation_error_func():
            nonlocal attempt_count
            attempt_count += 1
            raise ValidationError("Invalid input")
        
        try:
            await validation_error_func()
            assert False, "Should have raised exception"
        except ValidationError:
            assert attempt_count == 1  # Should not retry
            print("  ✅ Non-retryable error handling")
            tests_passed += 1
    except Exception as e:
        print(f"  ❌ Non-retryable error test failed: {e}")
    
    return tests_passed, tests_total


async def test_circuit_breaker():
    """Test circuit breaker functionality."""
    print("\n🧪 Testing Circuit Breaker...")
    
    tests_passed = 0
    tests_total = 0
    
    # Test circuit breaker opening
    tests_total += 1
    try:
        call_count = 0
        
        @circuit_breaker(
            "test_breaker",
            CircuitBreakerConfig(failure_threshold=2, recovery_timeout=0.1, timeout=0.5)
        )
        async def failing_service():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Service down")
        
        # First two calls should fail and open circuit
        for i in range(2):
            try:
                await failing_service()
            except ConnectionError:
                pass
        
        # Third call should be blocked by circuit breaker
        try:
            await failing_service()
            assert False, "Should have been blocked by circuit breaker"
        except KafkaOpsError as e:
            assert "Circuit breaker" in str(e)
            assert call_count == 2  # Third call was blocked
            print("  ✅ Circuit breaker opening")
            tests_passed += 1
    except Exception as e:
        print(f"  ❌ Circuit breaker test failed: {e}")
    
    return tests_passed, tests_total


def test_error_response_formatting():
    """Test error response formatting."""
    print("\n🧪 Testing Error Response Formatting...")
    
    tests_passed = 0
    tests_total = 0
    
    # Test API error formatting
    tests_total += 1
    try:
        error = TopicNotFoundError("test-topic")
        response = ErrorResponseFormatter.format_api_error(error)
        
        assert response['success'] is False
        assert response['error_code'] == ErrorCode.TOPIC_NOT_FOUND.value
        assert response['http_status'] == 404
        assert 'timestamp' in response
        
        print("  ✅ API error formatting")
        tests_passed += 1
    except Exception as e:
        print(f"  ❌ API error formatting test failed: {e}")
    
    # Test OSB error formatting
    tests_total += 1
    try:
        error = ValidationError("Invalid input")
        response, status_code = ErrorResponseFormatter.format_osb_error(error)
        
        assert response['error'] == ErrorCode.VALIDATION_ERROR.value
        assert response['description'] == "Invalid input"
        assert status_code == 400
        
        print("  ✅ OSB error formatting")
        tests_passed += 1
    except Exception as e:
        print(f"  ❌ OSB error formatting test failed: {e}")
    
    # Test validation error formatting
    tests_total += 1
    try:
        errors = [{"field": "name", "message": "Required"}]
        response = ErrorResponseFormatter.format_validation_errors(errors)
        
        assert response['success'] is False
        assert response['error_code'] == ErrorCode.VALIDATION_ERROR.value
        assert response['validation_errors'] == errors
        
        print("  ✅ Validation error formatting")
        tests_passed += 1
    except Exception as e:
        print(f"  ❌ Validation error formatting test failed: {e}")
    
    return tests_passed, tests_total


def test_retry_config():
    """Test retry configuration and delay calculation."""
    print("\n🧪 Testing Retry Configuration...")
    
    tests_passed = 0
    tests_total = 0
    
    # Test exponential backoff
    tests_total += 1
    try:
        config = RetryConfig(base_delay=1.0, exponential_base=2.0, max_delay=10.0, jitter=False)
        manager = RetryManager(config)
        
        assert manager.calculate_delay(1) == 1.0
        assert manager.calculate_delay(2) == 2.0
        assert manager.calculate_delay(3) == 4.0
        assert manager.calculate_delay(5) == 10.0  # Capped at max_delay
        
        print("  ✅ Exponential backoff calculation")
        tests_passed += 1
    except Exception as e:
        print(f"  ❌ Exponential backoff test failed: {e}")
    
    # Test linear backoff
    tests_total += 1
    try:
        config = RetryConfig(base_delay=2.0, backoff_strategy="linear", jitter=False)
        manager = RetryManager(config)
        
        assert manager.calculate_delay(1) == 2.0
        assert manager.calculate_delay(2) == 4.0
        assert manager.calculate_delay(3) == 6.0
        
        print("  ✅ Linear backoff calculation")
        tests_passed += 1
    except Exception as e:
        print(f"  ❌ Linear backoff test failed: {e}")
    
    return tests_passed, tests_total


def test_error_utilities():
    """Test error utility functions."""
    print("\n🧪 Testing Error Utilities...")
    
    tests_passed = 0
    tests_total = 0
    
    # Test create_error_response
    tests_total += 1
    try:
        response, status_code = create_error_response(
            "Test error",
            ErrorCode.VALIDATION_ERROR,
            {"field": "name"}
        )
        
        assert response['success'] is False
        assert response['error_code'] == ErrorCode.VALIDATION_ERROR.value
        assert response['message'] == "Test error"
        assert response['details']['field'] == "name"
        assert status_code == 400
        
        print("  ✅ create_error_response utility")
        tests_passed += 1
    except Exception as e:
        print(f"  ❌ create_error_response test failed: {e}")
    
    return tests_passed, tests_total


async def main():
    """Run all error handling tests."""
    print("🚀 Error Handling Tests")
    print("=" * 50)
    
    total_passed = 0
    total_tests = 0
    
    # Run all test suites
    test_suites = [
        ("Custom Exceptions", test_custom_exceptions),
        ("Retry Mechanism", test_retry_mechanism),
        ("Circuit Breaker", test_circuit_breaker),
        ("Error Response Formatting", test_error_response_formatting),
        ("Retry Configuration", test_retry_config),
        ("Error Utilities", test_error_utilities),
    ]
    
    for suite_name, test_func in test_suites:
        try:
            if asyncio.iscoroutinefunction(test_func):
                passed, total = await test_func()
            else:
                passed, total = test_func()
            
            total_passed += passed
            total_tests += total
            
            print(f"  {suite_name}: {passed}/{total} tests passed")
            
        except Exception as e:
            print(f"  ❌ {suite_name} suite failed: {e}")
    
    print(f"\n{'='*50}")
    print(f"Overall Results: {total_passed}/{total_tests} tests passed")
    
    if total_passed == total_tests:
        print("🎉 All error handling tests passed!")
        return 0
    else:
        print(f"💥 {total_tests - total_passed} tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))