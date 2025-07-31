#!/usr/bin/env python3
"""Script to run integration tests with proper setup and teardown."""

import sys
import os
import subprocess
import time
import argparse
import signal
from pathlib import Path
from typing import List, Optional

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


class IntegrationTestRunner:
    """Integration test runner with environment management."""
    
    def __init__(self, args):
        """Initialize test runner.
        
        Args:
            args: Command line arguments
        """
        self.args = args
        self.compose_file = project_root / "docker-compose.test.yml"
        self.test_dir = project_root / "tests" / "integration"
        self.results_dir = project_root / "test-results"
        self.logs_dir = project_root / "logs"
        
        # Ensure directories exist
        self.results_dir.mkdir(exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)
        
        self.services_started = False
        self.cleanup_on_exit = True
    
    def run(self) -> int:
        """Run integration tests.
        
        Returns:
            Exit code (0 for success, non-zero for failure)
        """
        try:
            # Set up signal handlers for cleanup
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
            
            print("🚀 Starting Integration Test Runner")
            print("=" * 50)
            
            if not self.args.skip_setup:
                if not self._setup_environment():
                    return 1
            
            if not self._run_tests():
                return 1
            
            if not self.args.skip_cleanup:
                self._cleanup_environment()
            
            print("\\n✅ Integration tests completed successfully!")
            return 0
            
        except KeyboardInterrupt:
            print("\\n⏹️  Test run interrupted by user")
            if not self.args.skip_cleanup:
                self._cleanup_environment()
            return 130
        
        except Exception as e:
            print(f"\\n❌ Test run failed: {e}")
            if not self.args.skip_cleanup:
                self._cleanup_environment()
            return 1
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        print(f"\\n⚠️  Received signal {signum}, shutting down...")
        if not self.args.skip_cleanup:
            self._cleanup_environment()
        sys.exit(130)
    
    def _setup_environment(self) -> bool:
        """Set up test environment.
        
        Returns:
            True if successful, False otherwise
        """
        print("\\n🔧 Setting up test environment...")
        
        # Check Docker is available
        if not self._check_docker():
            return False
        
        # Clean up any existing test environment
        print("🧹 Cleaning up existing test environment...")
        self._run_command([
            "docker-compose", "-f", str(self.compose_file), 
            "down", "-v", "--remove-orphans"
        ], check=False)
        
        # Build test images
        if not self.args.skip_build:
            print("🏗️  Building test images...")
            if not self._run_command([
                "docker-compose", "-f", str(self.compose_file), "build"
            ]):
                return False
        
        # Start services
        print("🚀 Starting test services...")
        if not self._run_command([
            "docker-compose", "-f", str(self.compose_file), 
            "up", "-d", "--wait"
        ]):
            return False
        
        self.services_started = True
        
        # Wait for services to be ready
        print("⏳ Waiting for services to be ready...")
        if not self._wait_for_services():
            return False
        
        print("✅ Test environment ready!")
        return True
    
    def _check_docker(self) -> bool:
        """Check if Docker is available.
        
        Returns:
            True if Docker is available, False otherwise
        """
        try:
            result = subprocess.run(
                ["docker", "--version"], 
                capture_output=True, 
                text=True, 
                timeout=10
            )
            
            if result.returncode != 0:
                print("❌ Docker is not available")
                return False
            
            result = subprocess.run(
                ["docker-compose", "--version"], 
                capture_output=True, 
                text=True, 
                timeout=10
            )
            
            if result.returncode != 0:
                print("❌ Docker Compose is not available")
                return False
            
            print("✅ Docker and Docker Compose are available")
            return True
            
        except (subprocess.TimeoutExpired, FileNotFoundError):
            print("❌ Docker is not available")
            return False
    
    def _wait_for_services(self) -> bool:
        """Wait for services to be ready.
        
        Returns:
            True if services are ready, False if timeout
        """
        import requests
        
        services = [
            ("Kafka Ops API", "http://localhost:8000/health"),
            ("Monitoring API", "http://localhost:8080/health")
        ]
        
        max_attempts = 60  # 5 minutes
        attempt = 0
        
        while attempt < max_attempts:
            all_ready = True
            
            for service_name, health_url in services:
                try:
                    response = requests.get(health_url, timeout=5)
                    if response.status_code >= 400:
                        all_ready = False
                        break
                except requests.RequestException:
                    all_ready = False
                    break
            
            if all_ready:
                print("✅ All services are ready!")
                return True
            
            attempt += 1
            if attempt % 10 == 0:
                print(f"   Still waiting... (attempt {attempt}/{max_attempts})")
            
            time.sleep(5)
        
        print("❌ Services did not become ready within timeout")
        self._show_service_logs()
        return False
    
    def _show_service_logs(self):
        """Show service logs for debugging."""
        print("\\n📋 Service logs for debugging:")
        
        services = ["kafka-ops-api", "kafka", "postgres"]
        
        for service in services:
            print(f"\\n--- {service} logs ---")
            self._run_command([
                "docker-compose", "-f", str(self.compose_file),
                "logs", "--tail=20", service
            ], check=False)
    
    def _run_tests(self) -> bool:
        """Run the integration tests.
        
        Returns:
            True if tests passed, False otherwise
        """
        print("\\n🧪 Running integration tests...")
        
        # Prepare pytest arguments
        pytest_args = [
            "python", "-m", "pytest",
            str(self.test_dir),
            "-v",
            "--tb=short",
            f"--junitxml={self.results_dir}/integration-tests.xml",
            f"--html={self.results_dir}/integration-tests.html",
            "--self-contained-html"
        ]
        
        # Add additional pytest arguments
        if self.args.test_pattern:
            pytest_args.extend(["-k", self.args.test_pattern])
        
        if self.args.test_file:
            pytest_args = [
                "python", "-m", "pytest",
                str(self.test_dir / self.args.test_file),
                "-v", "--tb=short"
            ]
        
        if self.args.parallel:
            pytest_args.extend(["-n", str(self.args.parallel)])
        
        if self.args.verbose:
            pytest_args.append("-s")
        
        # Set environment variables for tests
        test_env = os.environ.copy()
        test_env.update({
            "TEST_API_URL": "http://localhost:8000",
            "TEST_MONITORING_URL": "http://localhost:8080",
            "TEST_KAFKA_SERVERS": "localhost:9092",
            "TEST_DATABASE_URL": "postgresql://kafka_ops:test_password@localhost:5432/kafka_ops_test",
            "PYTHONPATH": str(project_root)
        })
        
        # Run tests
        print(f"Running: {' '.join(pytest_args)}")
        
        result = subprocess.run(
            pytest_args,
            cwd=project_root,
            env=test_env
        )
        
        if result.returncode == 0:
            print("✅ All integration tests passed!")
            return True
        else:
            print(f"❌ Integration tests failed (exit code: {result.returncode})")
            return False
    
    def _cleanup_environment(self):
        """Clean up test environment."""
        if not self.services_started:
            return
        
        print("\\n🧹 Cleaning up test environment...")
        
        # Stop and remove containers
        self._run_command([
            "docker-compose", "-f", str(self.compose_file),
            "down", "-v", "--remove-orphans"
        ], check=False)
        
        # Remove test images if requested
        if self.args.clean_images:
            print("🗑️  Removing test images...")
            self._run_command([
                "docker", "image", "prune", "-f", "--filter", "label=test=true"
            ], check=False)
        
        print("✅ Test environment cleaned up!")
    
    def _run_command(self, cmd: List[str], check: bool = True) -> bool:
        """Run a command and return success status.
        
        Args:
            cmd: Command to run
            check: Whether to check return code
            
        Returns:
            True if successful (or check=False), False otherwise
        """
        try:
            if self.args.verbose:
                print(f"Running: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                cwd=project_root,
                capture_output=not self.args.verbose,
                text=True
            )
            
            if check and result.returncode != 0:
                if not self.args.verbose:
                    print(f"Command failed: {' '.join(cmd)}")
                    if result.stdout:
                        print(f"STDOUT: {result.stdout}")
                    if result.stderr:
                        print(f"STDERR: {result.stderr}")
                return False
            
            return True
            
        except Exception as e:
            print(f"Error running command {' '.join(cmd)}: {e}")
            return False


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Run integration tests for Kafka Ops Agent"
    )
    
    parser.add_argument(
        "--skip-setup", 
        action="store_true",
        help="Skip environment setup (assume services are running)"
    )
    
    parser.add_argument(
        "--skip-cleanup", 
        action="store_true",
        help="Skip environment cleanup (leave services running)"
    )
    
    parser.add_argument(
        "--skip-build", 
        action="store_true",
        help="Skip building Docker images"
    )
    
    parser.add_argument(
        "--clean-images", 
        action="store_true",
        help="Remove test Docker images during cleanup"
    )
    
    parser.add_argument(
        "--test-pattern", 
        help="Run only tests matching this pattern"
    )
    
    parser.add_argument(
        "--test-file", 
        help="Run only tests in this file (relative to tests/integration/)"
    )
    
    parser.add_argument(
        "--parallel", 
        type=int,
        help="Run tests in parallel (requires pytest-xdist)"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output"
    )
    
    args = parser.parse_args()
    
    runner = IntegrationTestRunner(args)
    exit_code = runner.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()