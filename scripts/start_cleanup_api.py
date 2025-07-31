#!/usr/bin/env python3
"""Start the cleanup operations API server."""

import asyncio
import logging
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka_ops_agent.api.cleanup_operations import create_cleanup_app
from kafka_ops_agent.logging_config import setup_logging

def main():
    """Start the cleanup API server."""
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    print("🚀 Starting Kafka Ops Agent - Cleanup Operations API")
    print("=" * 60)
    
    try:
        # Create Flask app
        app = create_cleanup_app()
        
        print("📋 Available endpoints:")
        print("  POST /api/v1/cleanup/topics      - Execute topic cleanup")
        print("  POST /api/v1/cleanup/clusters    - Execute cluster cleanup")
        print("  POST /api/v1/cleanup/metadata    - Execute metadata cleanup")
        print("  GET  /api/v1/cleanup/status      - Get cleanup overview")
        print("  GET  /api/v1/cleanup/status/<id> - Get cleanup status")
        print("  GET  /api/v1/cleanup/logs/<id>   - Get cleanup logs")
        print()
        print("🌐 Server starting on http://localhost:8083")
        print("📖 Test with: python scripts/test_cleanup_api.py")
        print("🛑 Press Ctrl+C to stop")
        print("=" * 60)
        
        # Start the server
        app.run(
            host='0.0.0.0',
            port=8083,
            debug=False,
            threaded=True
        )
        
    except KeyboardInterrupt:
        print("\n🛑 Server stopped by user")
    except Exception as e:
        logger.error(f"Failed to start cleanup API server: {e}")
        print(f"❌ Failed to start server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()