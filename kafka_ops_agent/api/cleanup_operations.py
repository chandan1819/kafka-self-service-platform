"""REST API for on-demand cleanup operations."""

import logging
from typing import Dict, Any, Optional
from flask import Flask, request, jsonify
from datetime import datetime

from kafka_ops_agent.auth.middleware import AuthMiddleware
from kafka_ops_agent.auth.decorators import (
    cleanup_required, operator_required, authenticated,
    audit_context, log_operation, validate_request_data
)

from kafka_ops_agent.services.scheduler import SchedulerService, TaskType, TaskStatus
from kafka_ops_agent.storage.factory import get_metadata_store, get_audit_store

logger = logging.getLogger(__name__)

# Global scheduler service instance
_scheduler_service: Optional[SchedulerService] = None


async def get_scheduler_service() -> SchedulerService:
    """Get or create scheduler service instance."""
    global _scheduler_service
    
    if _scheduler_service is None:
        metadata_store = await get_metadata_store()
        audit_store = await get_audit_store()
        _scheduler_service = SchedulerService(metadata_store, audit_store)
    
    return _scheduler_service


def create_cleanup_api() -> Flask:
    """Create Flask app for cleanup operations API."""
    app = Flask(__name__)
    
    # Initialize authentication middleware
    auth_middleware = AuthMiddleware()
    auth_middleware.init_app(app)
    
    @app.route('/health', methods=['GET'])
    def health_check():
        """Health check endpoint."""
        return jsonify({
            'status': 'healthy',
            'service': 'cleanup-operations-api',
            'timestamp': datetime.utcnow().isoformat()
        })
    
    @app.route('/cleanup/topic', methods=['POST'])
    @cleanup_required
    @audit_context
    @log_operation("trigger_topic_cleanup")
    @validate_request_data(required_fields=['cluster_id'])
    async def trigger_topic_cleanup():
        """Trigger on-demand topic cleanup."""
        try:
            data = request.get_json() or {}
            
            # Validate required parameters
            cluster_id = data.get('cluster_id')
            if not cluster_id:
                return jsonify({
                    'error': 'cluster_id is required'
                }), 400
            
            # Extract parameters with defaults
            max_age_hours = data.get('max_age_hours', 24)
            retention_pattern = data.get('retention_pattern', 'expired')
            dry_run = data.get('dry_run', False)
            user_id = data.get('user_id')
            
            # Validate parameters
            if not isinstance(max_age_hours, int) or max_age_hours <= 0:
                return jsonify({
                    'error': 'max_age_hours must be a positive integer'
                }), 400
            
            if retention_pattern not in ['expired', 'test', 'all']:
                return jsonify({
                    'error': 'retention_pattern must be one of: expired, test, all'
                }), 400
            
            scheduler = await get_scheduler_service()
            
            # Execute cleanup
            execution = await scheduler.execute_topic_cleanup_now(
                cluster_id=cluster_id,
                max_age_hours=max_age_hours,
                retention_pattern=retention_pattern,
                dry_run=dry_run,
                user_id=user_id
            )
            
            return jsonify({
                'execution_id': execution.execution_id,
                'status': execution.status.value,
                'started_at': execution.started_at.isoformat(),
                'cluster_id': cluster_id,
                'parameters': {
                    'max_age_hours': max_age_hours,
                    'retention_pattern': retention_pattern,
                    'dry_run': dry_run
                },
                'message': f'Topic cleanup {"simulation" if dry_run else "operation"} started for cluster {cluster_id}'
            })
            
        except ValueError as e:
            logger.warning(f"Topic cleanup validation error: {e}")
            return jsonify({
                'error': str(e)
            }), 409  # Conflict
            
        except Exception as e:
            logger.error(f"Topic cleanup error: {e}")
            return jsonify({
                'error': 'Internal server error',
                'details': str(e)
            }), 500
    
    @app.route('/cleanup/cluster', methods=['POST'])
    @cleanup_required
    @audit_context
    @log_operation("trigger_cluster_cleanup")
    async def trigger_cluster_cleanup():
        """Trigger on-demand cluster cleanup."""
        try:
            data = request.get_json() or {}
            
            # Extract parameters with defaults
            max_age_hours = data.get('max_age_hours', 24)
            dry_run = data.get('dry_run', False)
            user_id = data.get('user_id')
            
            # Validate parameters
            if not isinstance(max_age_hours, int) or max_age_hours <= 0:
                return jsonify({
                    'error': 'max_age_hours must be a positive integer'
                }), 400
            
            scheduler = await get_scheduler_service()
            
            # Execute cleanup
            execution = await scheduler.execute_cluster_cleanup_now(
                max_age_hours=max_age_hours,
                dry_run=dry_run,
                user_id=user_id
            )
            
            return jsonify({
                'execution_id': execution.execution_id,
                'status': execution.status.value,
                'started_at': execution.started_at.isoformat(),
                'parameters': {
                    'max_age_hours': max_age_hours,
                    'dry_run': dry_run
                },
                'message': f'Cluster cleanup {"simulation" if dry_run else "operation"} started'
            })
            
        except ValueError as e:
            logger.warning(f"Cluster cleanup validation error: {e}")
            return jsonify({
                'error': str(e)
            }), 409  # Conflict
            
        except Exception as e:
            logger.error(f"Cluster cleanup error: {e}")
            return jsonify({
                'error': 'Internal server error',
                'details': str(e)
            }), 500
    
    @app.route('/cleanup/metadata', methods=['POST'])
    @cleanup_required
    @audit_context
    @log_operation("trigger_metadata_cleanup")
    async def trigger_metadata_cleanup():
        """Trigger on-demand metadata cleanup."""
        try:
            data = request.get_json() or {}
            
            # Extract parameters with defaults
            max_age_days = data.get('max_age_days', 30)
            dry_run = data.get('dry_run', False)
            user_id = data.get('user_id')
            
            # Validate parameters
            if not isinstance(max_age_days, int) or max_age_days <= 0:
                return jsonify({
                    'error': 'max_age_days must be a positive integer'
                }), 400
            
            scheduler = await get_scheduler_service()
            
            # Execute cleanup
            execution = await scheduler.execute_metadata_cleanup_now(
                max_age_days=max_age_days,
                dry_run=dry_run,
                user_id=user_id
            )
            
            return jsonify({
                'execution_id': execution.execution_id,
                'status': execution.status.value,
                'started_at': execution.started_at.isoformat(),
                'parameters': {
                    'max_age_days': max_age_days,
                    'dry_run': dry_run
                },
                'message': f'Metadata cleanup {"simulation" if dry_run else "operation"} started'
            })
            
        except ValueError as e:
            logger.warning(f"Metadata cleanup validation error: {e}")
            return jsonify({
                'error': str(e)
            }), 409  # Conflict
            
        except Exception as e:
            logger.error(f"Metadata cleanup error: {e}")
            return jsonify({
                'error': 'Internal server error',
                'details': str(e)
            }), 500
    
    @app.route('/cleanup/status/<execution_id>', methods=['GET'])
    @authenticated
    @audit_context
    async def get_cleanup_status(execution_id: str):
        """Get cleanup operation status."""
        try:
            scheduler = await get_scheduler_service()
            
            # Try to get from cleanup operations first
            execution = scheduler.get_cleanup_operation(execution_id)
            
            # If not found, try regular executions
            if not execution:
                execution = scheduler.get_execution(execution_id)
            
            if not execution:
                return jsonify({
                    'error': 'Cleanup operation not found'
                }), 404
            
            response_data = {
                'execution_id': execution.execution_id,
                'task_id': execution.task_id,
                'status': execution.status.value,
                'started_at': execution.started_at.isoformat(),
                'completed_at': execution.completed_at.isoformat() if execution.completed_at else None,
                'result': execution.result,
                'error_message': execution.error_message,
                'logs': execution.logs
            }
            
            # Add duration if completed
            if execution.completed_at:
                duration = (execution.completed_at - execution.started_at).total_seconds()
                response_data['duration_seconds'] = duration
            
            return jsonify(response_data)
            
        except Exception as e:
            logger.error(f"Error getting cleanup status: {e}")
            return jsonify({
                'error': 'Internal server error',
                'details': str(e)
            }), 500
    
    @app.route('/cleanup/operations', methods=['GET'])
    @authenticated
    @audit_context
    async def list_cleanup_operations():
        """List cleanup operations."""
        try:
            cluster_id = request.args.get('cluster_id')
            limit = request.args.get('limit', 50, type=int)
            
            scheduler = await get_scheduler_service()
            
            # Get cleanup operations
            operations = scheduler.list_cleanup_operations(cluster_id)
            
            # Limit results
            if limit > 0:
                operations = operations[:limit]
            
            # Format response
            operations_data = []
            for execution in operations:
                operation_data = {
                    'execution_id': execution.execution_id,
                    'task_id': execution.task_id,
                    'status': execution.status.value,
                    'started_at': execution.started_at.isoformat(),
                    'completed_at': execution.completed_at.isoformat() if execution.completed_at else None,
                    'result': execution.result,
                    'error_message': execution.error_message
                }
                
                # Add duration if completed
                if execution.completed_at:
                    duration = (execution.completed_at - execution.started_at).total_seconds()
                    operation_data['duration_seconds'] = duration
                
                operations_data.append(operation_data)
            
            return jsonify({
                'operations': operations_data,
                'total': len(operations_data),
                'filtered_by_cluster': cluster_id
            })
            
        except Exception as e:
            logger.error(f"Error listing cleanup operations: {e}")
            return jsonify({
                'error': 'Internal server error',
                'details': str(e)
            }), 500
    
    @app.route('/cleanup/conflicts/<cluster_id>', methods=['GET'])
    @operator_required
    @audit_context
    async def check_cleanup_conflicts(cluster_id: str):
        """Check for cleanup operation conflicts."""
        try:
            cleanup_type_param = request.args.get('type', 'topic_cleanup')
            
            # Map string to TaskType
            type_mapping = {
                'topic_cleanup': TaskType.TOPIC_CLEANUP,
                'cluster_cleanup': TaskType.CLUSTER_CLEANUP,
                'metadata_cleanup': TaskType.METADATA_CLEANUP
            }
            
            cleanup_type = type_mapping.get(cleanup_type_param)
            if not cleanup_type:
                return jsonify({
                    'error': f'Invalid cleanup type: {cleanup_type_param}. Must be one of: {list(type_mapping.keys())}'
                }), 400
            
            scheduler = await get_scheduler_service()
            
            # Check for conflicts
            conflict = scheduler._check_cleanup_conflicts(cleanup_type, cluster_id)
            
            return jsonify({
                'cluster_id': cluster_id,
                'cleanup_type': cleanup_type_param,
                'has_conflict': conflict is not None,
                'conflict_reason': conflict,
                'can_proceed': conflict is None
            })
            
        except Exception as e:
            logger.error(f"Error checking cleanup conflicts: {e}")
            return jsonify({
                'error': 'Internal server error',
                'details': str(e)
            }), 500
    
    @app.route('/scheduler/stats', methods=['GET'])
    @operator_required
    @audit_context
    async def get_scheduler_stats():
        """Get scheduler statistics."""
        try:
            scheduler = await get_scheduler_service()
            stats = scheduler.get_scheduler_stats()
            
            # Add cleanup-specific stats
            with scheduler.lock:
                active_cleanups = len(scheduler.active_cleanups)
                total_cleanup_operations = len(scheduler.cleanup_operations)
            
            stats.update({
                'active_cleanups': active_cleanups,
                'total_cleanup_operations': total_cleanup_operations
            })
            
            return jsonify(stats)
            
        except Exception as e:
            logger.error(f"Error getting scheduler stats: {e}")
            return jsonify({
                'error': 'Internal server error',
                'details': str(e)
            }), 500
    
    @app.errorhandler(404)
    def not_found(error):
        """Handle 404 errors."""
        return jsonify({
            'error': 'Endpoint not found',
            'available_endpoints': [
                'POST /cleanup/topic',
                'POST /cleanup/cluster', 
                'POST /cleanup/metadata',
                'GET /cleanup/status/<execution_id>',
                'GET /cleanup/operations',
                'GET /cleanup/conflicts/<cluster_id>',
                'GET /scheduler/stats',
                'GET /health'
            ]
        }), 404
    
    @app.errorhandler(405)
    def method_not_allowed(error):
        """Handle 405 errors."""
        return jsonify({
            'error': 'Method not allowed'
        }), 405
    
    return app


if __name__ == '__main__':
    import asyncio
    
    app = create_cleanup_api()
    
    # Run with asyncio support
    app.run(host='0.0.0.0', port=8083, debug=True)