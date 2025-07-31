"""REST API for topic management operations."""

import logging
import asyncio
from typing import Dict, Any, List, Optional
from flask import Flask, request, jsonify, Blueprint
from functools import wraps

from kafka_ops_agent.auth.decorators import (
    authenticated, topic_read_required, topic_write_required,
    audit_context, log_operation, validate_request_data
)
from kafka_ops_agent.auth.models import Permission

from kafka_ops_agent.services.topic_management import get_topic_service
from kafka_ops_agent.models.topic import (
    TopicConfig, TopicCreateRequest, TopicUpdateRequest, 
    TopicDeleteRequest, TopicPurgeRequest, TopicListRequest,
    BulkTopicOperation, TopicOperationResult
)
from kafka_ops_agent.models.service_broker import ErrorResponse
from kafka_ops_agent.exceptions import KafkaOpsError, ValidationError
from kafka_ops_agent.utils.error_handlers import (
    ErrorResponseFormatter, register_error_handlers, RequestContextLogger
)

logger = logging.getLogger(__name__)

# Create blueprint for topic management API
topic_api = Blueprint('topic_api', __name__, url_prefix='/api/v1')


def async_route(f):
    """Decorator to handle async routes in Flask."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(f(*args, **kwargs))
        finally:
            loop.close()
    return wrapper


def validate_cluster_id(cluster_id: str) -> bool:
    """Validate cluster ID format."""
    if not cluster_id or len(cluster_id) < 3:
        return False
    # Add more validation as needed
    return True


def get_user_id() -> Optional[str]:
    """Extract user ID from request headers."""
    return request.headers.get('X-User-ID') or request.headers.get('X-Broker-API-Originating-Identity')


@topic_api.route('/clusters/<cluster_id>/topics', methods=['POST'])
@topic_write_required
@audit_context
@log_operation("create_topic")
@validate_request_data(required_fields=['name'])
@async_route
async def create_topic(cluster_id: str):
    """Create a new topic in the specified cluster."""
    try:
        # Validate cluster ID
        if not validate_cluster_id(cluster_id):
            return jsonify({
                'error': 'INVALID_CLUSTER_ID',
                'message': 'Invalid cluster ID format'
            }), 400
        
        # Parse request body
        data = request.get_json()
        if not data:
            return jsonify({
                'error': 'MISSING_REQUEST_BODY',
                'message': 'Request body is required'
            }), 400
        
        try:
            # Validate topic configuration
            topic_config = TopicConfig(**data)
        except Exception as e:
            return jsonify({
                'error': 'INVALID_TOPIC_CONFIG',
                'message': f'Invalid topic configuration: {str(e)}'
            }), 400
        
        # Get topic service and create topic
        topic_service = await get_topic_service()
        user_id = get_user_id()
        
        result = await topic_service.create_topic(cluster_id, topic_config, user_id)
        
        if result.success:
            response = {
                'success': True,
                'message': result.message,
                'topic': {
                    'name': result.topic_name,
                    'cluster_id': cluster_id
                }
            }
            if result.details:
                response['topic'].update(result.details)
            
            return jsonify(response), 201
        else:
            error_code = result.error_code or 'CREATION_FAILED'
            status_code = 409 if 'already exists' in result.message.lower() else 500
            
            return jsonify({
                'error': error_code,
                'message': result.message,
                'topic_name': result.topic_name
            }), status_code
            
    except Exception as e:
        logger.error(f"Exception in create_topic: {e}")
        return jsonify({
            'error': 'INTERNAL_ERROR',
            'message': 'Internal server error'
        }), 500


@topic_api.route('/clusters/<cluster_id>/topics', methods=['GET'])
@topic_read_required
@audit_context
@async_route
async def list_topics(cluster_id: str):
    """List topics in the specified cluster."""
    try:
        # Validate cluster ID
        if not validate_cluster_id(cluster_id):
            return jsonify({
                'error': 'INVALID_CLUSTER_ID',
                'message': 'Invalid cluster ID format'
            }), 400
        
        # Parse query parameters
        include_internal = request.args.get('include_internal', 'false').lower() == 'true'
        
        # Get topic service and list topics
        topic_service = await get_topic_service()
        user_id = get_user_id()
        
        topics = await topic_service.list_topics(cluster_id, include_internal, user_id)
        
        # Format response
        topic_list = []
        for topic in topics:
            topic_list.append({
                'name': topic.name,
                'partitions': topic.partitions,
                'replication_factor': topic.replication_factor,
                'configs': topic.configs
            })
        
        return jsonify({
            'cluster_id': cluster_id,
            'topics': topic_list,
            'total_count': len(topic_list),
            'include_internal': include_internal
        }), 200
        
    except Exception as e:
        logger.error(f"Exception in list_topics: {e}")
        return jsonify({
            'error': 'INTERNAL_ERROR',
            'message': 'Internal server error'
        }), 500


@topic_api.route('/clusters/<cluster_id>/topics/<topic_name>', methods=['GET'])
@topic_read_required
@audit_context
@async_route
async def describe_topic(cluster_id: str, topic_name: str):
    """Get detailed information about a specific topic."""
    try:
        # Validate parameters
        if not validate_cluster_id(cluster_id):
            return jsonify({
                'error': 'INVALID_CLUSTER_ID',
                'message': 'Invalid cluster ID format'
            }), 400
        
        if not topic_name:
            return jsonify({
                'error': 'INVALID_TOPIC_NAME',
                'message': 'Topic name is required'
            }), 400
        
        # Get topic service and describe topic
        topic_service = await get_topic_service()
        user_id = get_user_id()
        
        topic_details = await topic_service.describe_topic(cluster_id, topic_name, user_id)
        
        if topic_details:
            return jsonify({
                'cluster_id': cluster_id,
                'topic': {
                    'name': topic_details.name,
                    'partitions': topic_details.partitions,
                    'replication_factor': topic_details.replication_factor,
                    'configs': topic_details.configs,
                    'partition_details': topic_details.partition_details,
                    'total_messages': topic_details.total_messages,
                    'total_size_bytes': topic_details.total_size_bytes
                }
            }), 200
        else:
            return jsonify({
                'error': 'TOPIC_NOT_FOUND',
                'message': f'Topic {topic_name} not found in cluster {cluster_id}'
            }), 404
            
    except Exception as e:
        logger.error(f"Exception in describe_topic: {e}")
        return jsonify({
            'error': 'INTERNAL_ERROR',
            'message': 'Internal server error'
        }), 500


@topic_api.route('/clusters/<cluster_id>/topics/<topic_name>/config', methods=['PUT'])
@topic_write_required
@audit_context
@log_operation("update_topic_config")
@async_route
async def update_topic_config(cluster_id: str, topic_name: str):
    """Update topic configuration."""
    try:
        # Validate parameters
        if not validate_cluster_id(cluster_id):
            return jsonify({
                'error': 'INVALID_CLUSTER_ID',
                'message': 'Invalid cluster ID format'
            }), 400
        
        if not topic_name:
            return jsonify({
                'error': 'INVALID_TOPIC_NAME',
                'message': 'Topic name is required'
            }), 400
        
        # Parse request body
        data = request.get_json()
        if not data or 'configs' not in data:
            return jsonify({
                'error': 'MISSING_CONFIGS',
                'message': 'Configuration updates are required'
            }), 400
        
        configs = data['configs']
        if not isinstance(configs, dict) or not configs:
            return jsonify({
                'error': 'INVALID_CONFIGS',
                'message': 'Configs must be a non-empty dictionary'
            }), 400
        
        # Get topic service and update config
        topic_service = await get_topic_service()
        user_id = get_user_id()
        
        result = await topic_service.update_topic_config(cluster_id, topic_name, configs, user_id)
        
        if result.success:
            return jsonify({
                'success': True,
                'message': result.message,
                'topic_name': result.topic_name,
                'updated_configs': result.details.get('updated_configs', {}) if result.details else {}
            }), 200
        else:
            error_code = result.error_code or 'UPDATE_FAILED'
            status_code = 404 if 'not found' in result.message.lower() else 500
            
            return jsonify({
                'error': error_code,
                'message': result.message,
                'topic_name': result.topic_name
            }), status_code
            
    except Exception as e:
        logger.error(f"Exception in update_topic_config: {e}")
        return jsonify({
            'error': 'INTERNAL_ERROR',
            'message': 'Internal server error'
        }), 500


@topic_api.route('/clusters/<cluster_id>/topics/<topic_name>', methods=['DELETE'])
@topic_write_required
@audit_context
@log_operation("delete_topic")
@async_route
async def delete_topic(cluster_id: str, topic_name: str):
    """Delete a topic from the cluster."""
    try:
        # Validate parameters
        if not validate_cluster_id(cluster_id):
            return jsonify({
                'error': 'INVALID_CLUSTER_ID',
                'message': 'Invalid cluster ID format'
            }), 400
        
        if not topic_name:
            return jsonify({
                'error': 'INVALID_TOPIC_NAME',
                'message': 'Topic name is required'
            }), 400
        
        # Get topic service and delete topic
        topic_service = await get_topic_service()
        user_id = get_user_id()
        
        result = await topic_service.delete_topic(cluster_id, topic_name, user_id)
        
        if result.success:
            return jsonify({
                'success': True,
                'message': result.message,
                'topic_name': result.topic_name
            }), 200
        else:
            error_code = result.error_code or 'DELETION_FAILED'
            status_code = 404 if 'not found' in result.message.lower() else 500
            
            return jsonify({
                'error': error_code,
                'message': result.message,
                'topic_name': result.topic_name
            }), status_code
            
    except Exception as e:
        logger.error(f"Exception in delete_topic: {e}")
        return jsonify({
            'error': 'INTERNAL_ERROR',
            'message': 'Internal server error'
        }), 500


@topic_api.route('/clusters/<cluster_id>/topics/<topic_name>/purge', methods=['POST'])
@topic_write_required
@audit_context
@log_operation("purge_topic")
@async_route
async def purge_topic(cluster_id: str, topic_name: str):
    """Purge messages from a topic."""
    try:
        # Validate parameters
        if not validate_cluster_id(cluster_id):
            return jsonify({
                'error': 'INVALID_CLUSTER_ID',
                'message': 'Invalid cluster ID format'
            }), 400
        
        if not topic_name:
            return jsonify({
                'error': 'INVALID_TOPIC_NAME',
                'message': 'Topic name is required'
            }), 400
        
        # Parse request body (optional)
        data = request.get_json() or {}
        retention_ms = data.get('retention_ms', 1000)
        
        if not isinstance(retention_ms, int) or retention_ms < 1 or retention_ms > 60000:
            return jsonify({
                'error': 'INVALID_RETENTION',
                'message': 'retention_ms must be between 1 and 60000 milliseconds'
            }), 400
        
        # Get topic service and purge topic
        topic_service = await get_topic_service()
        user_id = get_user_id()
        
        result = await topic_service.purge_topic(cluster_id, topic_name, retention_ms, user_id)
        
        if result.success:
            return jsonify({
                'success': True,
                'message': result.message,
                'topic_name': result.topic_name,
                'retention_ms': result.details.get('retention_ms') if result.details else retention_ms
            }), 200
        else:
            error_code = result.error_code or 'PURGE_FAILED'
            status_code = 404 if 'not found' in result.message.lower() else 500
            
            return jsonify({
                'error': error_code,
                'message': result.message,
                'topic_name': result.topic_name
            }), status_code
            
    except Exception as e:
        logger.error(f"Exception in purge_topic: {e}")
        return jsonify({
            'error': 'INTERNAL_ERROR',
            'message': 'Internal server error'
        }), 500


@topic_api.route('/clusters/<cluster_id>/topics/bulk', methods=['POST'])
@topic_write_required
@audit_context
@log_operation("bulk_topic_operations")
@validate_request_data(required_fields=['operations'])
@async_route
async def bulk_topic_operations(cluster_id: str):
    """Perform bulk operations on topics."""
    try:
        # Validate cluster ID
        if not validate_cluster_id(cluster_id):
            return jsonify({
                'error': 'INVALID_CLUSTER_ID',
                'message': 'Invalid cluster ID format'
            }), 400
        
        # Parse request body
        data = request.get_json()
        if not data:
            return jsonify({
                'error': 'MISSING_REQUEST_BODY',
                'message': 'Request body is required'
            }), 400
        
        operation = data.get('operation')
        if operation not in ['create', 'delete']:
            return jsonify({
                'error': 'INVALID_OPERATION',
                'message': 'Operation must be "create" or "delete"'
            }), 400
        
        # Get topic service
        topic_service = await get_topic_service()
        user_id = get_user_id()
        
        if operation == 'create':
            # Bulk create topics
            topics_data = data.get('topics', [])
            if not topics_data:
                return jsonify({
                    'error': 'MISSING_TOPICS',
                    'message': 'Topics list is required for bulk create'
                }), 400
            
            try:
                topic_configs = [TopicConfig(**topic_data) for topic_data in topics_data]
            except Exception as e:
                return jsonify({
                    'error': 'INVALID_TOPIC_CONFIGS',
                    'message': f'Invalid topic configurations: {str(e)}'
                }), 400
            
            results = await topic_service.bulk_create_topics(cluster_id, topic_configs, user_id)
            
        elif operation == 'delete':
            # Bulk delete topics
            topic_names = data.get('topic_names', [])
            if not topic_names:
                return jsonify({
                    'error': 'MISSING_TOPIC_NAMES',
                    'message': 'Topic names list is required for bulk delete'
                }), 400
            
            results = await topic_service.bulk_delete_topics(cluster_id, topic_names, user_id)
        
        # Format response
        successful = []
        failed = []
        
        for topic_name, result in results.items():
            if result.success:
                successful.append({
                    'topic_name': topic_name,
                    'message': result.message
                })
            else:
                failed.append({
                    'topic_name': topic_name,
                    'error': result.error_code or 'OPERATION_FAILED',
                    'message': result.message
                })
        
        return jsonify({
            'operation': operation,
            'cluster_id': cluster_id,
            'total': len(results),
            'successful': len(successful),
            'failed': len(failed),
            'results': {
                'successful': successful,
                'failed': failed
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Exception in bulk_topic_operations: {e}")
        return jsonify({
            'error': 'INTERNAL_ERROR',
            'message': 'Internal server error'
        }), 500


@topic_api.route('/clusters/<cluster_id>/info', methods=['GET'])
@authenticated
@audit_context
@async_route
async def get_cluster_info(cluster_id: str):
    """Get cluster information."""
    try:
        # Validate cluster ID
        if not validate_cluster_id(cluster_id):
            return jsonify({
                'error': 'INVALID_CLUSTER_ID',
                'message': 'Invalid cluster ID format'
            }), 400
        
        # Get topic service and cluster info
        topic_service = await get_topic_service()
        cluster_info = await topic_service.get_cluster_info(cluster_id)
        
        if cluster_info:
            return jsonify({
                'cluster_id': cluster_id,
                'info': cluster_info
            }), 200
        else:
            return jsonify({
                'error': 'CLUSTER_NOT_AVAILABLE',
                'message': f'Cluster {cluster_id} is not available or not running'
            }), 404
            
    except Exception as e:
        logger.error(f"Exception in get_cluster_info: {e}")
        return jsonify({
            'error': 'INTERNAL_ERROR',
            'message': 'Internal server error'
        }), 500


# Health check endpoint for topic management API
@topic_api.route('/health', methods=['GET'])
def topic_api_health():
    """Health check for topic management API."""
    return jsonify({
        'status': 'healthy',
        'service': 'topic-management-api',
        'version': '1.0.0'
    }), 200


# Error handlers
@topic_api.errorhandler(404)
def topic_api_not_found(error):
    """Handle 404 errors."""
    return jsonify({
        'error': 'ENDPOINT_NOT_FOUND',
        'message': 'Topic management endpoint not found'
    }), 404


@topic_api.errorhandler(405)
def topic_api_method_not_allowed(error):
    """Handle 405 errors."""
    return jsonify({
        'error': 'METHOD_NOT_ALLOWED',
        'message': 'HTTP method not allowed for this endpoint'
    }), 405


@topic_api.errorhandler(500)
def topic_api_internal_error(error):
    """Handle 500 errors."""
    return jsonify({
        'error': 'INTERNAL_ERROR',
        'message': 'Internal server error in topic management API'
    }), 500


def create_topic_management_app() -> Flask:
    """Create Flask app with topic management API."""
    app = Flask(__name__)
    
    # Initialize authentication middleware
    from kafka_ops_agent.auth.middleware import AuthMiddleware
    auth_middleware = AuthMiddleware()
    auth_middleware.init_app(app)
    
    # Register blueprint
    app.register_blueprint(topic_api)
    
    # Configure CORS if needed
    from kafka_ops_agent.config import config
    if config.api.enable_cors:
        from flask_cors import CORS
        CORS(app)
    
    return app


def run_topic_management_server():
    """Run the topic management API server."""
    from kafka_ops_agent.config import config
    
    app = create_topic_management_app()
    app.run(
        host=config.api.host,
        port=config.api.port + 1,  # Use different port from main API
        debug=config.api.debug
    )