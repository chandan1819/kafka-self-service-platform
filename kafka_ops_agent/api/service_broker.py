"""Open Service Broker API implementation."""

import logging
from typing import Dict, Any, Optional
from flask import Flask, request, jsonify, g
from functools import wraps
import asyncio
from concurrent.futures import ThreadPoolExecutor

from kafka_ops_agent.auth.middleware import AuthMiddleware
from kafka_ops_agent.auth.decorators import (
    service_broker_required, authenticated, audit_context, log_operation
)
from kafka_ops_agent.auth.models import Permission

from kafka_ops_agent.services.provisioning import ProvisioningService
from kafka_ops_agent.storage.factory import get_metadata_store, get_audit_store
from kafka_ops_agent.models.service_broker import (
    Catalog, ProvisionRequest, ProvisionResponse, UpdateRequest, UpdateResponse,
    DeprovisionResponse, LastOperationResponse, ErrorResponse
)
from kafka_ops_agent.models.factory import ServiceBrokerFactory
from kafka_ops_agent.providers.base import ProvisioningStatus
from kafka_ops_agent.config import config

logger = logging.getLogger(__name__)

# Global provisioning service instance
_provisioning_service: Optional[ProvisioningService] = None
_executor = ThreadPoolExecutor(max_workers=4)


def get_provisioning_service() -> ProvisioningService:
    """Get or create provisioning service instance."""
    global _provisioning_service
    if _provisioning_service is None:
        # This will be initialized when the app starts
        raise RuntimeError("Provisioning service not initialized")
    return _provisioning_service


async def initialize_provisioning_service():
    """Initialize the provisioning service."""
    global _provisioning_service
    if _provisioning_service is None:
        metadata_store = await get_metadata_store()
        audit_store = await get_audit_store()
        _provisioning_service = ProvisioningService(metadata_store, audit_store)
        logger.info("Provisioning service initialized")


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


# Authentication is now handled by middleware - keeping this for backward compatibility
def require_auth(f):
    """Legacy auth decorator - now handled by middleware."""
    return authenticated(f)


def validate_service_id(service_id: str) -> bool:
    """Validate service ID."""
    return service_id == "kafka-service"


def validate_plan_id(plan_id: str) -> bool:
    """Validate plan ID."""
    valid_plans = ["basic", "standard", "premium"]
    return plan_id in valid_plans


def create_app() -> Flask:
    """Create Flask application with OSB API routes."""
    app = Flask(__name__)
    
    # Initialize authentication middleware
    from kafka_ops_agent.auth.middleware import AuthMiddleware
    auth_middleware = AuthMiddleware()
    auth_middleware.init_app(app)
    
    # Configure CORS if enabled
    if config.api.enable_cors:
        from flask_cors import CORS
        CORS(app)
    
    @app.before_first_request
    def initialize():
        """Initialize services before first request."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(initialize_provisioning_service())
        finally:
            loop.close()
    
    @app.route('/v2/catalog', methods=['GET'])
    def get_catalog():  # Catalog endpoint is typically public
        """Get service catalog."""
        try:
            catalog = ServiceBrokerFactory.create_catalog()
            return jsonify(catalog.dict())
        except Exception as e:
            logger.error(f"Failed to get catalog: {e}")
            return jsonify(ErrorResponse(
                error="InternalError",
                description="Failed to retrieve service catalog"
            ).dict()), 500
    
    @app.route('/v2/service_instances/<instance_id>', methods=['PUT'])
    @service_broker_required
    @audit_context
    @log_operation("provision_service_instance")
    @async_route
    async def provision_service_instance(instance_id: str):
        """Provision a service instance."""
        try:
            # Parse request
            data = request.get_json()
            if not data:
                return jsonify(ErrorResponse(
                    error="BadRequest",
                    description="Request body is required"
                ).dict()), 400
            
            try:
                provision_request = ProvisionRequest(**data)
            except Exception as e:
                return jsonify(ErrorResponse(
                    error="BadRequest",
                    description=f"Invalid request format: {str(e)}"
                ).dict()), 400
            
            # Validate service and plan IDs
            if not validate_service_id(provision_request.service_id):
                return jsonify(ErrorResponse(
                    error="BadRequest",
                    description="Invalid service ID"
                ).dict()), 400
            
            if not validate_plan_id(provision_request.plan_id):
                return jsonify(ErrorResponse(
                    error="BadRequest",
                    description="Invalid plan ID"
                ).dict()), 400
            
            # Get user ID from headers (if available)
            user_id = request.headers.get('X-Broker-API-Originating-Identity')
            
            # Provision cluster
            provisioning_service = get_provisioning_service()
            result = await provisioning_service.provision_cluster(
                instance_id=instance_id,
                service_id=provision_request.service_id,
                plan_id=provision_request.plan_id,
                organization_guid=provision_request.organization_guid,
                space_guid=provision_request.space_guid,
                parameters=provision_request.parameters,
                user_id=user_id
            )
            
            if result.status == ProvisioningStatus.SUCCEEDED:
                response = ProvisionResponse()
                return jsonify(response.dict()), 201
            elif result.status == ProvisioningStatus.IN_PROGRESS:
                response = ProvisionResponse(operation=result.operation_id)
                return jsonify(response.dict()), 202
            else:
                # Check if it's a conflict (instance already exists)
                if "already exists" in (result.error_message or ""):
                    return jsonify(ErrorResponse(
                        error="Conflict",
                        description="Service instance already exists"
                    ).dict()), 409
                
                return jsonify(ErrorResponse(
                    error="InternalError",
                    description=result.error_message or "Provisioning failed"
                ).dict()), 500
                
        except Exception as e:
            logger.error(f"Provision request failed: {e}")
            return jsonify(ErrorResponse(
                error="InternalError",
                description="Internal server error"
            ).dict()), 500
    
    @app.route('/v2/service_instances/<instance_id>', methods=['PATCH'])
    @service_broker_required
    @audit_context
    @log_operation("update_service_instance")
    @async_route
    async def update_service_instance(instance_id: str):
        """Update a service instance."""
        try:
            # Parse request
            data = request.get_json()
            if not data:
                return jsonify(ErrorResponse(
                    error="BadRequest",
                    description="Request body is required"
                ).dict()), 400
            
            try:
                update_request = UpdateRequest(**data)
            except Exception as e:
                return jsonify(ErrorResponse(
                    error="BadRequest",
                    description=f"Invalid request format: {str(e)}"
                ).dict()), 400
            
            # For now, we don't support updates
            return jsonify(ErrorResponse(
                error="NotSupported",
                description="Service instance updates are not supported"
            ).dict()), 422
            
        except Exception as e:
            logger.error(f"Update request failed: {e}")
            return jsonify(ErrorResponse(
                error="InternalError",
                description="Internal server error"
            ).dict()), 500
    
    @app.route('/v2/service_instances/<instance_id>', methods=['DELETE'])
    @service_broker_required
    @audit_context
    @log_operation("deprovision_service_instance")
    @async_route
    async def deprovision_service_instance(instance_id: str):
        """Deprovision a service instance."""
        try:
            # Get query parameters
            service_id = request.args.get('service_id')
            plan_id = request.args.get('plan_id')
            
            if not service_id or not validate_service_id(service_id):
                return jsonify(ErrorResponse(
                    error="BadRequest",
                    description="Invalid or missing service_id parameter"
                ).dict()), 400
            
            if not plan_id or not validate_plan_id(plan_id):
                return jsonify(ErrorResponse(
                    error="BadRequest",
                    description="Invalid or missing plan_id parameter"
                ).dict()), 400
            
            # Get user ID from headers
            user_id = request.headers.get('X-Broker-API-Originating-Identity')
            
            # Deprovision cluster
            provisioning_service = get_provisioning_service()
            result = await provisioning_service.deprovision_cluster(instance_id, user_id)
            
            if result.status == ProvisioningStatus.SUCCEEDED:
                response = DeprovisionResponse()
                return jsonify(response.dict()), 200
            elif result.status == ProvisioningStatus.IN_PROGRESS:
                response = DeprovisionResponse(operation=result.operation_id)
                return jsonify(response.dict()), 202
            else:
                # Check if instance doesn't exist
                if "not found" in (result.error_message or ""):
                    return jsonify(ErrorResponse(
                        error="Gone",
                        description="Service instance does not exist"
                    ).dict()), 410
                
                return jsonify(ErrorResponse(
                    error="InternalError",
                    description=result.error_message or "Deprovisioning failed"
                ).dict()), 500
                
        except Exception as e:
            logger.error(f"Deprovision request failed: {e}")
            return jsonify(ErrorResponse(
                error="InternalError",
                description="Internal server error"
            ).dict()), 500
    
    @app.route('/v2/service_instances/<instance_id>/last_operation', methods=['GET'])
    @service_broker_required
    @audit_context
    @async_route
    async def get_last_operation(instance_id: str):
        """Get the status of the last operation."""
        try:
            # Get query parameters
            service_id = request.args.get('service_id')
            plan_id = request.args.get('plan_id')
            operation = request.args.get('operation')
            
            # Get cluster status
            provisioning_service = get_provisioning_service()
            status = await provisioning_service.get_cluster_status(instance_id)
            
            if status is None:
                return jsonify(ErrorResponse(
                    error="Gone",
                    description="Service instance does not exist"
                ).dict()), 410
            
            # Map cluster status to operation state
            if status.value == "running":
                state = "succeeded"
                description = "Service instance is running"
            elif status.value in ["creating", "stopping"]:
                state = "in progress"
                description = f"Service instance is {status.value}"
            elif status.value == "error":
                state = "failed"
                description = "Service instance failed"
            else:
                state = "in progress"
                description = f"Service instance status: {status.value}"
            
            response = LastOperationResponse(
                state=state,
                description=description
            )
            
            return jsonify(response.dict()), 200
            
        except Exception as e:
            logger.error(f"Last operation request failed: {e}")
            return jsonify(ErrorResponse(
                error="InternalError",
                description="Internal server error"
            ).dict()), 500
    
    @app.route('/v2/service_instances/<instance_id>/service_bindings/<binding_id>', methods=['PUT'])
    @service_broker_required
    @audit_context
    @log_operation("create_service_binding")
    def create_service_binding(instance_id: str, binding_id: str):
        """Create a service binding (not implemented)."""
        return jsonify(ErrorResponse(
            error="NotSupported",
            description="Service bindings are not supported"
        ).dict()), 422
    
    @app.route('/v2/service_instances/<instance_id>/service_bindings/<binding_id>', methods=['DELETE'])
    @service_broker_required
    @audit_context
    @log_operation("delete_service_binding")
    def delete_service_binding(instance_id: str, binding_id: str):
        """Delete a service binding (not implemented)."""
        return jsonify(ErrorResponse(
            error="NotSupported",
            description="Service bindings are not supported"
        ).dict()), 422
    
    # Health check endpoint
    @app.route('/health', methods=['GET'])
    def health_check():
        """Health check endpoint."""
        return jsonify({
            "status": "healthy",
            "service": "kafka-ops-agent",
            "version": "0.1.0"
        }), 200
    
    # Error handlers
    @app.errorhandler(404)
    def not_found(error):
        return jsonify(ErrorResponse(
            error="NotFound",
            description="Endpoint not found"
        ).dict()), 404
    
    @app.errorhandler(405)
    def method_not_allowed(error):
        return jsonify(ErrorResponse(
            error="MethodNotAllowed",
            description="Method not allowed for this endpoint"
        ).dict()), 405
    
    @app.errorhandler(500)
    def internal_error(error):
        return jsonify(ErrorResponse(
            error="InternalError",
            description="Internal server error"
        ).dict()), 500
    
    return app


def run_server():
    """Run the Flask server."""
    app = create_app()
    app.run(
        host=config.api.host,
        port=config.api.port,
        debug=config.api.debug
    )