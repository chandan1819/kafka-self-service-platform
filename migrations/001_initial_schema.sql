-- Initial schema for Kafka Ops Agent
-- Migration: 001_initial_schema
-- Created: 2024-01-01
-- Description: Create initial database schema for service instances, topics, and audit logs

BEGIN;

-- Create extension for UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create enum types
CREATE TYPE instance_state AS ENUM ('in_progress', 'succeeded', 'failed');
CREATE TYPE provider_type AS ENUM ('docker', 'kubernetes', 'terraform');
CREATE TYPE operation_type AS ENUM ('provision', 'deprovision', 'update', 'backup', 'restore');

-- Service instances table
CREATE TABLE service_instances (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    instance_id VARCHAR(255) UNIQUE NOT NULL,
    service_id VARCHAR(255) NOT NULL,
    plan_id VARCHAR(255) NOT NULL,
    organization_guid UUID,
    space_guid UUID,
    parameters JSONB DEFAULT '{}',
    context JSONB DEFAULT '{}',
    provider provider_type NOT NULL,
    state instance_state NOT NULL DEFAULT 'in_progress',
    dashboard_url TEXT,
    connection_info JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE
);

-- Operations table for tracking async operations
CREATE TABLE operations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    operation_id VARCHAR(255) UNIQUE NOT NULL,
    instance_id UUID NOT NULL REFERENCES service_instances(id) ON DELETE CASCADE,
    type operation_type NOT NULL,
    state instance_state NOT NULL DEFAULT 'in_progress',
    description TEXT,
    result JSONB DEFAULT '{}',
    error_message TEXT,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Topics table for topic management
CREATE TABLE topics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) UNIQUE NOT NULL,
    instance_id UUID REFERENCES service_instances(id) ON DELETE CASCADE,
    partitions INTEGER NOT NULL DEFAULT 1,
    replication_factor INTEGER NOT NULL DEFAULT 1,
    config JSONB DEFAULT '{}',
    state VARCHAR(50) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE
);

-- Audit logs table
CREATE TABLE audit_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_type VARCHAR(100) NOT NULL,
    resource_type VARCHAR(100) NOT NULL,
    resource_id VARCHAR(255),
    user_id VARCHAR(255),
    user_agent TEXT,
    ip_address INET,
    action VARCHAR(100) NOT NULL,
    details JSONB DEFAULT '{}',
    result VARCHAR(50) NOT NULL,
    error_message TEXT,
    duration_ms INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Cleanup jobs table
CREATE TABLE cleanup_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_type VARCHAR(100) NOT NULL,
    resource_type VARCHAR(100) NOT NULL,
    resource_id VARCHAR(255),
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL,
    executed_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    state VARCHAR(50) NOT NULL DEFAULT 'scheduled',
    result JSONB DEFAULT '{}',
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Configuration table for runtime configuration
CREATE TABLE configurations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    key VARCHAR(255) UNIQUE NOT NULL,
    value JSONB NOT NULL,
    description TEXT,
    is_sensitive BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX idx_service_instances_instance_id ON service_instances(instance_id);
CREATE INDEX idx_service_instances_state ON service_instances(state);
CREATE INDEX idx_service_instances_provider ON service_instances(provider);
CREATE INDEX idx_service_instances_created_at ON service_instances(created_at);
CREATE INDEX idx_service_instances_deleted_at ON service_instances(deleted_at) WHERE deleted_at IS NULL;

CREATE INDEX idx_operations_operation_id ON operations(operation_id);
CREATE INDEX idx_operations_instance_id ON operations(instance_id);
CREATE INDEX idx_operations_state ON operations(state);
CREATE INDEX idx_operations_type ON operations(type);
CREATE INDEX idx_operations_started_at ON operations(started_at);

CREATE INDEX idx_topics_name ON topics(name);
CREATE INDEX idx_topics_instance_id ON topics(instance_id);
CREATE INDEX idx_topics_state ON topics(state);
CREATE INDEX idx_topics_created_at ON topics(created_at);
CREATE INDEX idx_topics_deleted_at ON topics(deleted_at) WHERE deleted_at IS NULL;

CREATE INDEX idx_audit_logs_event_type ON audit_logs(event_type);
CREATE INDEX idx_audit_logs_resource_type ON audit_logs(resource_type);
CREATE INDEX idx_audit_logs_resource_id ON audit_logs(resource_id);
CREATE INDEX idx_audit_logs_user_id ON audit_logs(user_id);
CREATE INDEX idx_audit_logs_action ON audit_logs(action);
CREATE INDEX idx_audit_logs_result ON audit_logs(result);
CREATE INDEX idx_audit_logs_created_at ON audit_logs(created_at);

CREATE INDEX idx_cleanup_jobs_job_type ON cleanup_jobs(job_type);
CREATE INDEX idx_cleanup_jobs_resource_type ON cleanup_jobs(resource_type);
CREATE INDEX idx_cleanup_jobs_scheduled_at ON cleanup_jobs(scheduled_at);
CREATE INDEX idx_cleanup_jobs_state ON cleanup_jobs(state);

CREATE INDEX idx_configurations_key ON configurations(key);

-- Create triggers for updated_at timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_service_instances_updated_at 
    BEFORE UPDATE ON service_instances 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_operations_updated_at 
    BEFORE UPDATE ON operations 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_topics_updated_at 
    BEFORE UPDATE ON topics 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_cleanup_jobs_updated_at 
    BEFORE UPDATE ON cleanup_jobs 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_configurations_updated_at 
    BEFORE UPDATE ON configurations 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert default configurations
INSERT INTO configurations (key, value, description) VALUES
    ('cleanup.default_retention_days', '7', 'Default retention period for resources in days'),
    ('cleanup.orphaned_check_interval', '3600', 'Interval for checking orphaned resources in seconds'),
    ('api.rate_limit_per_minute', '1000', 'API rate limit per minute per client'),
    ('monitoring.metrics_retention_hours', '24', 'Metrics retention period in hours'),
    ('alerts.webhook_timeout_seconds', '30', 'Webhook timeout for alerts in seconds'),
    ('providers.docker.enabled', 'true', 'Enable Docker provider'),
    ('providers.kubernetes.enabled', 'true', 'Enable Kubernetes provider'),
    ('providers.terraform.enabled', 'true', 'Enable Terraform provider'),
    ('features.audit_logging', 'true', 'Enable audit logging'),
    ('features.metrics_collection', 'true', 'Enable metrics collection'),
    ('features.health_checks', 'true', 'Enable health checks'),
    ('features.cleanup_scheduler', 'true', 'Enable cleanup scheduler');

-- Create views for common queries
CREATE VIEW active_service_instances AS
SELECT * FROM service_instances 
WHERE deleted_at IS NULL;

CREATE VIEW active_topics AS
SELECT * FROM topics 
WHERE deleted_at IS NULL AND state = 'active';

CREATE VIEW pending_operations AS
SELECT * FROM operations 
WHERE state = 'in_progress';

CREATE VIEW recent_audit_logs AS
SELECT * FROM audit_logs 
WHERE created_at >= NOW() - INTERVAL '24 hours'
ORDER BY created_at DESC;

CREATE VIEW scheduled_cleanup_jobs AS
SELECT * FROM cleanup_jobs 
WHERE state = 'scheduled' AND scheduled_at <= NOW()
ORDER BY scheduled_at ASC;

-- Grant permissions (adjust as needed for your setup)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO kafka_ops_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO kafka_ops_user;

COMMIT;