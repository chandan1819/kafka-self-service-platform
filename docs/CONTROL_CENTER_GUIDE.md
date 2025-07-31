# Confluent Control Center Access Guide

## 🚀 Quick Start

To access Confluent Control Center with your Kafka cluster:

### 1. Start the Full Platform

```bash
# Start everything including Control Center
./scripts/start_with_control_center.sh
```

### 2. Access Control Center

Once all services are running, open your browser and go to:

**http://localhost:9021**

## 📊 What You'll See in Control Center

### Dashboard Overview
- **Cluster Health**: Real-time cluster metrics and health status
- **Throughput**: Message production and consumption rates
- **Topics**: List of all topics with partition and replication info
- **Consumers**: Active consumer groups and their lag metrics

### Key Features

#### 1. **Topics Management**
- Create, configure, and delete topics
- View topic configurations and partition details
- Monitor message throughput per topic

#### 2. **Consumer Monitoring**
- Track consumer group lag
- Monitor consumer performance
- View consumer group details and offsets

#### 3. **Connect Management**
- Manage Kafka Connect connectors
- Monitor connector status and throughput
- Create and configure new connectors

#### 4. **Schema Registry**
- View and manage Avro schemas
- Schema evolution and compatibility
- Schema usage across topics

#### 5. **Cluster Monitoring**
- Broker performance metrics
- Partition distribution
- Replication status

## 🔧 Integration with Your Kafka Ops Platform

Your Kafka Self-Service Platform API runs alongside Control Center:

- **Kafka Ops API**: http://localhost:8000
- **Monitoring Endpoint**: http://localhost:8080
- **Control Center**: http://localhost:9021

### Test Integration

```bash
# Create a topic via your API
curl -X POST \
  -H "Content-Type: application/json" \
  -H "X-API-Key: admin-secret-key" \
  -d '{"name":"test-topic","partitions":3,"replication_factor":1}' \
  http://localhost:8000/api/v1/topics

# View it in Control Center
# Go to http://localhost:9021 → Topics → test-topic
```

## 🛠️ Troubleshooting

### Control Center Not Loading

1. **Check if all services are running:**
   ```bash
   ./scripts/check_control_center_status.sh
   ```

2. **View Control Center logs:**
   ```bash
   docker compose logs -f control-center
   ```

3. **Restart Control Center:**
   ```bash
   docker compose restart control-center
   ```

### Common Issues

#### "Cluster not found" Error
- Wait 2-3 minutes after startup for Control Center to discover the cluster
- Check that Kafka metrics are being published

#### Slow Loading
- Control Center can take 2-3 minutes to fully initialize
- Check available system resources (RAM/CPU)

#### Connection Issues
- Ensure all services are healthy: `docker compose ps`
- Check network connectivity between containers

## 📈 Monitoring Your Kafka Ops Platform

### View Platform Metrics in Control Center

1. **Topics Created by Platform**: Look for topics with your naming patterns
2. **Consumer Groups**: Monitor groups created by your platform
3. **Message Flow**: Track messages produced/consumed by your services

### Custom Dashboards

Control Center allows you to create custom dashboards to monitor:
- Your platform's topic creation patterns
- Consumer lag for platform-managed topics
- Throughput metrics for automated operations

## 🔄 Alternative Access Methods

### If Control Center is not available:

1. **Kafka CLI Tools:**
   ```bash
   # List topics
   docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
   
   # Describe topic
   docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic your-topic
   
   # Consumer groups
   docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
   ```

2. **Your Platform's Monitoring:**
   ```bash
   # Health check
   curl http://localhost:8080/health
   
   # Metrics
   curl http://localhost:8080/metrics
   ```

3. **Third-party Tools:**
   - Kafka Tool (GUI)
   - Conduktor (Commercial)
   - Kafdrop (Web UI)

## 🚦 Service Dependencies

Control Center requires these services to be running:
- ✅ Zookeeper
- ✅ Kafka Broker
- ✅ Schema Registry
- ✅ Kafka Connect

Your platform works independently but integrates with all of them.

## 📝 Next Steps

1. **Explore Topics**: Create topics via your API and view them in Control Center
2. **Monitor Consumers**: Set up consumer groups and track their performance
3. **Configure Alerts**: Set up monitoring alerts for your platform
4. **Schema Management**: Use Schema Registry for structured data

---

**Need Help?** 
- Check service status: `./scripts/check_control_center_status.sh`
- View logs: `docker compose logs -f [service-name]`
- Restart services: `docker compose restart [service-name]`