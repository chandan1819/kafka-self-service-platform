# Complete Kafka Self-Service Platform Guide

## 📋 Table of Contents
1. [Prerequisites](#prerequisites)
2. [Installation & Setup](#installation--setup)
3. [Starting the Platform](#starting-the-platform)
4. [Accessing Control Center](#accessing-control-center)
5. [Creating Topics](#creating-topics)
6. [Producing Messages](#producing-messages)
7. [Consuming Messages](#consuming-messages)
8. [Using Your API](#using-your-api)
9. [Monitoring & Management](#monitoring--management)
10. [Troubleshooting](#troubleshooting)
11. [Stopping Services](#stopping-services)

---

## 🔧 Prerequisites

### Required Software
- **macOS** (this guide is for Mac)
- **Homebrew** package manager
- **Docker Desktop** (we'll install this)
- **Terminal/Command Line** access

### Check if Homebrew is installed
```bash
which brew
# Should return: /opt/homebrew/bin/brew (or similar)
```

If not installed, install Homebrew:
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

---

## 🚀 Installation & Setup

### Step 1: Install Docker Desktop
```bash
# Install Docker Desktop via Homebrew
brew install --cask docker
```

### Step 2: Start Docker Desktop
```bash
# Open Docker Desktop application
open /Applications/Docker.app

# Wait for Docker to start (you'll see whale icon in menu bar)
# Verify Docker is running:
docker --version
docker info
```

### Step 3: Verify Your Project Structure
Make sure you're in your Kafka project directory and have these files:
- `docker-compose.yml`
- `scripts/start_with_control_center.sh`
- `scripts/check_control_center_status.sh`

---

## 🎯 Starting the Platform

### Step 1: Start All Services
```bash
# Make sure script is executable
chmod +x scripts/start_with_control_center.sh

# Start the full Confluent Platform
./scripts/start_with_control_center.sh
```

**Expected Output:**
```
🚀 Starting Kafka Self-Service Platform with Confluent Control Center...
🧹 Cleaning up existing containers...
🔄 Starting services...
⏳ Waiting for services to be healthy...
  - Waiting for Kafka... ✅ Kafka is ready!
  - Waiting for Schema Registry... ✅ Schema Registry is ready!
  - Waiting for Kafka Connect... ✅ Kafka Connect is ready!
  - Waiting for Control Center... ✅ Control Center is ready!

🎉 All services are running!

📊 Access URLs:
  • Confluent Control Center: http://localhost:9021
  • Schema Registry:          http://localhost:8081
  • Kafka Connect:            http://localhost:8083
  • Kafka Ops API:            http://localhost:8000
  • Kafka Ops Monitoring:     http://localhost:8080
```

### Step 2: Verify Services Are Running
```bash
# Check service status
./scripts/check_control_center_status.sh

# Or check Docker containers
docker compose ps
```

---

## 🌐 Accessing Control Center

### Step 1: Open Control Center
**Open your web browser and go to:**
```
http://localhost:9021
```

### Step 2: Explore the Dashboard
You should see:
- **Cluster Overview**: Health metrics and status
- **Topics**: List of available topics
- **Consumers**: Consumer groups and lag metrics
- **Connect**: Kafka Connect connectors
- **Schema Registry**: Avro schema management

### Step 3: Navigate the Interface
- **Left Sidebar**: Main navigation menu
- **Cluster Health**: Real-time metrics
- **Topics Tab**: Topic management interface
- **Consumers Tab**: Consumer monitoring

---

## 📝 Creating Topics

### Method 1: Using Control Center (GUI)

1. **Open Control Center**: http://localhost:9021
2. **Click "Topics"** in the left sidebar
3. **Click "Add Topic"** button
4. **Fill in details:**
   - **Topic Name**: `my-test-topic`
   - **Partitions**: `3`
   - **Replication Factor**: `1`
5. **Click "Create"**

### Method 2: Using Your API
```bash
# Create topic via REST API
curl -X POST \
  -H "Content-Type: application/json" \
  -H "X-API-Key: admin-secret-key" \
  -d '{
    "name": "my-test-topic",
    "partitions": 3,
    "replication_factor": 1
  }' \
  http://localhost:8000/api/v1/topics
```

### Method 3: Using Kafka CLI
```bash
# Create topic using Kafka CLI tools
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic my-test-topic \
  --partitions 3 \
  --replication-factor 1
```

### Verify Topic Creation
```bash
# List all topics
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```

---

## 📤 Producing Messages

### Method 1: Using Kafka Console Producer
```bash
# Start console producer
docker compose exec kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic my-test-topic

# Type messages (press Enter after each):
Hello Kafka!
This is message 2
Testing producer
# Press Ctrl+C to exit
```

### Method 2: Using Control Center (GUI)
1. **Go to Control Center**: http://localhost:9021
2. **Click "Topics"** → **Select your topic**
3. **Click "Messages"** tab
4. **Click "Produce a new message to this topic"**
5. **Enter your message** and click "Produce"

### Method 3: Produce Messages with Key-Value
```bash
# Producer with keys
docker compose exec kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic my-test-topic \
  --property "key.separator=:" \
  --property "parse.key=true"

# Type key:value pairs:
user1:Hello from user1
user2:Hello from user2
order123:Order placed successfully
# Press Ctrl+C to exit
```

### Method 4: Batch Produce Messages
```bash
# Create a file with messages
echo -e "Message 1\nMessage 2\nMessage 3\nMessage 4\nMessage 5" > /tmp/messages.txt

# Produce from file
docker compose exec kafka bash -c "cat /tmp/messages.txt | kafka-console-producer --bootstrap-server localhost:9092 --topic my-test-topic"
```

---

## 📥 Consuming Messages

### Method 1: Using Kafka Console Consumer
```bash
# Start console consumer (from beginning)
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic my-test-topic \
  --from-beginning

# You'll see all messages produced to the topic
# Press Ctrl+C to exit
```

### Method 2: Consumer with Key-Value Display
```bash
# Consumer showing keys and values
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic my-test-topic \
  --from-beginning \
  --property "key.separator=:" \
  --property "print.key=true"
```

### Method 3: Consumer Group
```bash
# Start consumer with consumer group
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic my-test-topic \
  --group my-consumer-group \
  --from-beginning
```

### Method 4: Monitor Consumer Groups
```bash
# List consumer groups
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list

# Describe consumer group
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group my-consumer-group \
  --describe
```

### Method 5: Using Control Center (GUI)
1. **Go to Control Center**: http://localhost:9021
2. **Click "Topics"** → **Select your topic**
3. **Click "Messages"** tab
4. **View messages** in the topic
5. **Use filters** to search specific messages

---

## 🔌 Using Your API

### List Topics
```bash
curl -H "X-API-Key: admin-secret-key" \
  http://localhost:8000/api/v1/topics
```

### Get Topic Details
```bash
curl -H "X-API-Key: admin-secret-key" \
  http://localhost:8000/api/v1/topics/my-test-topic
```

### Update Topic Configuration
```bash
curl -X PUT \
  -H "Content-Type: application/json" \
  -H "X-API-Key: admin-secret-key" \
  -d '{"retention_ms": 604800000}' \
  http://localhost:8000/api/v1/topics/my-test-topic/config
```

### Delete Topic
```bash
curl -X DELETE \
  -H "X-API-Key: admin-secret-key" \
  http://localhost:8000/api/v1/topics/my-test-topic
```

---

## 📊 Monitoring & Management

### Control Center Features

#### 1. Topic Monitoring
- **Throughput**: Messages per second
- **Storage**: Topic size and growth
- **Partitions**: Partition distribution
- **Replication**: Replication status

#### 2. Consumer Monitoring
- **Consumer Lag**: How far behind consumers are
- **Consumer Groups**: Active consumer groups
- **Partition Assignment**: Which consumers handle which partitions

#### 3. Cluster Health
- **Broker Status**: Health of Kafka brokers
- **Resource Usage**: CPU, memory, disk usage
- **Network**: Network throughput metrics

### Command Line Monitoring
```bash
# Topic details
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic my-test-topic

# Consumer group lag
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group my-consumer-group \
  --describe

# Broker information
docker compose exec kafka kafka-broker-api-versions \
  --bootstrap-server localhost:9092
```

### Your Platform Monitoring
```bash
# Health check
curl http://localhost:8080/health

# Metrics
curl http://localhost:8080/metrics

# Service status
curl http://localhost:8000/api/v1/health
```

---

## 🧪 Complete End-to-End Test

### Step 1: Create Test Topic
```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -H "X-API-Key: admin-secret-key" \
  -d '{
    "name": "end-to-end-test",
    "partitions": 3,
    "replication_factor": 1
  }' \
  http://localhost:8000/api/v1/topics
```

### Step 2: Start Consumer (in one terminal)
```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic end-to-end-test \
  --group test-group
```

### Step 3: Produce Messages (in another terminal)
```bash
docker compose exec kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic end-to-end-test

# Type some messages:
Test message 1
Test message 2
Test message 3
```

### Step 4: Verify in Control Center
1. Go to http://localhost:9021
2. Check **Topics** → **end-to-end-test**
3. Check **Consumers** → **test-group**
4. Monitor message flow and consumer lag

---

## 🛠️ Troubleshooting

### Services Not Starting
```bash
# Check Docker is running
docker info

# Check container logs
docker compose logs kafka
docker compose logs control-center

# Restart specific service
docker compose restart kafka
```

### Control Center Not Loading
```bash
# Check Control Center logs
docker compose logs -f control-center

# Verify port is not in use
lsof -i :9021

# Wait longer (Control Center takes 2-3 minutes to start)
```

### Producer/Consumer Issues
```bash
# Test Kafka connectivity
docker compose exec kafka kafka-broker-api-versions \
  --bootstrap-server localhost:9092

# Check topic exists
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --list

# Verify topic configuration
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic your-topic-name
```

### API Issues
```bash
# Check API health
curl http://localhost:8000/api/v1/health

# Check API logs
docker compose logs kafka-ops-api

# Verify API key
curl -H "X-API-Key: admin-secret-key" \
  http://localhost:8000/api/v1/topics
```

---

## 🛑 Stopping Services

### Stop All Services
```bash
# Stop containers (keeps data)
docker compose down

# Stop and remove data volumes
docker compose down -v
```

### Stop Individual Services
```bash
# Stop specific service
docker compose stop control-center

# Restart specific service
docker compose restart kafka
```

### Check Status
```bash
# Check what's running
docker compose ps

# Check system resources
docker system df
```

---

## 🎯 Quick Reference Commands

### Essential Commands
```bash
# Start everything
./scripts/start_with_control_center.sh

# Check status
./scripts/check_control_center_status.sh

# Stop everything
docker compose down

# View logs
docker compose logs -f [service-name]
```

### Topic Operations
```bash
# List topics
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Create topic
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic TOPIC_NAME --partitions 3 --replication-factor 1

# Delete topic
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic TOPIC_NAME
```

### Producer/Consumer
```bash
# Producer
docker compose exec kafka kafka-console-producer --bootstrap-server localhost:9092 --topic TOPIC_NAME

# Consumer
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic TOPIC_NAME --from-beginning
```

### Access URLs
- **Control Center**: http://localhost:9021
- **Your API**: http://localhost:8000
- **Monitoring**: http://localhost:8080
- **Schema Registry**: http://localhost:8081
- **Kafka Connect**: http://localhost:8083

---

## 🎉 Success Checklist

- [ ] Docker Desktop installed and running
- [ ] All services started successfully
- [ ] Control Center accessible at http://localhost:9021
- [ ] Topic created successfully
- [ ] Messages produced and consumed
- [ ] Consumer groups working
- [ ] API endpoints responding
- [ ] Monitoring data visible

**Congratulations! You now have a fully functional Kafka Self-Service Platform with Control Center!**

---

## 📚 Next Steps

1. **Explore Control Center**: Try different features and dashboards
2. **Test Your API**: Use all the endpoints in your platform
3. **Create Complex Workflows**: Multi-topic, multi-consumer scenarios
4. **Monitor Performance**: Use the monitoring tools
5. **Develop Applications**: Build apps that use your Kafka cluster
6. **Scale Up**: Add more brokers or partitions as needed

For more advanced topics, check the other documentation files in the `docs/` directory.