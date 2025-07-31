# 🚀 Kafka Self-Service Platform

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Docker](https://img.shields.io/badge/docker-ready-blue.svg)](https://www.docker.com/)

An AI-powered automation system for Apache Kafka cluster provisioning and lifecycle management with **Confluent Control Center** integration.

## ✨ Features

- 🎯 **One-Click Kafka Setup** - Complete Kafka cluster with Control Center in minutes
- 🌐 **Confluent Control Center** - Full web UI for cluster management and monitoring
- 🔌 **Open Service Broker API** - Standard cloud platform integration
- 🐳 **Multi-Runtime Support** - Docker, Kubernetes, Terraform deployments
- 📊 **Topic Management** - Complete lifecycle management via API and CLI
- 🔄 **Producer/Consumer Tools** - Interactive scripts for testing
- 📈 **Built-in Monitoring** - Health checks, metrics, and audit trails
- 🛠️ **Developer Friendly** - Comprehensive documentation and examples

## 🎯 Quick Start

### Prerequisites
- **macOS** (this guide is for Mac)
- **Docker Desktop** (we'll help you install it)
- **Homebrew** package manager

### 🚀 Get Started in 3 Steps

1. **Install Docker Desktop:**
   ```bash
   brew install --cask docker
   open /Applications/Docker.app  # Start Docker Desktop
   ```

2. **Start the Platform:**
   ```bash
   ./scripts/start_with_control_center.sh
   ```

3. **Access Control Center:**
   Open your browser to **http://localhost:9021**

That's it! You now have a full Kafka cluster with Control Center running locally.

## 🎮 Interactive Demo

Run the complete demo to see everything in action:

```bash
./scripts/quick_demo.sh
```

This will:
- ✅ Start all services
- ✅ Create demo topics
- ✅ Produce sample messages
- ✅ Consume messages
- ✅ Test the API
- ✅ Show you all access URLs

## 🛠️ Easy Topic Operations

Use the interactive topic management script:

```bash
# List all topics
./scripts/topic_operations.sh list

# Create a topic
./scripts/topic_operations.sh create my-topic 5

# Start interactive producer
./scripts/topic_operations.sh produce my-topic

# Start interactive consumer
./scripts/topic_operations.sh consume my-topic
```

## 🌐 What You Get

When you start the platform, you get all these services running locally:

| Service | URL | Description |
|---------|-----|-------------|
| **Control Center** | http://localhost:9021 | 🎯 Main Kafka management UI |
| **Kafka Broker** | localhost:9092 | 📡 Kafka cluster |
| **Schema Registry** | http://localhost:8081 | 📋 Schema management |
| **Kafka Connect** | http://localhost:8083 | 🔌 Data integration |
| **Your API** | http://localhost:8000 | 🚀 Self-service API |
| **Monitoring** | http://localhost:8080 | 📊 Health & metrics |

## 📚 Documentation

- **[Complete Setup Guide](docs/COMPLETE_KAFKA_GUIDE.md)** - Step-by-step instructions for everything
- **[Control Center Guide](docs/CONTROL_CENTER_GUIDE.md)** - How to use the web UI
- **[Deployment Guide](docs/DEPLOYMENT.md)** - Production deployment options

## 🔌 API Examples

### Create a Topic
```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -H "X-API-Key: admin-secret-key" \
  -d '{"name":"my-topic","partitions":3,"replication_factor":1}' \
  http://localhost:8000/api/v1/topics
```

### List Topics
```bash
curl -H "X-API-Key: admin-secret-key" \
  http://localhost:8000/api/v1/topics
```

### Provision Kafka Cluster (OSB API)
```bash
curl -X PUT \
  -H "Content-Type: application/json" \
  -d '{"service_id":"kafka-service","plan_id":"standard"}' \
  http://localhost:8000/v2/service_instances/my-cluster
```

## 🧪 Producer/Consumer Examples

### Quick Producer Test
```bash
# Start interactive producer
./scripts/interactive_producer.sh my-topic

# Type messages and press Enter to send
# Press Ctrl+C to exit
```

### Quick Consumer Test
```bash
# Start interactive consumer
./scripts/interactive_consumer.sh my-topic

# You'll see messages as they arrive
# Press Ctrl+C to exit
```

### CLI Producer/Consumer
```bash
# Producer
docker compose exec kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic my-topic

# Consumer
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --from-beginning
```

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Control       │    │   Your Kafka    │    │   Schema        │
│   Center        │    │   Ops API       │    │   Registry      │
│   :9021         │    │   :8000         │    │   :8081         │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   Kafka Broker  │
                    │   :9092         │
                    └─────────────────┘
```

## 🛑 Stop Services

```bash
# Stop all services
docker compose down

# Stop and remove all data
docker compose down -v
```

## 🔧 Development

### Run Tests
```bash
# Unit tests
python -m pytest tests/

# Integration tests
python -m pytest tests/integration/

# With coverage
python -m pytest --cov=kafka_ops_agent tests/
```

### Development Mode
```bash
# Install dependencies
pip install -r requirements-dev.txt

# Run in development mode
python -m kafka_ops_agent.api
```

## 🚀 Deployment Options

### Docker (Recommended for local development)
```bash
docker compose up -d
```

### Kubernetes
```bash
kubectl apply -f k8s/
# or
helm install kafka-ops-platform helm/kafka-ops-agent/
```

### Cloud Platforms
- AWS ECS/EKS
- Google Cloud Run/GKE  
- Azure Container Instances/AKS

## 📊 Monitoring & Observability

- **Health Checks**: `/health`, `/ready`
- **Metrics**: Prometheus-compatible metrics at `/metrics`
- **Audit Logs**: Structured logging for all operations
- **Control Center**: Real-time cluster monitoring
- **Custom Dashboards**: Grafana integration ready

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- 📖 **Documentation**: Check the `docs/` directory
- 🐛 **Issues**: Open an issue on GitHub
- 💬 **Discussions**: Use GitHub Discussions for questions
- 📧 **Contact**: Open an issue for support requests

## ⭐ Star History

If this project helps you, please consider giving it a star! ⭐

---

**Made with ❤️ for the Kafka community**