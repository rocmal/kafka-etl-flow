#!/bin/bash

echo "🏥 Checking service health..."
echo ""

# Check Kafka
echo "Kafka:"
if docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    echo "  ✓ Healthy"
else
    echo "  ✗ Not responding"
fi

# Check Elasticsearch
echo "Elasticsearch:"
if curl -s http://localhost:9200/_cluster/health > /dev/null 2>&1; then
    echo "  ✓ Healthy"
else
    echo "  ✗ Not responding"
fi

# Check Airflow
echo "Airflow:"
if curl -s http://localhost:8081/health > /dev/null 2>&1; then
    echo "  ✓ Healthy"
else
    echo "  ✗ Not responding"
fi

# Check Producer
echo "Producer:"
if curl -s http://localhost:3001/health > /dev/null 2>&1; then
    echo "  ✓ Healthy"
else
    echo "  ✗ Not responding"
fi

# Check Consumer
echo "Consumer:"
if curl -s http://localhost:3002/health > /dev/null 2>&1; then
    echo "  ✓ Healthy"
else
    echo "  ✗ Not responding"
fi

echo ""
echo "Docker containers status:"
docker-compose ps
