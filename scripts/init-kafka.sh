#!/bin/bash

echo "🔧 Initializing Kafka topics..."

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
sleep 30

# Create orders topic
echo "Creating 'orders' topic..."
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists \
  --config retention.ms=604800000

# Create order-analytics topic
echo "Creating 'order-analytics' topic..."
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic order-analytics \
  --partitions 2 \
  --replication-factor 1 \
  --if-not-exists

# Create DLQ topic
echo "Creating 'orders-dlq' topic..."
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic orders-dlq \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

echo ""
echo "✅ Topics created successfully!"
echo ""
echo "📋 All Kafka topics:"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

echo ""
echo "📊 Topic details:"
docker exec kafka kafka-topics --describe --bootstrap-server localhost:9092

echo ""
echo "✅ Kafka initialization complete!"
