#!/bin/bash

echo "🧹 Cleaning up kafka-etl-flow..."

# Stop all containers
echo "Stopping containers..."
docker-compose down

# Remove volumes
read -p "Remove all data volumes? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Removing volumes..."
    docker-compose down -v
fi

# Remove Docker images
read -p "Remove Docker images? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Removing images..."
    docker-compose down --rmi all
fi

echo "✅ Cleanup complete!"
