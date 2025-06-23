#!/bin/bash

echo "Starting Real-Time Logistics Pipeline..."

# Start Docker services
docker-compose up -d

# Wait for services to be ready
sleep 30

# Create Kafka topics
docker exec $(docker ps -q -f name=kafka) kafka-topics --create --topic driver_locations --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3
docker exec $(docker ps -q -f name=kafka) kafka-topics --create --topic delivery_status --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3
docker exec $(docker ps -q -f name=kafka) kafka-topics --create --topic traffic_conditions --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3

echo "Pipeline started successfully!"
echo "Access dashboard at: http://localhost:8501"