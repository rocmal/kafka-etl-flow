# Architecture Documentation

## System Overview

kafka-etl-flow is a modern data pipeline built on:
- **Apache Kafka (KRaft)**: Event streaming
- **Node.js**: Producer/Consumer applications
- **Apache Airflow**: ETL orchestration
- **Elasticsearch**: Data storage
- **Prometheus + Grafana**: Monitoring

## Component Interaction

[Detailed architecture diagrams and explanations]

## Data Flow

1. Producer generates events
2. Kafka stores and distributes
3. Consumer processes and indexes
4. Airflow runs analytics
5. Monitoring tracks metrics

## Scalability Considerations

- Horizontal scaling of consumers
- Kafka partition strategy
- Elasticsearch sharding
- Airflow worker pools
