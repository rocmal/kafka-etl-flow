"""
Kafka Health Monitoring DAG
Monitors Kafka cluster health, topic metrics, and consumer group lag
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic
from elasticsearch import Elasticsearch
import logging

default_args = {
    'owner': 'platform-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['alerts@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'kafka_health_monitoring',
    default_args=default_args,
    description='Monitor Kafka cluster health and metrics',
    schedule_interval='*/10 * * * *',  # Every 10 minutes
    start_date=days_ago(1),
    catchup=False,
    tags=['monitoring', 'kafka', 'health-check'],
)

# Configuration
KAFKA_BROKERS = ['kafka:29092']
ES_HOST = 'http://elasticsearch:9200'

es_client = Elasticsearch([ES_HOST])

def check_kafka_broker_health(**context):
    """Check if Kafka brokers are healthy"""
    logging.info("Checking Kafka broker health...")
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKERS,
            request_timeout_ms=10000
        )
        
        # Get cluster metadata
        cluster_metadata = admin_client._client.cluster
        
        broker_count = len(cluster_metadata.brokers())
        controller_id = cluster_metadata.controller.id if cluster_metadata.controller else None
        
        health_status = {
            'timestamp': datetime.utcnow().isoformat(),
            'broker_count': broker_count,
            'controller_id': controller_id,
            'status': 'healthy' if broker_count > 0 else 'unhealthy',
            'brokers': [
                {
                    'id': broker.nodeId,
                    'host': broker.host,
                    'port': broker.port
                }
                for broker in cluster_metadata.brokers()
            ]
        }
        
        logging.info(f"Kafka cluster health: {health_status['status']}")
        logging.info(f"Active brokers: {broker_count}")
        
        admin_client.close()
        
        # Store in XCom
        context['task_instance'].xcom_push(key='broker_health', value=health_status)
        
        return health_status
        
    except Exception as e:
        logging.error(f"Failed to check broker health: {e}")
        raise

def check_topic_metrics(**context):
    """Check metrics for all topics"""
    logging.info("Checking topic metrics...")
    
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS)
        
        # List all topics
        topics = admin_client.list_topics()
        
        topic_metrics = []
        
        for topic in topics:
            # Get topic details
            topic_metadata = admin_client._client.cluster.topics()
            
            if topic in topic_metadata:
                partitions = topic_metadata[topic]
                
                metric = {
                    'topic_name': topic,
                    'partition_count': len(partitions),
                    'replication_factor': len(partitions[0].replicas) if partitions else 0,
                    'partitions': [
                        {
                            'partition_id': p.partition,
                            'leader': p.leader,
                            'replicas': p.replicas,
                            'isr': p.isr
                        }
                        for p in partitions
                    ]
                }
                
                topic_metrics.append(metric)
                
                logging.info(f"Topic: {topic} | Partitions: {len(partitions)}")
        
        admin_client.close()
        
        context['task_instance'].xcom_push(key='topic_metrics', value=topic_metrics)
        
        return topic_metrics
        
    except Exception as e:
        logging.error(f"Failed to check topic metrics: {e}")
        raise

def check_consumer_group_lag(**context):
    """Check consumer group lag"""
    logging.info("Checking consumer group lag...")
    
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS)
        
        # List consumer groups
        consumer_groups = admin_client.list_consumer_groups()
        
        lag_metrics = []
        
        for group_info in consumer_groups:
            group_id = group_info[0]
            
            try:
                # Get group offsets
                offsets = admin_client.list_consumer_group_offsets(group_id)
                
                group_lag = {
                    'consumer_group': group_id,
                    'timestamp': datetime.utcnow().isoformat(),
                    'topics': {}
                }
                
                total_lag = 0
                
                for topic_partition, offset_metadata in offsets.items():
                    topic = topic_partition.topic
                    partition = topic_partition.partition
                    current_offset = offset_metadata.offset
                    
                    # Get high water mark (latest offset)
                    consumer = KafkaConsumer(
                        bootstrap_servers=KAFKA_BROKERS,
                        group_id=f'lag_checker_{group_id}',
                        enable_auto_commit=False
                    )
                    
                    partitions = consumer.partitions_for_topic(topic)
                    if partitions:
                        tp = topic_partition
                        consumer.assign([tp])
                        consumer.seek_to_end(tp)
                        high_water_mark = consumer.position(tp)
                        
                        lag = high_water_mark - current_offset
                        total_lag += lag
                        
                        if topic not in group_lag['topics']:
                            group_lag['topics'][topic] = {
                                'partitions': {},
                                'total_lag': 0
                            }
                        
                        group_lag['topics'][topic]['partitions'][partition] = {
                            'current_offset': current_offset,
                            'high_water_mark': high_water_mark,
                            'lag': lag
                        }
                        group_lag['topics'][topic]['total_lag'] += lag
                    
                    consumer.close()
                
                group_lag['total_lag'] = total_lag
                lag_metrics.append(group_lag)
                
                logging.info(f"Consumer Group: {group_id} | Total Lag: {total_lag}")
                
                # Alert if lag is high
                if total_lag > 1000:
                    logging.warning(f"HIGH LAG ALERT: {group_id} has lag of {total_lag}")
                
            except Exception as e:
                logging.warning(f"Could not get lag for group {group_id}: {e}")
        
        admin_client.close()
        
        context['task_instance'].xcom_push(key='lag_metrics', value=lag_metrics)
        
        return lag_metrics
        
    except Exception as e:
        logging.error(f"Failed to check consumer lag: {e}")
        raise

def check_topic_health(**context):
    """Check if critical topics exist and are healthy"""
    logging.info("Checking critical topic health...")
    
    critical_topics = ['orders', 'order-analytics']
    
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS)
        existing_topics = admin_client.list_topics()
        
        topic_health = {
            'timestamp': datetime.utcnow().isoformat(),
            'critical_topics': [],
            'all_healthy': True
        }
        
        for topic in critical_topics:
            is_healthy = topic in existing_topics
            
            topic_health['critical_topics'].append({
                'name': topic,
                'exists': is_healthy,
                'status': 'healthy' if is_healthy else 'missing'
            })
            
            if not is_healthy:
                topic_health['all_healthy'] = False
                logging.error(f"CRITICAL: Topic '{topic}' is missing!")
        
        admin_client.close()
        
        context['task_instance'].xcom_push(key='topic_health', value=topic_health)
        
        return topic_health
        
    except Exception as e:
        logging.error(f"Failed to check topic health: {e}")
        raise

def aggregate_health_report(**context):
    """Aggregate all health metrics into a single report"""
    logging.info("Aggregating health report...")
    
    broker_health = context['task_instance'].xcom_pull(
        task_ids='check_broker_health',
        key='broker_health'
    )
    
    topic_metrics = context['task_instance'].xcom_pull(
        task_ids='check_topic_metrics',
        key='topic_metrics'
    )
    
    lag_metrics = context['task_instance'].xcom_pull(
        task_ids='check_consumer_lag',
        key='lag_metrics'
    )
    
    topic_health = context['task_instance'].xcom_pull(
        task_ids='check_topic_health',
        key='topic_health'
    )
    
    # Aggregate report
    health_report = {
        'timestamp': datetime.utcnow().isoformat(),
        'execution_date': context['execution_date'].isoformat(),
        'broker_health': broker_health,
        'topic_count': len(topic_metrics) if topic_metrics else 0,
        'topic_metrics': topic_metrics,
        'consumer_groups': len(lag_metrics) if lag_metrics else 0,
        'lag_metrics': lag_metrics,
        'topic_health': topic_health,
        'overall_status': 'healthy'
    }
    
    # Determine overall status
    if not broker_health or broker_health['status'] != 'healthy':
        health_report['overall_status'] = 'critical'
    elif topic_health and not topic_health['all_healthy']:
        health_report['overall_status'] = 'degraded'
    elif lag_metrics:
        max_lag = max([g['total_lag'] for g in lag_metrics], default=0)
        if max_lag > 5000:
            health_report['overall_status'] = 'warning'
    
    logging.info("=" * 80)
    logging.info("KAFKA HEALTH REPORT")
    logging.info("=" * 80)
    logging.info(f"Overall Status: {health_report['overall_status'].upper()}")
    logging.info(f"Brokers: {broker_health['broker_count'] if broker_health else 0}")
    logging.info(f"Topics: {health_report['topic_count']}")
    logging.info(f"Consumer Groups: {health_report['consumer_groups']}")
    logging.info("=" * 80)
    
    context['task_instance'].xcom_push(key='health_report', value=health_report)
    
    return health_report

def store_health_metrics(**context):
    """Store health metrics in Elasticsearch"""
    logging.info("Storing health metrics to Elasticsearch...")
    
    health_report = context['task_instance'].xcom_pull(
        task_ids='aggregate_health',
        key='health_report'
    )
    
    if not health_report:
        logging.warning("No health report to store")
        return
    
    try:
        # Create index if not exists
        index_name = 'kafka_health_metrics'
        
        if not es_client.indices.exists(index=index_name):
            mapping = {
                "mappings": {
                    "properties": {
                        "timestamp": {"type": "date"},
                        "execution_date": {"type": "date"},
                        "overall_status": {"type": "keyword"},
                        "broker_health.broker_count": {"type": "integer"},
                        "topic_count": {"type": "integer"},
                        "consumer_groups": {"type": "integer"}
                    }
                }
            }
            es_client.indices.create(index=index_name, body=mapping)
            logging.info(f"Created index: {index_name}")
        
        # Index document
        doc_id = context['execution_date'].strftime('%Y%m%d_%H%M')
        
        es_client.index(
            index=index_name,
            id=doc_id,
            body=health_report
        )
        
        logging.info(f"Stored health metrics with ID: {doc_id}")
        
        return True
        
    except Exception as e:
        logging.error(f"Failed to store health metrics: {e}")
        raise

def send_alerts_if_needed(**context):
    """Send alerts if critical issues detected"""
    logging.info("Checking for alert conditions...")
    
    health_report = context['task_instance'].xcom_pull(
        task_ids='aggregate_health',
        key='health_report'
    )
    
    if not health_report:
        return
    
    alerts = []
    
    # Check overall status
    if health_report['overall_status'] == 'critical':
        alerts.append({
            'severity': 'CRITICAL',
            'message': 'Kafka cluster is in critical state',
            'details': health_report['broker_health']
        })
    
    # Check broker count
    broker_count = health_report.get('broker_health', {}).get('broker_count', 0)
    if broker_count == 0:
        alerts.append({
            'severity': 'CRITICAL',
            'message': 'No Kafka brokers available',
            'details': 'All brokers are down'
        })
    
    # Check consumer lag
    if health_report.get('lag_metrics'):
        for lag_metric in health_report['lag_metrics']:
            total_lag = lag_metric.get('total_lag', 0)
            if total_lag > 10000:
                alerts.append({
                    'severity': 'WARNING',
                    'message': f"High consumer lag detected",
                    'details': f"Group: {lag_metric['consumer_group']}, Lag: {total_lag}"
                })
    
    # Check critical topics
    topic_health = health_report.get('topic_health', {})
    if not topic_health.get('all_healthy', True):
        alerts.append({
            'severity': 'CRITICAL',
            'message': 'Critical topics are missing',
            'details': topic_health['critical_topics']
        })
    
    # Log alerts
    if alerts:
        logging.warning(f"ALERTS GENERATED: {len(alerts)}")
        for alert in alerts:
            logging.warning(f"{alert['severity']}: {alert['message']} - {alert['details']}")
        
        # In production: send to Slack, PagerDuty, email, etc.
        # Example:
        # send_slack_alert(alerts)
        # send_pagerduty_alert(alerts)
    else:
        logging.info("No alerts - system is healthy")
    
    return alerts


# Define tasks
check_broker_health_task = PythonOperator(
    task_id='check_broker_health',
    python_callable=check_broker_health,
    dag=dag,
)

check_topic_metrics_task = PythonOperator(
    task_id='check_topic_metrics',
    python_callable=check_topic_metrics,
    dag=dag,
)

check_consumer_lag_task = PythonOperator(
    task_id='check_consumer_lag',
    python_callable=check_consumer_group_lag,
    dag=dag,
)

check_topic_health_task = PythonOperator(
    task_id='check_topic_health',
    python_callable=check_topic_health,
    dag=dag,
)

aggregate_health_task = PythonOperator(
    task_id='aggregate_health',
    python_callable=aggregate_health_report,
    dag=dag,
)

store_metrics_task = PythonOperator(
    task_id='store_metrics',
    python_callable=store_health_metrics,
    dag=dag,
)

send_alerts_task = PythonOperator(
    task_id='send_alerts',
    python_callable=send_alerts_if_needed,
    dag=dag,
)

# Set dependencies
[check_broker_health_task, check_topic_metrics_task, 
 check_consumer_lag_task, check_topic_health_task] >> aggregate_health_task

aggregate_health_task >> [store_metrics_task, send_alerts_task]