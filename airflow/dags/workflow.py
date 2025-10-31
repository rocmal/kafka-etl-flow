"""
Data Quality Validation DAG
Validates data quality in Elasticsearch orders index
Checks for anomalies, missing data, and data integrity
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import numpy as np
import logging

default_args = {
    'owner': 'data-quality-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['data-quality@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'data_quality_validation',
    default_args=default_args,
    description='Validate data quality in orders index',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    start_date=days_ago(1),
    catchup=False,
    tags=['data-quality', 'validation', 'orders'],
)

es_client = Elasticsearch(['http://elasticsearch:9200'])

def validate_completeness(**context):
    """Check for missing or null values in critical fields"""
    logging.info("Validating data completeness...")
    
    critical_fields = [
        'order_id', 'timestamp', 'product_id', 'product_name',
        'quantity', 'price', 'total_amount', 'region', 'status', 'customer_id'
    ]
    
    # Get recent orders (last 6 hours)
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=6)
    
    query = {
        "query": {
            "range": {
                "timestamp": {
                    "gte": start_time.isoformat(),
                    "lt": end_time.isoformat()
                }
            }
        },
        "size": 10000
    }
    
    response = es_client.search(index='orders', body=query)
    orders = [hit['_source'] for hit in response['hits']['hits']]
    
    completeness_issues = {
        'total_records': len(orders),
        'missing_fields': {},
        'null_values': {},
        'completeness_score': 0
    }
    
    for field in critical_fields:
        missing_count = 0
        null_count = 0
        
        for order in orders:
            if field not in order:
                missing_count += 1
            elif order[field] is None or order[field] == '':
                null_count += 1
        
        if missing_count > 0:
            completeness_issues['missing_fields'][field] = missing_count
        if null_count > 0:
            completeness_issues['null_values'][field] = null_count
    
    # Calculate completeness score
    total_checks = len(orders) * len(critical_fields)
    total_issues = sum(completeness_issues['missing_fields'].values()) + \
                   sum(completeness_issues['null_values'].values())
    
    completeness_issues['completeness_score'] = (
        1 - (total_issues / total_checks)
    ) if total_checks > 0 else 0
    
    logging.info(f"Completeness Score: {completeness_issues['completeness_score']:.2%}")
    
    if completeness_issues['missing_fields']:
        logging.warning(f"Missing fields detected: {completeness_issues['missing_fields']}")
    if completeness_issues['null_values']:
        logging.warning(f"Null values detected: {completeness_issues['null_values']}")
    
    context['task_instance'].xcom_push(key='completeness', value=completeness_issues)
    
    return completeness_issues

def validate_consistency(**context):
    """Check for data consistency issues"""
    logging.info("Validating data consistency...")
    
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=6)
    
    query = {
        "query": {
            "range": {
                "timestamp": {
                    "gte": start_time.isoformat(),
                    "lt": end_time.isoformat()
                }
            }
        },
        "size": 10000
    }
    
    response = es_client.search(index='orders', body=query)
    orders = [hit['_source'] for hit in response['hits']['hits']]
    
    consistency_issues = {
        'invalid_amounts': [],
        'invalid_quantities': [],
        'price_mismatches': [],
        'future_timestamps': [],
        'invalid_statuses': [],
        'consistency_score': 0
    }
    
    valid_statuses = ['pending', 'confirmed', 'processing', 'shipped', 'delivered']
    total_issues = 0
    
    for order in orders:
        order_id = order.get('order_id', 'unknown')
        
        # Check amounts
        total_amount = order.get('total_amount', 0)
        if total_amount <= 0:
            consistency_issues['invalid_amounts'].append({
                'order_id': order_id,
                'amount': total_amount
            })
            total_issues += 1
        
        # Check quantities
        quantity = order.get('quantity', 0)
        if quantity <= 0:
            consistency_issues['invalid_quantities'].append({
                'order_id': order_id,
                'quantity': quantity
            })
            total_issues += 1
        
        # Check price consistency
        price = order.get('price', 0)
        calculated_total = price * quantity
        if abs(calculated_total - total_amount) > 0.01:
            consistency_issues['price_mismatches'].append({
                'order_id': order_id,
                'expected': calculated_total,
                'actual': total_amount
            })
            total_issues += 1
        
        # Check timestamps
        try:
            timestamp = datetime.fromisoformat(order['timestamp'].replace('Z', '+00:00'))
            if timestamp > datetime.utcnow():
                consistency_issues['future_timestamps'].append({
                    'order_id': order_id,
                    'timestamp': order['timestamp']
                })
                total_issues += 1
        except:
            pass
        
        # Check status values
        status = order.get('status', '')
        if status not in valid_statuses:
            consistency_issues['invalid_statuses'].append({
                'order_id': order_id,
                'status': status
            })
            total_issues += 1
    
    # Calculate consistency score
    consistency_issues['consistency_score'] = (
        1 - (total_issues / len(orders))
    ) if orders else 0
    
    logging.info(f"Consistency Score: {consistency_issues['consistency_score']:.2%}")
    
    for issue_type, issues in consistency_issues.items():
        if isinstance(issues, list) and len(issues) > 0:
            logging.warning(f"{issue_type}: {len(issues)} issues found")
    
    context['task_instance'].xcom_push(key='consistency', value=consistency_issues)
    
    return consistency_issues

def validate_uniqueness(**context):
    """Check for duplicate order IDs"""
    logging.info("Validating uniqueness...")
    
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=6)
    
    # Aggregation to find duplicates
    query = {
        "query": {
            "range": {
                "timestamp": {
                    "gte": start_time.isoformat(),
                    "lt": end_time.isoformat()
                }
            }
        },
        "size": 0,
        "aggs": {
            "duplicate_orders": {
                "terms": {
                    "field": "order_id.keyword",
                    "min_doc_count": 2,
                    "size": 100
                }
            }
        }
    }
    
    response = es_client.search(index='orders', body=query)
    duplicates = response['aggregations']['duplicate_orders']['buckets']
    
    uniqueness_issues = {
        'duplicate_count': len(duplicates),
        'duplicates': [
            {
                'order_id': bucket['key'],
                'count': bucket['doc_count']
            }
            for bucket in duplicates
        ],
        'uniqueness_score': 0
    }
    
    # Get total count
    total_response = es_client.count(
        index='orders',
        body={"query": query["query"]}
    )
    total_count = total_response['count']
    
    uniqueness_issues['uniqueness_score'] = (
        1 - (len(duplicates) / total_count)
    ) if total_count > 0 else 1
    
    logging.info(f"Uniqueness Score: {uniqueness_issues['uniqueness_score']:.2%}")
    
    if duplicates:
        logging.warning(f"Found {len(duplicates)} duplicate order IDs")
        for dup in duplicates[:5]:  # Log first 5
            logging.warning(f"  {dup['key']}: {dup['doc_count']} occurrences")
    
    context['task_instance'].xcom_push(key='uniqueness', value=uniqueness_issues)
    
    return uniqueness_issues

def validate_timeliness(**context):
    """Check if data is arriving in a timely manner"""
    logging.info("Validating data timeliness...")
    
    # Get latest order timestamp
    query = {
        "size": 1,
        "sort": [{"timestamp": "desc"}]
    }
    
    response = es_client.search(index='orders', body=query)
    
    if response['hits']['total']['value'] == 0:
        timeliness_issues = {
            'latest_timestamp': None,
            'delay_seconds': float('inf'),
            'status': 'critical',
            'timeliness_score': 0
        }
    else:
        latest_order = response['hits']['hits'][0]['_source']
        latest_timestamp = datetime.fromisoformat(
            latest_order['timestamp'].replace('Z', '+00:00')
        )
        
        delay = (datetime.utcnow() - latest_timestamp.replace(tzinfo=None)).total_seconds()
        
        # Determine status based on delay
        if delay < 300:  # 5 minutes
            status = 'healthy'
            score = 1.0
        elif delay < 600:  # 10 minutes
            status = 'warning'
            score = 0.8
        elif delay < 1800:  # 30 minutes
            status = 'degraded'
            score = 0.5
        else:
            status = 'critical'
            score = 0.2
        
        timeliness_issues = {
            'latest_timestamp': latest_order['timestamp'],
            'delay_seconds': delay,
            'delay_minutes': delay / 60,
            'status': status,
            'timeliness_score': score
        }
    
    logging.info(f"Timeliness Score: {timeliness_issues['timeliness_score']:.2%}")
    logging.info(f"Latest data delay: {timeliness_issues.get('delay_minutes', 'N/A')} minutes")
    
    context['task_instance'].xcom_push(key='timeliness', value=timeliness_issues)
    
    return timeliness_issues

def validate_statistical_anomalies(**context):
    """Detect statistical anomalies in order data"""
    logging.info("Detecting statistical anomalies...")
    
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=6)
    
    query = {
        "query": {
            "range": {
                "timestamp": {
                    "gte": start_time.isoformat(),
                    "lt": end_time.isoformat()
                }
            }
        },
        "size": 10000
    }
    
    response = es_client.search(index='orders', body=query)
    orders = [hit['_source'] for hit in response['hits']['hits']]
    
    if not orders:
        logging.warning("No orders to analyze for anomalies")
        return {}
    
    # Extract numerical data
    amounts = [o.get('total_amount', 0) for o in orders]
    quantities = [o.get('quantity', 0) for o in orders]
    
    # Calculate statistics
    mean_amount = np.mean(amounts)
    std_amount = np.std(amounts)
    mean_quantity = np.mean(quantities)
    std_quantity = np.std(quantities)
    
    anomalies = {
        'amount_outliers': [],
        'quantity_outliers': [],
        'statistics': {
            'amount': {
                'mean': float(mean_amount),
                'std': float(std_amount),
                'min': float(min(amounts)),
                'max': float(max(amounts))
            },
            'quantity': {
                'mean': float(mean_quantity),
                'std': float(std_quantity),
                'min': int(min(quantities)),
                'max': int(max(quantities))
            }
        },
        'anomaly_score': 1.0
    }
    
    # Detect outliers (3 standard deviations)
    outlier_count = 0
    
    for order in orders:
        order_id = order.get('order_id', 'unknown')
        amount = order.get('total_amount', 0)
        quantity = order.get('quantity', 0)
        
        # Check amount outliers
        if abs(amount - mean_amount) > 3 * std_amount:
            anomalies['amount_outliers'].append({
                'order_id': order_id,
                'amount': amount,
                'z_score': (amount - mean_amount) / std_amount if std_amount > 0 else 0
            })
            outlier_count += 1
        
        # Check quantity outliers
        if abs(quantity - mean_quantity) > 3 * std_quantity:
            anomalies['quantity_outliers'].append({
                'order_id': order_id,
                'quantity': quantity,
                'z_score': (quantity - mean_quantity) / std_quantity if std_quantity > 0 else 0
            })
            outlier_count += 1
    
    anomalies['anomaly_score'] = 1 - (outlier_count / len(orders)) if orders else 1
    
    logging.info(f"Anomaly Score: {anomalies['anomaly_score']:.2%}")
    logging.info(f"Amount outliers: {len(anomalies['amount_outliers'])}")
    logging.info(f"Quantity outliers: {len(anomalies['quantity_outliers'])}")
    
    context['task_instance'].xcom_push(key='anomalies', value=anomalies)
    
    return anomalies

def generate_quality_report(**context):
    """Generate comprehensive data quality report"""
    logging.info("Generating data quality report...")
    
    completeness = context['task_instance'].xcom_pull(
        task_ids='validate_completeness',
        key='completeness'
    )
    
    consistency = context['task_instance'].xcom_pull(
        task_ids='validate_consistency',
        key='consistency'
    )
    
    uniqueness = context['task_instance'].xcom_pull(
        task_ids='validate_uniqueness',
        key='uniqueness'
    )
    
    timeliness = context['task_instance'].xcom_pull(
        task_ids='validate_timeliness',
        key='timeliness'
    )
    
    anomalies = context['task_instance'].xcom_pull(
        task_ids='validate_anomalies',
        key='anomalies'
    )
    
    # Calculate overall quality score
    scores = [
        completeness.get('completeness_score', 0),
        consistency.get('consistency_score', 0),
        uniqueness.get('uniqueness_score', 0),
        timeliness.get('timeliness_score', 0),
        anomalies.get('anomaly_score', 0)
    ]
    
    overall_score = np.mean(scores)
    
    # Determine overall status
    if overall_score >= 0.95:
        status = 'excellent'
    elif overall_score >= 0.85:
        status = 'good'
    elif overall_score >= 0.70:
        status = 'fair'
    else:
        status = 'poor'
    
    quality_report = {
        'timestamp': datetime.utcnow().isoformat(),
        'execution_date': context['execution_date'].isoformat(),
        'overall_score': overall_score,
        'overall_status': status,
        'scores': {
            'completeness': completeness.get('completeness_score', 0),
            'consistency': consistency.get('consistency_score', 0),
            'uniqueness': uniqueness.get('uniqueness_score', 0),
            'timeliness': timeliness.get('timeliness_score', 0),
            'anomaly_detection': anomalies.get('anomaly_score', 0)
        },
        'details': {
            'completeness': completeness,
            'consistency': consistency,
            'uniqueness': uniqueness,
            'timeliness': timeliness,
            'anomalies': anomalies
        }
    }
    
    # Print summary
    logging.info("=" * 80)
    logging.info("DATA QUALITY REPORT")
    logging.info("=" * 80)
    logging.info(f"Overall Score: {overall_score:.2%} ({status.upper()})")
    logging.info(f"Completeness: {scores[0]:.2%}")
    logging.info(f"Consistency:  {scores[1]:.2%}")
    logging.info(f"Uniqueness:   {scores[2]:.2%}")
    logging.info(f"Timeliness:   {scores[3]:.2%}")
    logging.info(f"Anomalies:    {scores[4]:.2%}")
    logging.info("=" * 80)
    
    context['task_instance'].xcom_push(key='quality_report', value=quality_report)
    
    return quality_report

def store_quality_report(**context):
    """Store quality report in Elasticsearch"""
    logging.info("Storing quality report...")
    
    report = context['task_instance'].xcom_pull(
        task_ids='generate_report',
        key='quality_report'
    )
    
    index_name = 'data_quality_reports'
    
    # Create index if needed
    if not es_client.indices.exists(index=index_name):
        mapping = {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "overall_score": {"type": "float"},
                    "overall_status": {"type": "keyword"}
                }
            }
        }
        es_client.indices.create(index=index_name, body=mapping)
    
    # Store report
    doc_id = context['execution_date'].strftime('%Y%m%d_%H')
    es_client.index(index=index_name, id=doc_id, body=report)
    
    logging.info(f"Stored quality report: {doc_id}")
    
    return True


# Define tasks
validate_completeness_task = PythonOperator(
    task_id='validate_completeness',
    python_callable=validate_completeness,
    dag=dag,
)

validate_consistency_task = PythonOperator(
    task_id='validate_consistency',
    python_callable=validate_consistency,
    dag=dag,
)

validate_uniqueness_task = PythonOperator(
    task_id='validate_uniqueness',
    python_callable=validate_uniqueness,
    dag=dag,
)

validate_timeliness_task = PythonOperator(
    task_id='validate_timeliness',
    python_callable=validate_timeliness,
    dag=dag,
)

validate_anomalies_task = PythonOperator(
    task_id='validate_anomalies',
    python_callable=validate_statistical_anomalies,
    dag=dag,
)

generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_quality_report,
    dag=dag,
)

store_report_task = PythonOperator(
    task_id='store_report',
    python_callable=store_quality_report,
    dag=dag,
)

# Set dependencies - all validations run in parallel
[validate_completeness_task, validate_consistency_task, 
 validate_uniqueness_task, validate_timeliness_task,
 validate_anomalies_task] >> generate_report_task >> store_report_task