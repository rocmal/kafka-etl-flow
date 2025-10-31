"""
Airflow DAG - Order Analytics ETL Pipeline
Extracts data from Elasticsearch, transforms it, and loads aggregated insights
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import json
import logging

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'order_analytics_etl',
    default_args=default_args,
    description='ETL pipeline for order analytics',
    schedule_interval='*/15 * * * *',  # Run every 15 minutes
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'orders', 'analytics'],
)

# Elasticsearch client
es_client = Elasticsearch(['http://elasticsearch:9200'])

# ===== EXTRACT =====
def extract_orders(**context):
    """Extract recent orders from Elasticsearch"""
    logging.info("Starting extraction from Elasticsearch...")
    
    # Get last 15 minutes of data
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=15)
    
    query = {
        "query": {
            "range": {
                "timestamp": {
                    "gte": start_time.isoformat(),
                    "lte": end_time.isoformat()
                }
            }
        },
        "size": 10000,
        "sort": [{"timestamp": "asc"}]
    }
    
    try:
        response = es_client.search(index='orders', body=query)
        orders = [hit['_source'] for hit in response['hits']['hits']]
        
        logging.info(f"Extracted {len(orders)} orders")
        
        # Push to XCom for next task
        context['task_instance'].xcom_push(key='raw_orders', value=orders)
        
        return len(orders)
        
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        raise

# ===== TRANSFORM =====
def transform_orders(**context):
    """Transform and aggregate order data"""
    logging.info("Starting transformation...")
    
    # Pull data from previous task
    orders = context['task_instance'].xcom_pull(
        task_ids='extract_orders',
        key='raw_orders'
    )
    
    if not orders:
        logging.warning("No orders to transform")
        return
    
    # Initialize aggregations
    aggregations = {
        'total_orders': len(orders),
        'total_revenue': 0,
        'avg_order_value': 0,
        'by_region': {},
        'by_status': {},
        'by_category': {},
        'by_payment_method': {},
        'top_products': {},
        'hourly_orders': {},
        'timestamp': datetime.utcnow().isoformat()
    }
    
    # Process each order
    for order in orders:
        # Revenue calculations
        amount = order.get('total_amount', 0)
        aggregations['total_revenue'] += amount
        
        # Region aggregation
        region = order.get('region', 'Unknown')
        if region not in aggregations['by_region']:
            aggregations['by_region'][region] = {'count': 0, 'revenue': 0}
        aggregations['by_region'][region]['count'] += 1
        aggregations['by_region'][region]['revenue'] += amount
        
        # Status aggregation
        status = order.get('status', 'Unknown')
        if status not in aggregations['by_status']:
            aggregations['by_status'][status] = {'count': 0, 'revenue': 0}
        aggregations['by_status'][status]['count'] += 1
        aggregations['by_status'][status]['revenue'] += amount
        
        # Category aggregation
        category = order.get('category', 'Unknown')
        if category not in aggregations['by_category']:
            aggregations['by_category'][category] = {'count': 0, 'revenue': 0}
        aggregations['by_category'][category]['count'] += 1
        aggregations['by_category'][category]['revenue'] += amount
        
        # Payment method aggregation
        payment = order.get('payment_method', 'Unknown')
        if payment not in aggregations['by_payment_method']:
            aggregations['by_payment_method'][payment] = {'count': 0, 'revenue': 0}
        aggregations['by_payment_method'][payment]['count'] += 1
        aggregations['by_payment_method'][payment]['revenue'] += amount
        
        # Product aggregation
        product_name = order.get('product_name', 'Unknown')
        if product_name not in aggregations['top_products']:
            aggregations['top_products'][product_name] = {
                'count': 0,
                'revenue': 0,
                'quantity': 0
            }
        aggregations['top_products'][product_name]['count'] += 1
        aggregations['top_products'][product_name]['revenue'] += amount
        aggregations['top_products'][product_name]['quantity'] += order.get('quantity', 0)
        
        # Hourly aggregation
        timestamp = datetime.fromisoformat(order['timestamp'].replace('Z', '+00:00'))
        hour_key = timestamp.strftime('%Y-%m-%d %H:00:00')
        if hour_key not in aggregations['hourly_orders']:
            aggregations['hourly_orders'][hour_key] = {'count': 0, 'revenue': 0}
        aggregations['hourly_orders'][hour_key]['count'] += 1
        aggregations['hourly_orders'][hour_key]['revenue'] += amount
    
    # Calculate average order value
    aggregations['avg_order_value'] = (
        aggregations['total_revenue'] / aggregations['total_orders']
        if aggregations['total_orders'] > 0 else 0
    )
    
    # Sort top products by revenue
    aggregations['top_products'] = dict(
        sorted(
            aggregations['top_products'].items(),
            key=lambda x: x[1]['revenue'],
            reverse=True
        )[:10]
    )
    
    logging.info(f"Transformed {len(orders)} orders into aggregations")
    logging.info(f"Total Revenue: ${aggregations['total_revenue']:.2f}")
    
    # Push to XCom
    context['task_instance'].xcom_push(key='aggregations', value=aggregations)
    
    return aggregations

# ===== LOAD =====
def load_aggregations(**context):
    """Load aggregated data back to Elasticsearch"""
    logging.info("Starting load to Elasticsearch...")
    
    # Pull transformed data
    aggregations = context['task_instance'].xcom_pull(
        task_ids='transform_orders',
        key='aggregations'
    )
    
    if not aggregations:
        logging.warning("No aggregations to load")
        return
    
    try:
        # Index aggregations
        doc_id = aggregations['timestamp'].replace(':', '-').replace('.', '-')
        
        es_client.index(
            index='order_analytics',
            id=doc_id,
            body=aggregations
        )
        
        logging.info(f"Loaded aggregations to Elasticsearch with ID: {doc_id}")
        
        return True
        
    except Exception as e:
        logging.error(f"Load failed: {e}")
        raise

# ===== DATA QUALITY CHECKS =====
def validate_data(**context):
    """Validate data quality"""
    logging.info("Running data quality checks...")
    
    orders = context['task_instance'].xcom_pull(
        task_ids='extract_orders',
        key='raw_orders'
    )
    
    if not orders:
        logging.warning("No orders to validate")
        return True
    
    issues = []
    
    # Check for required fields
    required_fields = ['order_id', 'timestamp', 'product_id', 'total_amount']
    
    for i, order in enumerate(orders):
        for field in required_fields:
            if field not in order or order[field] is None:
                issues.append(f"Order {i}: Missing field '{field}'")
        
        # Check for negative amounts
        if order.get('total_amount', 0) < 0:
            issues.append(f"Order {i}: Negative total_amount")
        
        # Check for invalid quantity
        if order.get('quantity', 0) <= 0:
            issues.append(f"Order {i}: Invalid quantity")
    
    if issues:
        logging.warning(f"Data quality issues found: {len(issues)}")
        for issue in issues[:10]:  # Log first 10 issues
            logging.warning(issue)
    else:
        logging.info("All data quality checks passed")
    
    return len(issues) == 0

# ===== SEND ALERTS =====
def send_alert_if_needed(**context):
    """Send alert if revenue drops significantly"""
    logging.info("Checking for alerts...")
    
    aggregations = context['task_instance'].xcom_pull(
        task_ids='transform_orders',
        key='aggregations'
    )
    
    if not aggregations:
        return
    
    # Get previous period aggregations
    try:
        end_time = datetime.utcnow() - timedelta(minutes=15)
        start_time = end_time - timedelta(minutes=15)
        
        query = {
            "query": {
                "range": {
                    "timestamp": {
                        "gte": start_time.isoformat(),
                        "lt": end_time.isoformat()
                    }
                }
            },
            "size": 1,
            "sort": [{"timestamp": "desc"}]
        }
        
        response = es_client.search(index='order_analytics', body=query)
        
        if response['hits']['total']['value'] > 0:
            prev_agg = response['hits']['hits'][0]['_source']
            current_revenue = aggregations['total_revenue']
            prev_revenue = prev_agg['total_revenue']
            
            # Alert if revenue drops by more than 50%
            if current_revenue < prev_revenue * 0.5 and prev_revenue > 0:
                logging.warning(
                    f"ALERT: Revenue dropped from ${prev_revenue:.2f} to ${current_revenue:.2f}"
                )
                # In production, send email/slack notification here
            else:
                logging.info(f"Revenue is normal: ${current_revenue:.2f}")
        
    except Exception as e:
        logging.error(f"Alert check failed: {e}")

# ===== CLEANUP =====
def cleanup_old_data(**context):
    """Delete old analytics data (older than 30 days)"""
    logging.info("Cleaning up old data...")
    
    cutoff_date = datetime.utcnow() - timedelta(days=30)
    
    query = {
        "query": {
            "range": {
                "timestamp": {
                    "lt": cutoff_date.isoformat()
                }
            }
        }
    }
    
    try:
        result = es_client.delete_by_query(
            index='order_analytics',
            body=query
        )
        
        logging.info(f"Deleted {result['deleted']} old records")
        
    except Exception as e:
        logging.error(f"Cleanup failed: {e}")

# ===== DEFINE TASK DEPENDENCIES =====

# Extract task
extract_task = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_orders,
    dag=dag,
)

# Validate task
validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

# Transform task
transform_task = PythonOperator(
    task_id='transform_orders',
    python_callable=transform_orders,
    dag=dag,
)

# Load task
load_task = PythonOperator(
    task_id='load_aggregations',
    python_callable=load_aggregations,
    dag=dag,
)

# Alert task
alert_task = PythonOperator(
    task_id='send_alert',
    python_callable=send_alert_if_needed,
    dag=dag,
)

# Cleanup task
cleanup_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data,
    dag=dag,
)

# Set task dependencies (DAG flow)
extract_task >> validate_task >> transform_task >> load_task >> alert_task >> cleanup_task