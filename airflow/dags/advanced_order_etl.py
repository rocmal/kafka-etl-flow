"""
Advanced ETL Pipeline - Multi-stage data processing with quality checks
Demonstrates complex DAG with branching, parallel tasks, and sensors
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import logging
import json

default_args = {
    'owner': 'analytics-team',
    'depends_on_past': False,
    'email': ['alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'advanced_order_etl',
    default_args=default_args,
    description='Advanced multi-stage ETL with data quality gates',
    schedule_interval='0 * * * *',  # Hourly
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'advanced', 'data-quality'],
    max_active_runs=1,
)

es_client = Elasticsearch(['http://elasticsearch:9200'])

# ===== STAGE 1: DATA EXTRACTION =====

def extract_from_elasticsearch(**context):
    """Extract orders from Elasticsearch"""
    exec_date = context['execution_date']
    start_time = exec_date - timedelta(hours=1)
    end_time = exec_date
    
    logging.info(f"Extracting data from {start_time} to {end_time}")
    
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
    
    context['task_instance'].xcom_push(key='orders', value=orders)
    context['task_instance'].xcom_push(key='record_count', value=len(orders))
    
    logging.info(f"Extracted {len(orders)} orders")
    return len(orders)

def extract_from_kafka_metrics(**context):
    """Simulate extracting metrics from Kafka monitoring"""
    # In production, this would query Prometheus or Kafka JMX metrics
    metrics = {
        'avg_latency_ms': 45.2,
        'messages_per_sec': 125.5,
        'consumer_lag': 150,
        'error_rate': 0.02
    }
    
    context['task_instance'].xcom_push(key='kafka_metrics', value=metrics)
    logging.info("Extracted Kafka metrics")
    return metrics

# ===== STAGE 2: DATA QUALITY CHECKS =====

def check_data_completeness(**context):
    """Check if extracted data meets minimum requirements"""
    record_count = context['task_instance'].xcom_pull(
        task_ids='extract_elasticsearch',
        key='record_count'
    )
    
    min_records = 10  # Minimum expected records
    
    logging.info(f"Record count: {record_count}, Minimum: {min_records}")
    
    if record_count < min_records:
        logging.warning(f"Low record count: {record_count}")
        return 'handle_low_volume'
    else:
        logging.info("Data completeness check passed")
        return 'data_quality_passed'

def handle_low_volume(**context):
    """Handle low data volume scenario"""
    logging.warning("Low data volume detected - sending alert")
    # In production: send Slack/email alert
    return True

def check_data_quality(**context):
    """Comprehensive data quality checks"""
    orders = context['task_instance'].xcom_pull(
        task_ids='extract_elasticsearch',
        key='orders'
    )
    
    issues = {
        'missing_fields': 0,
        'invalid_amounts': 0,
        'duplicate_ids': 0,
        'future_dates': 0
    }
    
    seen_ids = set()
    now = datetime.utcnow()
    
    for order in orders:
        # Check required fields
        required = ['order_id', 'timestamp', 'total_amount', 'product_id']
        if not all(field in order for field in required):
            issues['missing_fields'] += 1
        
        # Check amounts
        if order.get('total_amount', 0) <= 0:
            issues['invalid_amounts'] += 1
        
        # Check duplicates
        order_id = order.get('order_id')
        if order_id in seen_ids:
            issues['duplicate_ids'] += 1
        seen_ids.add(order_id)
        
        # Check timestamps
        try:
            order_time = datetime.fromisoformat(order['timestamp'].replace('Z', '+00:00'))
            if order_time > now:
                issues['future_dates'] += 1
        except:
            pass
    
    total_issues = sum(issues.values())
    quality_score = 1 - (total_issues / len(orders)) if orders else 0
    
    context['task_instance'].xcom_push(key='quality_score', value=quality_score)
    context['task_instance'].xcom_push(key='quality_issues', value=issues)
    
    logging.info(f"Quality Score: {quality_score:.2%}")
    logging.info(f"Issues: {issues}")
    
    return quality_score

# ===== STAGE 3: TRANSFORMATIONS =====

def transform_revenue_analytics(**context):
    """Calculate revenue analytics"""
    orders = context['task_instance'].xcom_pull(
        task_ids='extract_elasticsearch',
        key='orders'
    )
    
    analytics = {
        'total_revenue': sum(o.get('total_amount', 0) for o in orders),
        'avg_order_value': 0,
        'max_order': 0,
        'min_order': float('inf'),
        'order_count': len(orders)
    }
    
    if orders:
        amounts = [o.get('total_amount', 0) for o in orders]
        analytics['avg_order_value'] = analytics['total_revenue'] / len(orders)
        analytics['max_order'] = max(amounts)
        analytics['min_order'] = min(amounts)
    
    context['task_instance'].xcom_push(key='revenue_analytics', value=analytics)
    logging.info(f"Revenue Analytics: {analytics}")
    
    return analytics

def transform_customer_analytics(**context):
    """Calculate customer analytics"""
    orders = context['task_instance'].xcom_pull(
        task_ids='extract_elasticsearch',
        key='orders'
    )
    
    customers = {}
    
    for order in orders:
        customer_id = order.get('customer_id')
        if customer_id:
            if customer_id not in customers:
                customers[customer_id] = {
                    'order_count': 0,
                    'total_spent': 0,
                    'products': set()
                }
            
            customers[customer_id]['order_count'] += 1
            customers[customer_id]['total_spent'] += order.get('total_amount', 0)
            customers[customer_id]['products'].add(order.get('product_id'))
    
    # Find top customers
    top_customers = sorted(
        customers.items(),
        key=lambda x: x[1]['total_spent'],
        reverse=True
    )[:10]
    
    analytics = {
        'unique_customers': len(customers),
        'avg_orders_per_customer': sum(c['order_count'] for c in customers.values()) / len(customers) if customers else 0,
        'top_customers': [
            {
                'customer_id': cid,
                'order_count': data['order_count'],
                'total_spent': data['total_spent']
            }
            for cid, data in top_customers
        ]
    }
    
    context['task_instance'].xcom_push(key='customer_analytics', value=analytics)
    logging.info(f"Customer Analytics: Unique customers: {analytics['unique_customers']}")
    
    return analytics

def transform_product_analytics(**context):
    """Calculate product performance analytics"""
    orders = context['task_instance'].xcom_pull(
        task_ids='extract_elasticsearch',
        key='orders'
    )
    
    products = {}
    
    for order in orders:
        product_id = order.get('product_id')
        product_name = order.get('product_name', 'Unknown')
        
        if product_id:
            if product_id not in products:
                products[product_id] = {
                    'name': product_name,
                    'order_count': 0,
                    'quantity_sold': 0,
                    'revenue': 0
                }
            
            products[product_id]['order_count'] += 1
            products[product_id]['quantity_sold'] += order.get('quantity', 0)
            products[product_id]['revenue'] += order.get('total_amount', 0)
    
    # Top products by revenue
    top_products = sorted(
        products.items(),
        key=lambda x: x[1]['revenue'],
        reverse=True
    )[:10]
    
    analytics = {
        'total_products': len(products),
        'top_products': [
            {
                'product_id': pid,
                **data
            }
            for pid, data in top_products
        ]
    }
    
    context['task_instance'].xcom_push(key='product_analytics', value=analytics)
    logging.info(f"Product Analytics: {len(products)} products analyzed")
    
    return analytics

# ===== STAGE 4: LOAD & REPORTING =====

def merge_analytics(**context):
    """Merge all analytics into final report"""
    revenue = context['task_instance'].xcom_pull(
        task_ids='transform_revenue',
        key='revenue_analytics'
    )
    
    customer = context['task_instance'].xcom_pull(
        task_ids='transform_customers',
        key='customer_analytics'
    )
    
    product = context['task_instance'].xcom_pull(
        task_ids='transform_products',
        key='product_analytics'
    )
    
    kafka_metrics = context['task_instance'].xcom_pull(
        task_ids='extract_kafka_metrics',
        key='kafka_metrics'
    )
    
    quality_score = context['task_instance'].xcom_pull(
        task_ids='check_quality',
        key='quality_score'
    )
    
    final_report = {
        'timestamp': datetime.utcnow().isoformat(),
        'execution_date': context['execution_date'].isoformat(),
        'data_quality_score': quality_score,
        'revenue': revenue,
        'customers': customer,
        'products': product,
        'system_metrics': kafka_metrics
    }
    
    context['task_instance'].xcom_push(key='final_report', value=final_report)
    
    logging.info("=" * 80)
    logging.info("FINAL ANALYTICS REPORT")
    logging.info("=" * 80)
    logging.info(json.dumps(final_report, indent=2))
    logging.info("=" * 80)
    
    return final_report

def load_to_elasticsearch(**context):
    """Load final analytics to Elasticsearch"""
    report = context['task_instance'].xcom_pull(
        task_ids='merge_analytics',
        key='final_report'
    )
    
    doc_id = context['execution_date'].strftime('%Y%m%d_%H')
    
    es_client.index(
        index='analytics_reports',
        id=doc_id,
        body=report
    )
    
    logging.info(f"Loaded analytics report to Elasticsearch: {doc_id}")
    return True

def generate_summary_report(**context):
    """Generate human-readable summary"""
    report = context['task_instance'].xcom_pull(
        task_ids='merge_analytics',
        key='final_report'
    )
    
    revenue = report['revenue']
    
    summary = f"""
    📊 HOURLY ANALYTICS SUMMARY
    ================================
    
    💰 Revenue:
       - Total: ${revenue['total_revenue']:,.2f}
       - Orders: {revenue['order_count']}
       - Avg Order: ${revenue['avg_order_value']:,.2f}
    
    👥 Customers:
       - Unique: {report['customers']['unique_customers']}
       - Avg Orders: {report['customers']['avg_orders_per_customer']:.1f}
    
    📦 Products:
       - Total Products: {report['products']['total_products']}
    
    ✅ Data Quality: {report['data_quality_score']:.1%}
    """
    
    logging.info(summary)
    return summary

# ===== DEFINE TASKS =====

start = DummyOperator(task_id='start', dag=dag)

# Extraction tasks (parallel)
extract_es = PythonOperator(
    task_id='extract_elasticsearch',
    python_callable=extract_from_elasticsearch,
    dag=dag,
)

extract_kafka = PythonOperator(
    task_id='extract_kafka_metrics',
    python_callable=extract_from_kafka_metrics,
    dag=dag,
)

extraction_complete = DummyOperator(task_id='extraction_complete', dag=dag)

# Data quality branch
check_completeness = BranchPythonOperator(
    task_id='check_completeness',
    python_callable=check_data_completeness,
    dag=dag,
)

handle_low = PythonOperator(
    task_id='handle_low_volume',
    python_callable=handle_low_volume,
    dag=dag,
)

quality_passed = DummyOperator(task_id='data_quality_passed', dag=dag)

check_quality = PythonOperator(
    task_id='check_quality',
    python_callable=check_data_quality,
    dag=dag,
)

# Transformation tasks (parallel)
transform_revenue = PythonOperator(
    task_id='transform_revenue',
    python_callable=transform_revenue_analytics,
    dag=dag,
)

transform_customers = PythonOperator(
    task_id='transform_customers',
    python_callable=transform_customer_analytics,
    dag=dag,
)

transform_products = PythonOperator(
    task_id='transform_products',
    python_callable=transform_product_analytics,
    dag=dag,
)

transform_complete = DummyOperator(
    task_id='transform_complete',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# Loading tasks
merge = PythonOperator(
    task_id='merge_analytics',
    python_callable=merge_analytics,
    dag=dag,
)

load_es = PythonOperator(
    task_id='load_to_elasticsearch',
    python_callable=load_to_elasticsearch,
    dag=dag,
)

generate_summary = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_summary_report,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# ===== SET DEPENDENCIES =====

# Extraction phase
start >> [extract_es, extract_kafka] >> extraction_complete

# Quality checks with branching
extraction_complete >> check_completeness
check_completeness >> handle_low >> check_quality
check_completeness >> quality_passed >> check_quality

# Parallel transformations
check_quality >> [transform_revenue, transform_customers, transform_products]

# Merge transformations
[transform_revenue, transform_customers, transform_products] >> transform_complete

# Load and reporting
transform_complete >> merge >> [load_es, generate_summary] >> end