from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys

# Add your source code to Python path
sys.path.insert(0, '/opt/airflow/src')

def check_if_weekend(**context):
    """Skip pipeline on weekends."""
    execution_date = context['execution_date']
    
    # Monday=0, Sunday=6
    if execution_date.weekday() >= 5:
        print(f"Skipping - {execution_date.date()} is a weekend")
        return 'skip_weekend'
    else:
        print(f"Running - {execution_date.date()} is a weekday")
        return 'generate_and_load_data'


def run_daily_load(**context):
    """Generate and load today's trading data."""
    from load_data import load_single_day
    
    # Use Airflow's execution date
    execution_date = context['execution_date']
    
    # Make it timezone-naive if needed
    trade_date = execution_date.replace(tzinfo=None)
    
    print(f"Starting data generation for {trade_date.date()}")
    order_count, exec_count = load_single_day(trade_date)
    
    # Store counts in XCom for validation task
    context['task_instance'].xcom_push(key='order_count', value=order_count)
    context['task_instance'].xcom_push(key='exec_count', value=exec_count)
    
    return f"Loaded {order_count} orders, {exec_count} executions"


def validate_data(**context):
    """Validate that data was loaded correctly."""
    import psycopg2
    
    execution_date = context['execution_date'].replace(tzinfo=None)
    
    # Get counts from previous task
    ti = context['task_instance']
    expected_orders = ti.xcom_pull(task_ids='generate_and_load_data', key='order_count')
    expected_execs = ti.xcom_pull(task_ids='generate_and_load_data', key='exec_count')
    
    # Connect and verify
    conn = psycopg2.connect(
        host='postgres',
        database='surveillance_db',
        user='surveillance_user',
        password='surveillance_pass'
    )
    
    cursor = conn.cursor()
    
    # Count orders for this date
    cursor.execute("""
        SELECT COUNT(*) FROM orders 
        WHERE DATE(transact_time) = %s
    """, (execution_date.date(),))
    actual_orders = cursor.fetchone()[0]
    
    # Count executions for this date
    cursor.execute("""
        SELECT COUNT(*) FROM executions 
        WHERE DATE(transact_time) = %s
    """, (execution_date.date(),))
    actual_execs = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"Validation Results:")
    print(f"  Orders: {actual_orders} (expected {expected_orders})")
    print(f"  Executions: {actual_execs} (expected {expected_execs})")
    
    # Basic assertions
    assert actual_orders == expected_orders, f"Order count mismatch!"
    assert actual_execs == expected_execs, f"Execution count mismatch!"
    assert actual_orders > 0, "No orders loaded!"
    
    print("✓ Validation passed")


default_args = {
    'owner': 'tony',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 3),  # Start from a Monday
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'trade_surveillance_daily',
    default_args=default_args,
    description='Daily trade surveillance data pipeline',
    schedule_interval='0 6 * * 1-5',  # 6 AM, Monday-Friday only
    catchup=False,
    tags=['surveillance', 'etl', 'compliance'],
) as dag:

    # Check if weekend
    check_weekend = BranchPythonOperator(
        task_id='check_if_weekend',
        python_callable=check_if_weekend,
        provide_context=True,
    )

    # Skip marker
    skip_weekend = EmptyOperator(
        task_id='skip_weekend',
    )

    # Main ETL
    generate_load = PythonOperator(
        task_id='generate_and_load_data',
        python_callable=run_daily_load,
        provide_context=True,
    )

    # Validation
    validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        provide_context=True,
    )

    # Pipeline flow
    check_weekend >> [skip_weekend, generate_load]
    generate_load >> validate