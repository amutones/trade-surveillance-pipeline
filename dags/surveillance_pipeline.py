from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from pendulum import timezone
import sys
import os

# Add your source code to Python path
sys.path.insert(0, '/opt/airflow/src')
DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")

def check_if_weekend(**context):
    """Skip pipeline on weekends."""
    execution_date = context['execution_date']
    
    # Monday=0, Sunday=6
    if execution_date.weekday() >= 5:
        print(f"Skipping - {execution_date.date()} is a weekend")
        return 'skip_weekend'
    else:
        print(f"Running - {execution_date.date()} is a weekday")
        return 'generate_new_orders'

def verify_s3_files(**context):
    from s3_utils import file_exists_in_s3

    execution_date = context['execution_date']
    trade_date = execution_date.replace(tzinfo=None)
    date_partition = trade_date.strftime("%Y/%m/%d")
    date_str = trade_date.strftime("%Y-%m-%d")

    required_files = [
        f"raw/orders/{date_partition}/orders_{date_str}.csv",
        f"raw/executions/{date_partition}/executions_{date_str}.csv",
        f"staged/orders/{date_partition}/orders_{date_str}.csv",
        f"staged/executions/{date_partition}/executions_{date_str}.csv",
    ]

    print(f"S3 Verification for {date_str}:")

    missing = []
    for key in required_files:
        exists = file_exists_in_s3(key)
        status = "✓ Found" if exists else "✗ Missing"
        print(f"  {key} - {status}")
        if not exists:
            missing.append(key)

    if missing:
        raise FileNotFoundError(f"Missing files in S3: {missing}")
    
    print("✓ All files verified")
    return True

def run_generate_new_orders(**context):
    """Generate day's trading data and save to csv."""
    from generate_orders import generate_trading_day
    from s3_utils import file_exists_in_s3

    # Use Airflow's execution date
    execution_date = context['execution_date']
    
    # Make it timezone-naive if needed
    trade_date = execution_date.replace(tzinfo=None)
    date_partition = trade_date.strftime("%Y/%m/%d")
    date_str = trade_date.strftime("%Y-%m-%d")

    s3_key = f"staged/orders/{date_partition}/orders_{date_str}.csv"

    if file_exists_in_s3(s3_key):
        print(f"CSV for {date_str} already exists in S3. Skipping generation.")
        return "skipped"
    
    print(f"Generating orders for {date_str}...")
    generate_trading_day(trade_date)
    print(f"CSV generated for {date_str}")
    return "generated"


def run_load_orders(**context):
    """Load day's trading data from csv to database."""
    from load_data import load_single_day

    # Use Airflow's execution date
    execution_date = context['execution_date']
    
    # Make it timezone-naive if needed
    trade_date = execution_date.replace(tzinfo=None)
    
    print(f"Loading data for {trade_date.date()}")
    order_count, exec_count = load_single_day(trade_date)

    # Store counts for validation
    context['task_instance'].xcom_push(key='order_count', value=order_count)
    context['task_instance'].xcom_push(key='exec_count', value=exec_count)
    
    return f"Loaded {order_count} orders, {exec_count} executions"

def run_validate_data(**context):
    """Validate that data was loaded correctly."""
    import psycopg2
    
    execution_date = context['execution_date'].replace(tzinfo=None)
    
    # Get counts from previous task
    ti = context['task_instance']
    expected_orders = ti.xcom_pull(task_ids='load_orders', key='order_count')
    expected_execs = ti.xcom_pull(task_ids='load_orders', key='exec_count')
    
    # Connect and verify
    conn = psycopg2.connect(
        host=os.environ.get("DB_HOST", "postgres"),
        port=int(os.environ.get("DB_PORT", 5432)),
        database=os.environ.get("DB_NAME", "postgres"),
        user=os.environ.get("DB_USER", "postgres"),
        password=os.environ.get("DB_PASSWORD", "surveillance_pass")
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
    assert actual_orders >= expected_orders, f"Order count mismatch!"
    assert actual_execs >= expected_execs, f"Execution count mismatch!"
    assert actual_orders > 0, "No orders loaded!"
    
    print("✓ Validation passed")

def run_dbt(**context):
    """Run dbt models after data load."""
    import subprocess

    dbt_project_dir = "/opt/airflow/surveillance_dbt"
    dbt_profiles_dir = "/home/airflow/.dbt"

    # Run dbt models
    print("Running dbt models...")
    result = subprocess.run(
        ["dbt", "run", "--project-dir", dbt_project_dir, "--profiles-dir", dbt_profiles_dir],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"dbt run failed: {result.stderr}")

    # Run dbt tests
    print("Running dbt tests...")
    result = subprocess.run(
        ["dbt", "test", "--project-dir", dbt_project_dir, "--profiles-dir", dbt_profiles_dir],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"dbt run failed: {result.stderr}")
    
    return "dbt completed successfully"

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
    schedule_interval='0 8 * * 1-5',  # 6 AM, Monday-Friday only
    start_date=datetime(2026, 2, 3, tzinfo=timezone("America/Chicago")),
    catchup=False,
    tags=['surveillance', 'etl', 'compliance'],
) as dag:

    # Check if weekend
    check_weekend = BranchPythonOperator(
        task_id='check_if_weekend',
        python_callable=check_if_weekend,
    )

    # Skip marker
    skip_weekend = EmptyOperator(
        task_id='skip_weekend',
    )

    # Main ETL
    #Generate orders
    generate_new_orders = PythonOperator(
        task_id='generate_new_orders',
        python_callable=run_generate_new_orders,
    )

    verify_s3 = PythonOperator(
        task_id='verify_s3_files',
        python_callable=verify_s3_files
    )

    load_orders = PythonOperator(
        task_id='load_orders',
        python_callable=run_load_orders,
    )
    
    
    # Validation
    validate = PythonOperator(
        task_id='validate_data',
        python_callable=run_validate_data,
    )

    run_dbt_models = PythonOperator(
        task_id="run_dbt_models",
        python_callable=run_dbt,
    )

    # Pipeline flow
    check_weekend >> [skip_weekend, generate_new_orders]
    generate_new_orders >> verify_s3 >> load_orders >> validate >> run_dbt_models