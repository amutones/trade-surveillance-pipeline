import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import pandas as pd
import os
from s3_utils import download_from_s3

DATA_DIR = os.environ.get("DATA_DIR", "./data")

# Database connection config - updated for Docker
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),  # Changed from localhost - this is the Docker service name
    "port": int(os.environ.get("DB_PORT", 5432)),
    "database": os.environ.get("DB_NAME", "postgres"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", "surveillance_pass")
}


def get_connection():
    """Create database connection."""
    return psycopg2.connect(**DB_CONFIG)


def insert_accounts(conn, orders: list[dict]):
    """Insert unique accounts from orders."""
    # Extract unique account/firm combinations
    accounts = set()
    for o in orders:
        accounts.add((o["account_id"], o["firm_id"]))
    
    cursor = conn.cursor()
    
    for account_id, firm_id in accounts:
        cursor.execute("""
            INSERT INTO accounts (account_id, firm_id)
            VALUES (%s, %s)
            ON CONFLICT (account_id) DO NOTHING
        """, (account_id, firm_id))
    
    conn.commit()
    cursor.close()
    print(f"Inserted {len(accounts)} accounts")


def insert_orders(conn, orders: list[dict]):
    """Bulk insert orders."""
    cursor = conn.cursor()
    
    columns = ["cl_ord_id", "symbol", "side", "order_type", "quantity", 
               "transact_time", "account_id", "firm_id", "ord_status"]
    
    values = [
        (o["cl_ord_id"], o["symbol"], o["side"], o["order_type"], o["quantity"],
         o["transact_time"], o["account_id"], o["firm_id"], o["ord_status"])
        for o in orders
    ]
    
    execute_values(
        cursor,
        """
        INSERT INTO orders (cl_ord_id, symbol, side, order_type, quantity,
                           transact_time, account_id, firm_id, ord_status)
        VALUES %s
        ON CONFLICT (cl_ord_id) DO NOTHING
        """,
        values
    )
    
    conn.commit()
    cursor.close()
    print(f"Inserted {len(orders)} orders")


def insert_executions(conn, executions: list[dict]):
    """Bulk insert executions."""
    cursor = conn.cursor()
    
    values = [
        (e["exec_id"], e["cl_ord_id"], e["symbol"], e["side"],
         e["fill_qty"], e["fill_price"], e["transact_time"], e["venue"])
        for e in executions
    ]
    
    execute_values(
        cursor,
        """
        INSERT INTO executions (exec_id, cl_ord_id, symbol, side,
                               fill_qty, fill_price, transact_time, venue)
        VALUES %s
        ON CONFLICT (exec_id) DO NOTHING
        """,
        values
    )
    
    conn.commit()
    cursor.close()
    print(f"Inserted {len(executions)} executions")


# NEW FUNCTION - for daily runs instead of clearing tables
def load_single_day(trade_date: datetime):
    """Load data for a single trading day - used by Airflow."""
    date_str = trade_date.strftime("%Y-%m-%d")
    
    orders_filename = f"orders_{date_str}.csv"
    executions_filename = f"executions_{date_str}.csv"
    local_orders_path = os.path.join(DATA_DIR, orders_filename)
    local_executions_path = os.path.join(DATA_DIR, executions_filename)

    download_from_s3(f"orders/{orders_filename}", local_orders_path)
    download_from_s3(f"executions/{executions_filename}", local_executions_path)

    orders_data = read_from_csv(orders_filename)
    executions_data = read_from_csv(executions_filename)

    conn = get_connection()
    
    try:
        
        insert_accounts(conn, orders_data)
        insert_orders(conn, orders_data)
        insert_executions(conn, executions_data)
        
        print(f"Loaded {len(orders_data)} orders and {len(executions_data)} executions for {trade_date.date()}")
        return len(orders_data), len(executions_data)
        
    finally:
        conn.close()

def read_from_csv(filename):
    filepath = os.path.join(DATA_DIR, filename)

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"CSV not found: {filepath}")
    
    df = pd.read_csv(filepath, parse_dates=["transact_time"])
    return df.to_dict("records")


def clear_tables(conn):
    """Clear existing data for fresh load."""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM executions")
    cursor.execute("DELETE FROM orders")
    cursor.execute("DELETE FROM accounts")
    conn.commit()
    cursor.close()
    print("Cleared existing data")


# Keep your original main for manual testing
def main():
    
    base_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    load_single_day(base_date)


if __name__ == "__main__":
    main()