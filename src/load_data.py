import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from generate_orders import generate_trading_day, orders_to_dicts, executions_to_dicts
import pandas as pd
import os

DATA_DIR = os.environ.get("DATA_DIR", "./data")

# Database connection config - updated for Docker
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),  # Changed from localhost - this is the Docker service name
    "port": int(os.environ.get("DB_PORT", 5433)),
    "database": "surveillance_db",
    "user": "surveillance_user",
    "password": "surveillance_pass"
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
        """,
        values
    )
    
    conn.commit()
    cursor.close()
    print(f"Inserted {len(orders)} orders")


def insert_executions(conn, executions: list[dict]):
    """Bulk insert executions."""
    cursor = cursor = conn.cursor()
    
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
    orders_data = read_from_csv(f"orders_{date_str}.csv")
    executions_data = read_from_csv(f"executions_{date_str}.csv")

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
    
    df = pd.read_csv(filepath, parse_date=["transact_time"])
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
    from datetime import timedelta
    
    conn = get_connection()
    
    try:
        clear_tables(conn)
        
        # Generate 5 days of trading data
        base_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        all_orders = []
        all_executions = []
        
        for days_back in range(5):
            trade_date = base_date - timedelta(days=days_back)
            # Skip weekends
            if trade_date.weekday() >= 5:
                continue
            
            orders, executions = generate_trading_day(trade_date)
            all_orders.extend(orders)
            all_executions.extend(executions)
            print(f"Generated data for {trade_date.date()}")
        
        orders_data = orders_to_dicts(all_orders)
        executions_data = executions_to_dicts(all_executions)
        
        insert_accounts(conn, orders_data)
        insert_orders(conn, orders_data)
        insert_executions(conn, executions_data)
        
        print(f"\nLoaded {len(all_orders)} orders and {len(all_executions)} executions")
        print("Data load complete!")
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()