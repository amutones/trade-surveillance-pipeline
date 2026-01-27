import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from generate_orders import generate_trading_day, orders_to_dicts, executions_to_dicts

# Database connection config
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
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
        """,
        values
    )
    
    conn.commit()
    cursor.close()
    print(f"Inserted {len(executions)} executions")


def clear_tables(conn):
    """Clear existing data for fresh load."""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM executions")
    cursor.execute("DELETE FROM orders")
    cursor.execute("DELETE FROM accounts")
    conn.commit()
    cursor.close()
    print("Cleared existing data")


def main():
    # Generate data
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    orders, executions = generate_trading_day(today, num_orders=500)
    
    orders_data = orders_to_dicts(orders)
    executions_data = executions_to_dicts(executions)
    
    # Load to database
    conn = get_connection()
    
    try:
        clear_tables(conn)
        insert_accounts(conn, orders_data)
        insert_orders(conn, orders_data)
        insert_executions(conn, executions_data)
        print("\nData load complete!")
    finally:
        conn.close()


if __name__ == "__main__":
    main()