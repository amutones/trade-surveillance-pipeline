import uuid
import random
import csv
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List

# FIX-standard values
SIDE_BUY = "1"
SIDE_SELL = "2"
ORD_TYPE_MARKET = "1"
ORD_STATUS_NEW = "0"
ORD_STATUS_FILLED = "2"

# Config
SYMBOLS = ["AAPL", "MSFT", "SPY", "QQQ", "GOOGL", "AMZN", "TSLA", "META"]
FIRMS = [
    {"firm_id": "FIRM_A", "firm_name": "Alpha Capital", "account_type": "proprietary"},
    {"firm_id": "FIRM_B", "firm_name": "Beta Investments", "account_type": "institutional"},
]
VENUES = ["NYSE", "NASDAQ", "CBOE", "ARCA"]


@dataclass
class Order:
    cl_ord_id: str          # Tag 11 - unique client order ID
    symbol: str             # Tag 55
    side: str               # Tag 54 - 1=buy, 2=sell
    order_type: str         # Tag 40 - 1=market
    quantity: int           # Tag 38
    transact_time: datetime # Tag 60
    account_id: str         # Tag 1
    firm_id: str
    ord_status: str         # Tag 39


@dataclass
class Execution:
    exec_id: str            # Tag 17
    cl_ord_id: str          # Tag 11 - links to order
    symbol: str             # Tag 55
    side: str               # Tag 54
    fill_qty: int           # Tag 32
    fill_price: float       # Tag 31
    transact_time: datetime # Tag 60
    venue: str              # Tag 30


def generate_trading_day(date: datetime, num_orders: int = 1000) -> tuple[List[Order], List[Execution]]:
    """Generate a day's worth of orders and executions."""
    
    orders = []
    executions = []
    
    # Market hours: 9:30 AM - 4:00 PM
    market_open = date.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = date.replace(hour=16, minute=0, second=0, microsecond=0)
    trading_seconds = int((market_close - market_open).total_seconds())
    
    for _ in range(num_orders):
        # Random timestamp during market hours
        random_seconds = random.randint(0, trading_seconds)
        order_time = market_open + timedelta(seconds=random_seconds)
        
        # Pick firm and create account
        firm = random.choice(FIRMS)
        account_id = f"{firm['firm_id']}_ACCT_{random.randint(100, 999)}"
        
        # Create order
        order = Order(
            cl_ord_id=str(uuid.uuid4()),
            symbol=random.choice(SYMBOLS),
            side=random.choice([SIDE_BUY, SIDE_SELL]),
            order_type=ORD_TYPE_MARKET,
            quantity=random.choice([100, 200, 500, 1000, 2500]),
            transact_time=order_time,
            account_id=account_id,
            firm_id=firm["firm_id"],
            ord_status=ORD_STATUS_FILLED  # Market orders fill immediately
        )
        orders.append(order)
        
        # Market orders get immediate fill - create execution
        # Simulate realistic price based on symbol
        base_prices = {
            "AAPL": 175.00, "MSFT": 375.00, "SPY": 450.00, "QQQ": 380.00,
            "GOOGL": 140.00, "AMZN": 175.00, "TSLA": 250.00, "META": 500.00
        }
        base_price = base_prices.get(order.symbol, 100.00)
        # Add some variance (+/- 0.5%)
        fill_price = round(base_price * random.uniform(0.995, 1.005), 2)
        
        execution = Execution(
            exec_id=str(uuid.uuid4()),
            cl_ord_id=order.cl_ord_id,
            symbol=order.symbol,
            side=order.side,
            fill_qty=order.quantity,
            fill_price=fill_price,
            transact_time=order_time + timedelta(milliseconds=random.randint(10, 500)),
            venue=random.choice(VENUES)
        )
        executions.append(execution)
    
    # Sort by time
    orders.sort(key=lambda x: x.transact_time)
    executions.sort(key=lambda x: x.transact_time)
    
    date_str = date.strftime("%Y-%m-%d")
    save_to_csv(orders_to_dicts(orders), f"/opt/airflow/data/orders_{date_str}.csv")
    save_to_csv(executions_to_dicts(executions), f"/opt/airflow/data/executions_{date_str}.csv")

    return orders, executions


def orders_to_dicts(orders: List[Order]) -> List[dict]:
    """Convert orders to dictionaries for database insertion."""
    return [
        {
            "cl_ord_id": o.cl_ord_id,
            "symbol": o.symbol,
            "side": o.side,
            "order_type": o.order_type,
            "quantity": o.quantity,
            "transact_time": o.transact_time,
            "account_id": o.account_id,
            "firm_id": o.firm_id,
            "ord_status": o.ord_status
        }
        for o in orders
    ]


def executions_to_dicts(executions: List[Execution]) -> List[dict]:
    """Convert executions to dictionaries for database insertion."""
    return [
        {
            "exec_id": e.exec_id,
            "cl_ord_id": e.cl_ord_id,
            "symbol": e.symbol,
            "side": e.side,
            "fill_qty": e.fill_qty,
            "fill_price": e.fill_price,
            "transact_time": e.transact_time,
            "venue": e.venue
        }
        for e in executions
    ]

def save_to_csv(data, filepath):
    if not data:
        return
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


if __name__ == "__main__":
    # Generate one trading day
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    orders, executions = generate_trading_day(today, num_orders=1000)
    
    print(f"Generated {len(orders)} orders and {len(executions)} executions")
    print(f"\nSample order:")
    print(orders[0])
    print(f"\nSample execution:")
    print(executions[0])
    
    # Summary stats
    firm_a_orders = len([o for o in orders if o.firm_id == "FIRM_A"])
    firm_b_orders = len([o for o in orders if o.firm_id == "FIRM_B"])
    print(f"\nFirm A orders: {firm_a_orders}")
    print(f"Firm B orders: {firm_b_orders}")