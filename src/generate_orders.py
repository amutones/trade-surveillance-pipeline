import uuid
import random
import csv
import os
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List
from s3_utils import upload_to_s3

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

DATA_DIR = os.environ.get("DATA_DIR", "./data")
NUM_ORDERS = int(os.environ.get("NUM_ORDERS", 1000))

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

    def validate(self) -> List[str]:
        """Returns list of errors (empty if valid)"""
        errors = []
        if not self.cl_ord_id or not isinstance(self.cl_ord_id, str):
            errors.append(f"Invalid cl_ord_id: {self.cl_ord_id}")
        if self.symbol not in SYMBOLS:
            errors.append(f"Invalid symbol: {self.symbol}")
        if self.side not in [SIDE_BUY, SIDE_SELL]:
            errors.append(f"Invalid side: {self.side} (must be '1' or '2')")
        if self.order_type not in [ORD_TYPE_MARKET]:
            errors.append(f"Invalid order_type: {self.order_type}")
        if not isinstance(self.quantity, int) or self.quantity <= 0:
            errors.append(f"Invalid quantity: {self.quantity}")
        if not isinstance(self.transact_time, datetime):
            errors.append(f"Invalid transact_time: {self.transact_time}")
        if not self.account_id or not isinstance(self.account_id, str):
            errors.append(f"Invalid account_id: {self.account_id}")
        valid_firm_ids = [f["firm_id"] for f in FIRMS]
        if self.firm_id not in valid_firm_ids:
            errors.append(f"Invalid firm_id: {self.firm_id}")
        if self.ord_status not in [ORD_STATUS_NEW, ORD_STATUS_FILLED]:
            errors.append(f"Invalid ord_status: {self.ord_status}")
        
        return errors
        

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

    def validate(self) -> List[str]:
        """Returns list of errors (empty if valid)."""
        errors = []
        
        if not self.exec_id or not isinstance(self.exec_id, str):
            errors.append(f"Invalid exec_id: {self.exec_id}")
        if not self.cl_ord_id or not isinstance(self.cl_ord_id, str):
            errors.append(f"Invalid cl_ord_id: {self.cl_ord_id}")
        if self.symbol not in SYMBOLS:
            errors.append(f"Invalid symbol: {self.symbol}")
        if self.side not in [SIDE_BUY, SIDE_SELL]:
            errors.append(f"Invalid side: {self.side}")
        if not isinstance(self.fill_qty, int) or self.fill_qty <= 0:
            errors.append(f"Invalid fill_qty: {self.fill_qty}")
        if not isinstance(self.fill_price, (int, float)) or self.fill_price <= 0:
            errors.append(f"Invalid fill_price: {self.fill_price}")
        if not isinstance(self.transact_time, datetime):
            errors.append(f"Invalid transact_time: {self.transact_time}")
        if self.venue not in VENUES:
            errors.append(f"Invalid venue: {self.venue}")
        
        return errors

def generate_trading_day(date: datetime, num_orders: int = None) -> tuple[List[Order], List[Execution]]:
    """Generate a day's worth of orders and executions."""
    
    if num_orders is None:
        num_orders = NUM_ORDERS
    
    raw_orders = []
    raw_executions = []
    
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

        errors = order.validate()
        if errors:
            raise ValueError(f"Order validation failed: {errors}")

        raw_orders.append(order)
        
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

        errors = execution.validate()
        if errors:
            raise ValueError(f"Execution validation failed: {errors}")

        raw_executions.append(execution)
    
    # Sort by time
    raw_orders.sort(key=lambda x: x.transact_time)
    raw_executions.sort(key=lambda x: x.transact_time)
    
    date_partition = date.strftime("%Y/%m/%d")
    date_str = date.strftime("%Y-%m-%d")
    orders_filename = f"orders_{date_str}.csv"
    executions_filename = f"executions_{date_str}.csv"

    save_to_csv(orders_to_dicts(raw_orders), orders_filename)
    save_to_csv(executions_to_dicts(raw_executions), executions_filename)

    local_orders_path = os.path.join(DATA_DIR, orders_filename)
    local_executions_path = os.path.join(DATA_DIR, executions_filename)

    upload_to_s3(local_orders_path, f"raw/orders/{date_partition}/{orders_filename}")
    upload_to_s3(local_executions_path, f"raw/executions/{date_partition}/{executions_filename}")
    
    # Sort by time
    raw_orders.sort(key=lambda x: x.transact_time)
    raw_executions.sort(key=lambda x: x.transact_time)

    save_to_csv(orders_to_dicts(raw_orders), orders_filename)
    save_to_csv(executions_to_dicts(raw_executions), executions_filename)
    
    upload_to_s3(local_orders_path, f"staged/orders/{date_partition}/{orders_filename}")
    upload_to_s3(local_executions_path, f"staged/executions/{date_partition}/{executions_filename}")

    return raw_orders, raw_executions


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
    
    os.makedirs(DATA_DIR, exist_ok=True)
    filepath = os.path.join(DATA_DIR, filepath)

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


if __name__ == "__main__":
    # Generate one trading day
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    orders, executions = generate_trading_day(today)
    
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