# Trade Surveillance Pipeline

A data engineering project that simulates trade order generation and surveillance monitoring, built with Apache Airflow, PostgreSQL, and Docker.

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [DAG Workflow](#dag-workflow)
- [Data Model](#data-model)
- [Idempotency](#idempotency)
- [Configuration](#configuration)
- [Future Enhancements](#future-enhancements)

---

## Overview

This pipeline simulates a trade surveillance system that:

1. **Generates** synthetic FIX-protocol trade orders and executions
2. **Stores** data as CSV files (data lake layer)
3. **Loads** data into PostgreSQL (data warehouse layer)
4. **Validates** data integrity after each load

The project demonstrates core data engineering concepts including orchestration, idempotent data loading, and separation of concerns between data generation and data loading.

---

## Architecture
```
Airflow DAG
│
├── check_if_weekend
│   ├── Yes → skip_weekend (end)
│   └── No ↓
│
├── generate_new_orders
│   ├── Save to ./data/orders_YYYY-MM-DD.csv
│   └── Save to ./data/executions_YYYY-MM-DD.csv
│
├── load_orders
│   └── Read CSVs → Insert to PostgreSQL
│
└── validate_data
    └── Assert counts match
```


Data Flow:
──────────
1. Airflow scheduler triggers DAG (6 AM, Mon-Fri)
2. Check if weekend → skip if true
3. Generate 1,000 orders and executions → save to CSVs
4. Load CSVs → insert to PostgreSQL (ON CONFLICT DO NOTHING)
5. Validate → assert counts match
```

---

## Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Orchestration | Apache Airflow 2.7.1 | Schedule and monitor pipeline |
| Database | PostgreSQL 15 | Store trade and execution data |
| Language | Python 3.11 | Data generation and loading |
| Containerization | Docker & Docker Compose | Local development environment |
| Data Format | CSV | Intermediate storage (data lake) |

---

## Project Structure
```
trade-surveillance-pipeline/
├── dags/
│   └── surveillance_pipeline.py    # Airflow DAG definition
├── src/
│   ├── generate_orders.py          # Order/execution generation
│   └── load_data.py                # CSV reading and DB loading
├── sql/
│   ├── init-airflow-db.sql                    # Database schema
│   └── create_tables.sql           # Table creation scripts
├── data/                           # Generated CSV files
├── logs/                           # Airflow logs
├── plugins/                        # Airflow plugins
├── docker-compose.yml              # Docker services configuration
├── requirements.txt                # Python dependencies
└── README.md
```


---

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Git
- Python 3.11+ (for local development)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/trade-surveillance-pipeline.git
   cd trade-surveillance-pipeline
   ```

2. **Create required directories**
   ```bash
   mkdir -p ./data ./logs ./plugins
   ```

3. **Fix permissions for Airflow**
   ```bash
   sudo chown -R 50000:50000 ./data ./logs ./plugins
   ```
    *Note: This may already be handled by docker-compose, but run it if you encounter permission errors.*

4. **Start Docker containers**
   ```bash
   docker-compose up -d
   ```

5. **Wait for services to initialize** (30-60 seconds)

6. **Access Airflow UI**
   - URL: http://localhost:8080
   - Username: `admin`
   - Password: `admin`

### Running the Pipeline

1. Enable the DAG by toggling `trade_surveillance_daily` to ON
2. Click the play button (▶) to trigger manually
3. Monitor task progress in the Airflow UI

### Verify Data

```bash
# Check CSV files generated
ls ./data/

# Check database records
docker exec -it surveillance_db psql -U surveillance_user -d surveillance_db \
  -c "SELECT COUNT(*) FROM orders;"

# View sample data
docker exec -it surveillance_db psql -U surveillance_user -d surveillance_db \
  -c "SELECT * FROM orders LIMIT 5;"
```

---

## DAG Workflow

| Task | Description | Idempotent |
|------|-------------|------------|
| `check_if_weekend` | Branch: skip weekends, proceed on weekdays | N/A |
| `skip_weekend` | Empty task for weekend branch | N/A |
| `generate_new_orders` | Generate 1,000 orders → CSV (skip if exists) | ✅ |
| `load_orders` | Read CSV → insert to PostgreSQL | ✅ |
| `validate_data` | Assert database counts match expected | ✅ |

### Schedule

- **Frequency:** Daily at 6:00 AM UTC
- **Days:** Monday through Friday
- **Cron:** `0 6 * * 1-5`

---

## Data Model

### FIX Protocol Tags

This project uses standard FIX (Financial Information eXchange) protocol field tags:

| Tag | Field | Description |
|-----|-------|-------------|
| 11 | ClOrdID | Unique client order identifier |
| 17 | ExecID | Unique execution identifier |
| 31 | LastPx | Fill price |
| 32 | LastQty | Fill quantity |
| 38 | OrderQty | Order quantity |
| 39 | OrdStatus | Order status (0=New, 2=Filled) |
| 40 | OrdType | Order type (1=Market) |
| 54 | Side | Side (1=Buy, 2=Sell) |
| 55 | Symbol | Ticker symbol |
| 60 | TransactTime | Transaction timestamp |

### Database Schema

```sql
-- Firms
firms (
    firm_id VARCHAR(50) PRIMARY KEY,
    firm_name VARCHAR(100),
    account_type VARCHAR(50)
)

-- Accounts
accounts (
    account_id VARCHAR(50) PRIMARY KEY,
    firm_id VARCHAR(50) REFERENCES firms(firm_id)
)

-- Orders
orders (
    cl_ord_id VARCHAR(36) PRIMARY KEY,
    symbol VARCHAR(10),
    side CHAR(1),
    order_type CHAR(1),
    quantity INTEGER,
    transact_time TIMESTAMP,
    account_id VARCHAR(50),
    firm_id VARCHAR(50) REFERENCES firms(firm_id),
    ord_status CHAR(1)
)

-- Executions
executions (
    exec_id VARCHAR(36) PRIMARY KEY,
    cl_ord_id VARCHAR(36) REFERENCES orders(cl_ord_id),
    symbol VARCHAR(10),
    side CHAR(1),
    fill_qty INTEGER,
    fill_price DECIMAL(12,4),
    transact_time TIMESTAMP,
    venue VARCHAR(20)
)
```

### Entity Relationship

```
┌─────────┐       ┌──────────┐       ┌─────────┐       ┌────────────┐
│  firms  │──────<│ accounts │       │ orders  │──────<│ executions │
└─────────┘       └──────────┘       └─────────┘       └────────────┘
     │                                    │
     └────────────────────────────────────┘
```

---

## Idempotency

The pipeline is designed to be safely re-run without creating duplicate data.

### Why It Matters

In production, pipelines can fail mid-run and need to be retried. Without idempotency:
- Retries create duplicate records
- Data counts become unreliable
- Downstream reports are wrong

### How It's Implemented

| Layer | Mechanism | Behavior |
|-------|-----------|----------|
| CSV Generation | File existence check | Skip if `orders_YYYY-MM-DD.csv` exists |
| Database Insert | `ON CONFLICT DO NOTHING` | Duplicate primary keys are ignored |
| Validation | Count comparison | Uses `>=` to handle pre-existing data |

### CSV Generation
```python
if os.path.exists(orders_file):
    print(f"CSV already exists. Skipping generation.")
    return "skipped"
```

### Database Upsert Pattern
```sql
INSERT INTO orders (...)
VALUES (...)
ON CONFLICT (cl_ord_id) DO NOTHING
```

### Testing Idempotency

```bash
# Run the DAG twice
# Second run should:
# - Skip CSV generation
# - Insert 0 new rows (duplicates ignored)
# - Validation passes

docker exec -it surveillance_db psql -U surveillance_user -d surveillance_db \
  -c "SELECT DATE(transact_time), COUNT(*) FROM orders GROUP BY 1;"
```

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_DIR` | `./data` (local) or `/opt/airflow/data` (Docker) | CSV storage path |
| `DB_HOST` | `localhost` (local) or `postgres` (Docker) | Database host |
| `DB_PORT` | `5433` (local) or `5432` (Docker) | Database port |

### Docker Services

| Service | Port | Description |
|---------|------|-------------|
| `airflow_webserver` | 8080 | Airflow UI |
| `airflow_scheduler` | - | DAG scheduling |
| `surveillance_db` | 5433 | PostgreSQL database |

---

## Stopping the Pipeline

```bash
# Stop all containers (keep data)
docker-compose down

# Stop and delete all data (fresh start)
docker-compose down -v
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| DAG not appearing | Check scheduler logs: `docker logs airflow_scheduler` |
| Permission denied on ./data | Run: `sudo chown -R 50000:50000 ./data` |
| Database connection failed | Verify PostgreSQL is running: `docker ps` |
| Port 8080 in use | Stop other services or change port in docker-compose.yml |

---

## Future Enhancements

- [ ] **Cloud Migration:** Move CSVs to S3, database to Supabase
- [ ] **EC2 Deployment:** Run Airflow on AWS EC2
- [ ] **dbt Transformations:** Add staging and mart layers
- [ ] **Surveillance Logic:** Implement wash trade detection
- [ ] **Power BI Dashboard:** Visualize alerts and metrics
- [ ] **Kafka Streaming:** Real-time order processing

---

## Author

Tony - Data Engineer

---

## License

This project is for portfolio and educational purposes.