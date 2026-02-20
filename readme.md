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
│   ├── Generate 1,000 orders + 1,000 executions
│   ├── Save to local ./data/
│   └── Upload to S3 bucket
│
├── load_orders
│   ├── Download from S3 bucket
│   └── Insert to PostgreSQL
│
└── validate_data
    └── Assert counts match
```

Data Flow:
──────────
1. Airflow scheduler triggers DAG (6 AM, Mon-Fri)
2. Check if weekend → skip if true
3. Generate 1,000 orders + 1,000 executions → save to S3
4. Download from S3 → insert to PostgreSQL (ON CONFLICT DO NOTHING)
5. Validate → assert order and execution counts match
```

---

## Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Orchestration | Apache Airflow 2.7.1 | Schedule and monitor pipeline |
| Database | PostgreSQL 15 | Store trade and execution data |
| Cloud Storage | AWS S3 | Data lake for CSV files |
| Language | Python 3.11 | Data generation and loading |
| Containerization | Docker & Docker Compose | Local development environment |
| Data Format | CSV | Intermediate storage |
---

## Project Structure
```
trade-surveillance-pipeline/
├── dags/
│   └── surveillance_pipeline.py    # Airflow DAG definition
├── src/
│   ├── generate_orders.py          # Order/execution generation + S3 upload
│   ├── load_data.py                # S3 download + DB loading
│   └── s3_utils.py                 # S3 helper functions
├── sql/
│   └── create_tables.sql           # Database schema
├── data/                           # Local CSV files (temporary)
├── logs/                           # Airflow logs
├── plugins/                        # Airflow plugins
├── docker-compose.yml              # Docker services configuration
├── requirements.txt                # Python dependencies
├── .env                            # Environment variables (not in git)
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

The pipeline is designed to be safely re-run without creating duplicate data. Running the pipeline multiple times for the same date produces the same result.


### Why It Matters

In production, pipelines can fail mid-run and need to be retried. Without idempotency:
- Retries create duplicate records
- Data counts become unreliable
- Downstream reports are wrong

### How It's Implemented

| Layer | Mechanism | Behavior |
|-------|-----------|----------|
| S3 Upload | `file_exists_in_s3()` check | Skip generation if CSV exists in S3 |
| Database Insert | `ON CONFLICT DO NOTHING` | Duplicate primary keys are ignored |
| Validation | Count comparison | Uses `>=` to handle pre-existing data |

### S3 Check
```python
from s3_utils import file_exists_in_s3

if file_exists_in_s3(s3_key):
    print(f"CSV already exists in S3. Skipping generation.")
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

# Verify no duplicates
docker exec -it surveillance_db psql -U surveillance_user -d surveillance_db \
  -c "SELECT DATE(transact_time), COUNT(*) FROM orders GROUP BY 1 ORDER BY 1;"
```

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_DIR` | `./data` (local) or `/opt/airflow/data` (Docker) | Local CSV storage path |
| `DB_HOST` | `localhost` (local) or `postgres` (Docker) | Database host |
| `DB_PORT` | `5433` (local) or `5432` (Docker) | Database port |
| `S3_BUCKET` | - | S3 bucket name |
| `AWS_ACCESS_KEY_ID` | - | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | - | AWS secret key |
| `AWS_REGION` | `us-east-1` | AWS region |

### .env File (Create This)
```bash
# AWS Credentials
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
S3_BUCKET=your-bucket-name

# Airflow Secrets
AIRFLOW__CORE__FERNET_KEY=your-fernet-key
AIRFLOW__WEBSERVER__SECRET_KEY=your-secret-key

# Database
POSTGRES_PASSWORD=surveillance_pass
```

**Important:** Add `.env` to `.gitignore` — never commit credentials.

### Docker Services

| Service | Port | Description |
|---------|------|-------------|
| `airflow_webserver` | 8080 | Airflow UI |
| `airflow_scheduler` | - | DAG scheduling |
| `surveillance_db` | 5433 | PostgreSQL database |

---

## AWS Setup

### Prerequisites

1. AWS account with free tier
2. S3 bucket created
3. IAM user with S3 access

### Create S3 Bucket

1. Go to AWS Console → S3
2. Create bucket (e.g., `trade-surveillance-data-yourname`)
3. Region: `us-east-1`
4. Leave defaults, click Create

### Create IAM User

1. Go to AWS Console → IAM → Users
2. Create user: `surveillance-pipeline-user`
3. Attach policy: `AmazonS3FullAccess`
4. Create access key, save credentials

### Set Billing Alert

1. Go to AWS Console → Budgets
2. Create budget: $1 threshold
3. Add email alert at 80%

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

- [x] **S3 Integration:** CSVs stored in S3 data lake
- [ ] **Cloud Database:** Move PostgreSQL to Supabase
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