# Trade Surveillance Pipeline

A data engineering project that simulates trade order generation and surveillance monitoring, built with Apache Airflow, PostgreSQL, and Docker.

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [EC2 Deployment](#ec2-deployment)
- [DAG Workflow](#dag-workflow)
- [Data Model](#data-model)
- [dbt Models](#dbt-models)
- [Idempotency](#idempotency)
- [Configuration](#configuration)
- [Future Enhancements](#future-enhancements)

---

## Overview

This pipeline simulates a trade surveillance system that:

1. **Generates** synthetic FIX-protocol trade orders and executions with inline validation
2. **Stores** data in S3 with date partitioning (raw and staged layers)
3. **Loads** data into PostgreSQL/Supabase (data warehouse layer)
4. **Transforms** data using dbt into analytics-ready models
5. **Validates** data integrity after each load and runs dbt tests after each load

The project demonstrates core data engineering concepts including orchestration, idempotent data loading, date-partitioned 
storage, data transformation with dbt, and separation of concerns between data generation, loading and transformation.

---

## Architecture
```
Airflow DAG (AWS EC2 / Docker)
│
├── check_if_weekend
│   ├── Yes → skip_weekend (end)
│   └── No ↓
│
├── generate_new_orders
│   ├── Generate 1,000 orders + 1,000 executions
│   ├── Inline validation (FIX protocol compliance)
│   ├── Save raw (unsorted) → AWS S3 (raw/orders/YYYY/MM/DD/)
│   └── Save staged (sorted) → AWS S3 (staged/orders/YYYY/MM/DD/)
│
├── verify_s3_files
│   └── Verify raw + staged files exist in S3
│
├── load_orders
│   ├── Download from S3 staged bucket
│   └── Insert to Supabase PostgreSQL (orders, executions, firms)
│
├── validate_data
│   └── Assert counts match
│
└── run_dbt_models
    ├── dbt run
    │   ├── Staging (views)
    │   │   ├── stg_orders
    │   │   └── stg_executions
    │   └── Marts (tables)
    │       ├── fct_daily_trading
    │       └── fct_wash_trade_flags
    └── dbt test
        ├── unique, not_null checks
        └── relationship validations
                │
                ▼
        ┌───────────────┐
        │   Power BI    │
        │   (future)    │
        └───────────────┘
```

S3 Data Lake Structure:
```
s3://trade-surveillance-data-au/
├── raw/
│   ├── orders/
│   │   └── 2026/02/24/
│   │       └── orders_2026-02-24.csv
│   └── executions/
│       └── 2026/02/24/
│           └── executions_2026-02-24.csv
└── staged/
    ├── orders/
    │   └── 2026/02/24/
    │       └── orders_2026-02-24.csv
    └── executions/
        └── 2026/02/24/
            └── executions_2026-02-24.csv
```

Data Flow:
──────────
1. Airflow scheduler triggers DAG (8 AM CT, Mon-Fri)
2. Check if weekend → skip if true
3. Generate orders/executions with validation → upload raw (unsorted) + staged (sorted) to S3
4. Verify all S3 files exist
5. Download from S3 staged → insert to PostgreSQL/Supabase
6. Validate → assert order and execution counts match
7. Run dbt models → create staging views and mart tables
8. Run dbt tests → validate data quality
```

---

## Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Orchestration | Apache Airflow 2.7.1 | Schedule and monitor pipeline |
| Transformation | dbt 1.10.0 | SQL-based data modeling |
| Database | PostgreSQL 15 | Store trade and execution data |
| Cloud Storage | AWS S3 | Data lake with raw/staged layers |
| Compute | AWS EC2 (t3.micro) | Production deployment |
| Language | Python 3.11 | Data generation and loading |
| Containerization | Docker & Docker Compose | Development and deployment |
| Data Format | CSV | Intermediate storage |
---

## Project Structure
```
trade-surveillance-pipeline/
├── dags/
│   └── surveillance_pipeline.py    # Airflow DAG definition
├── src/
│   ├── generate_orders.py          # Order/execution generation + validation + S3 upload
│   ├── load_data.py                # S3 download + DB loading
│   └── s3_utils.py                 # S3 helper functions
├── surveillance_dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml         # Source definitions
│   │   │   ├── schema.yml          # Model tests
│   │   │   ├── stg_orders.sql      # Staged orders view
│   │   │   └── stg_executions.sql  # Staged executions view
│   │   └── marts/
│   │       ├── fct_daily_trading.sql      # Daily trading summary
│   │       └── fct_wash_trade_flags.sql   # Wash trade detection
│   └── dbt_project.yml             # dbt configuration
├── sql/
│   └── create_tables.sql           # Database schema
├── Dockerfile.airflow              # Custom Airflow image with dbt
├── profiles.yml                    # dbt connection profile
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
- AWS Account (for S3)
- Supabase Account (for PostgreSQL)

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

4. **Create .env file**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

5. **Start Docker containers**
   ```bash
   docker-compose up -d
   ```

6. **Wait for services to initialize** (30-60 seconds)

7. **Access Airflow UI**
   - URL: http://localhost:8080
   - Username: `admin`
   - Password: `admin`

### Running the Pipeline

1. Enable the DAG by toggling `trade_surveillance_daily` to ON
2. Click the play button (▶) to trigger manually
3. Monitor task progress in the Airflow UI

### Verify Data

```bash
# Check S3 files
aws s3 ls s3://trade-surveillance-data-au/staged/orders/ --recursive

# Check database records (via Supabase SQL Editor)
SELECT COUNT(*) FROM orders;
SELECT COUNT(*) FROM executions;

# Check dbt models
SELECT * FROM fct_daily_trading LIMIT 10;
SELECT * FROM fct_wash_trade_flags;
```

---

## EC2 Deployment

The pipeline runs on AWS EC2 with auto-start on reboot.

### Instance Details

| Setting | Value |
|---------|-------|
| AMI | Amazon Linux 2023 |
| Instance Type | t3.micro (free tier) |
| Storage | 8 GB gp3 |
| Region | us-east-1 |

### Setup Steps

1. **Launch EC2 instance** via AWS Console

2. **Configure Security Group**
   - SSH (port 22) from your IP
   - Custom TCP (port 8080) from your IP (for Airflow UI)

3. **Connect via SSH**
   ```bash
   ssh -i ~/.ssh/your-key.pem ec2-user@<public-ip>
   ```

4. **Install dependencies**
   ```bash
   sudo yum update -y
   sudo yum install -y docker git
   sudo systemctl start docker
   sudo systemctl enable docker
   sudo usermod -aG docker ec2-user
   
   # Log out and back in
   exit
   ssh -i ~/.ssh/your-key.pem ec2-user@<public-ip>
   
   # Install Docker Compose
   sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
   sudo chmod +x /usr/local/bin/docker-compose

   # Install Docker Buildx (required for custom Dockerfile)
   mkdir -p ~/.docker/cli-plugins
   curl -SL https://github.com/docker/buildx/releases/download/v0.17.1/buildx-v0.17.1.linux-amd64 -o ~/.docker/cli-plugins/docker-buildx
   chmod +x ~/.docker/cli-plugins/docker-buildx
   ```

5. **Clone repo and configure**
   ```bash
   git clone https://github.com/amutones/trade-surveillance-pipeline.git
   cd trade-surveillance-pipeline
   nano .env  # Add your credentials
   chmod -R 777 surveillance_dbt/
   ```

6. **Add swap space** (t3.micro has limited RAM)
   ```bash
   sudo dd if=/dev/zero of=/swapfile bs=128M count=16
   sudo chmod 600 /swapfile
   sudo mkswap /swapfile
   sudo swapon /swapfile
   echo '/swapfile swap swap defaults 0 0' | sudo tee -a /etc/fstab
   ```

7. **Build and start Airflow**
   ```bash
   docker-compose build
   docker-compose up -d
   ```

8. **Configure auto-start on reboot**
   ```bash
   sudo nano /etc/systemd/system/trade-surveillance.service
   ```
   
   Add:
   ```
   [Unit]
   Description=Trade Surveillance Pipeline
   Requires=docker.service
   After=docker.service

   [Service]
   Type=oneshot
   RemainAfterExit=yes
   WorkingDirectory=/home/ec2-user/trade-surveillance-pipeline
   ExecStart=/usr/local/bin/docker-compose up -d
   ExecStop=/usr/local/bin/docker-compose down
   User=ec2-user

   [Install]
   WantedBy=multi-user.target
   ```
   
   Enable the service:
   ```bash
   sudo systemctl enable trade-surveillance.service
   ```

9. **Access Airflow UI**
   - URL: http://<ec2-public-ip>:8080
   - Username: `admin`
   - Password: `admin`

### Notes

- Update security group IP when your home IP changes
- Instance runs 24/7, DAG executes at 8 AM CT Mon-Fri
- Check Airflow UI or Supabase to verify daily runs

---

## DAG Workflow

| Task | Description | Idempotent |
|------|-------------|------------|
| `check_if_weekend` | Branch: skip weekends, proceed on weekdays | N/A |
| `skip_weekend` | Empty task for weekend branch | N/A |
| `generate_new_orders` | Generate orders → validate → upload raw + staged to S3 | ✅ |
| `verify_s3_files` | Check all raw and staged files exist in S3 | ✅ |
| `load_orders` | Download staged CSV from S3 → insert to PostgreSQL | ✅ |
| `validate_data` | Assert database counts match expected | ✅ |
| `run_dbt_models` | Run dbt models and tests | ✅ |

### Schedule

- **Frequency:** Daily at 8:00 AM CT (14:00 UTC)
- **Days:** Monday through Friday
- **Cron:** `0 8 * * 1-5` (with America/Chicago timezone)

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

### Inline Validation

Orders and executions are validated at generation time:

```python
@dataclass
class Order:
    # ... fields ...
    
    def validate(self) -> List[str]:
        errors = []
        if self.symbol not in SYMBOLS:
            errors.append(f"Invalid symbol: {self.symbol}")
        if self.side not in [SIDE_BUY, SIDE_SELL]:
            errors.append(f"Invalid side: {self.side}")
        # ... more validations ...
        return errors
```

### Database Schema

```sql
-- Firms
firms (
    firm_id VARCHAR(50) PRIMARY KEY,
    firm_name VARCHAR(100),
    account_type VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
)

-- Accounts
accounts (
    account_id VARCHAR(50) PRIMARY KEY,
    firm_id VARCHAR(50) REFERENCES firms(firm_id),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
)

-- Orders
orders (
    cl_ord_id VARCHAR(36) PRIMARY KEY,
    symbol VARCHAR(10),
    side CHAR(1),
    order_type CHAR(1),
    quantity INTEGER,
    transact_time TIMESTAMPTZ,
    account_id VARCHAR(50),
    firm_id VARCHAR(50) REFERENCES firms(firm_id),
    ord_status CHAR(1),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
)

-- Executions
executions (
    exec_id VARCHAR(36) PRIMARY KEY,
    cl_ord_id VARCHAR(36) REFERENCES orders(cl_ord_id),
    symbol VARCHAR(10),
    side CHAR(1),
    fill_qty INTEGER,
    fill_price NUMERIC(18,6),
    transact_time TIMESTAMPTZ,
    venue VARCHAR(20),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
)

-- Composite indexes for surveillance queries
CREATE INDEX idx_orders_symbol_time ON orders(symbol, transact_time);
CREATE INDEX idx_executions_symbol_time ON executions(symbol, transact_time);
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

## dbt Models

dbt transforms raw data into analytics-ready models.

### Model Layers

| Layer | Materialization | Purpose |
|-------|-----------------|---------|
| Staging | View | Clean and standardize raw data |
| Marts | Table | Business-ready aggregations |

### Staging Models

**stg_orders** - Cleaned orders with readable labels
```sql
SELECT
    cl_ord_id,
    symbol,
    CASE WHEN side = '1' THEN 'BUY' ELSE 'SELL' END as side_label,
    quantity,
    transact_time,
    DATE(transact_time) as trade_date,
    firm_id
FROM orders
```

**stg_executions** - Cleaned executions with calculated fields
```sql
SELECT
    exec_id,
    cl_ord_id,
    symbol,
    CASE WHEN side = '1' THEN 'BUY' ELSE 'SELL' END as side_label,
    fill_qty,
    fill_price,
    fill_qty * fill_price as notional_value,
    transact_time,
    venue
FROM executions
```

### Mart Models

**fct_daily_trading** - Daily trading summary by firm and symbol
| Column | Description |
|--------|-------------|
| trade_date | Trading date |
| firm_id | Firm identifier |
| symbol | Stock symbol |
| side_label | BUY or SELL |
| order_count | Number of orders |
| total_shares | Total shares traded |
| total_notional | Total dollar value |
| avg_fill_price | Average execution price |
| venues_used | Number of venues used |

**fct_wash_trade_flags** - Potential wash trade detection
| Column | Description |
|--------|-------------|
| trade_date | Trading date |
| firm_id | Firm identifier |
| symbol | Stock symbol |
| buy_shares | Total shares bought |
| sell_shares | Total shares sold |
| wash_trade_flag | TRUE if firm bought AND sold same symbol same day |

### dbt Tests

| Test | Model | Column | Description |
|------|-------|--------|-------------|
| unique | stg_orders | cl_ord_id | No duplicate orders |
| not_null | stg_orders | cl_ord_id, symbol | Required fields |
| accepted_values | stg_orders | side_label | Only BUY or SELL |
| unique | stg_executions | exec_id | No duplicate executions |
| relationships | stg_executions | cl_ord_id | FK to stg_orders |

### Running dbt Manually

```bash
cd surveillance_dbt

# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
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
| S3 Verify | `verify_s3_files` task | Confirm all files present before loading |
| Database Insert | `ON CONFLICT DO NOTHING` | Duplicate primary keys are ignored |
| Validation | Count comparison | Uses `>=` to handle pre-existing data |
| dbt | Incremental rebuild | Tables recreated with current data |

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
# - dbt models rebuild successfully

# Verify no duplicates (via Supabase SQL Editor)
SELECT DATE(transact_time), COUNT(*) FROM orders GROUP BY 1 ORDER BY 1;
```

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_DIR` | `./data` (local) or `/opt/airflow/data` (Docker) | Local CSV storage path |
| `DB_HOST` | `localhost` | Supabase pooler host |
| `DB_PORT` | `5432` | 6543 for Supabase pooler |
| `DB_NAME` | `postgres` | Database name |
| `DB_USER` | `postgres` | Database user |
| `DB_PASSWORD` | - | Database password (required) |
| `S3_BUCKET` | - | S3 bucket name (required) |
| `AWS_ACCESS_KEY_ID` | - | AWS credentials (required) |
| `AWS_SECRET_ACCESS_KEY` | - | AWS credentials (required) |
| `AWS_REGION` | `us-east-1` | AWS region |

### .env File (Create This)
```bash
# AWS Credentials
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-east-1
S3_BUCKET=your-bucket-name

# Supabase Database
DB_HOST=aws-0-us-east-1.pooler.supabase.com
DB_PORT=6543
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=your-supabase-password

# Airflow Secrets
AIRFLOW__CORE__FERNET_KEY=your-fernet-key
AIRFLOW__WEBSERVER__SECRET_KEY=your-secret-key

# Local Database (Docker)
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

## Cloud Infrastructure

### AWS S3 (Data Lake)

**Setup:**
1. Go to AWS Console → S3
2. Create bucket (e.g., `trade-surveillance-data-yourname`)
3. Region: `us-east-1`


### AWS IAM

1. Go to AWS Console → IAM → Users
2. Create user: `surveillance-pipeline-user`
3. Attach policy: `AmazonS3FullAccess`
4. Create access key, save credentials

### AWS Billing Alert

1. Go to AWS Console → Budgets
2. Create budget: $1 threshold
3. Add email alert at 80%

### Supabase PostgreSQL (Database)

**Setup:**
1. Create account at supabase.com
2. Create new project
3. Run `sql/create_tables.sql` in SQL Editor
4. Use Transaction pooler connection string

**Connection:**
- Host: `aws-X-us-east-1.pooler.supabase.com`
- Port: `6543`
- Database: `postgres`

```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| DAG not appearing | Check scheduler logs: `docker logs airflow_scheduler` |
| Permission denied on ./data | Run: `sudo chown -R 50000:50000 ./data` |
| Permission denied on dbt | Run: `chmod -R 777 surveillance_dbt/` |
| Database connection failed | Verify PostgreSQL is running: `docker ps` |
| Port 8080 in use | Stop other services or change port in docker-compose.yml |
| EC2 SSH hangs | Update security group with your current IP |
| EC2 out of memory | Add swap space (see EC2 Deployment section) |
| S3 upload fails | Check AWS credentials in .env |
| dbt run fails | Check `surveillance_dbt/logs/dbt.log` |

---

## Future Enhancements

- [x] **S3 Integration:** CSVs stored in S3 data lake
- [x] **Cloud Database:** PostgreSQL on Supabase
- [x] **Date Partitioning:** S3 paths use YYYY/MM/DD structure
- [x] **Inline Validation:** Orders/executions validated at generation
- [x] **Raw/Staged Layers:** Separate unsorted (raw) and sorted (staged) data
- [x] **NUMERIC Precision:** fill_price uses NUMERIC(18,6)
- [x] **TIMESTAMPTZ:** All timestamps timezone-aware
- [x] **Composite Indexes:** Optimized for symbol + timestamp queries
- [x] **S3 Verification:** DAG verifies files before loading
- [x] **EC2 Deployment:** Airflow running on AWS EC2 with auto-start
- [x] **dbt Transformations:** Staging and mart layers
- [x] **Surveillance Logic:** Wash trade detection flags
- [ ] **Power BI Dashboard:** Visualize alerts and metrics
- [ ] **Kafka Streaming:** Real-time order processing (Java/Spring Boot)

---

## Author

Tony - Data Engineer

---

## License

This project is for portfolio and educational purposes.