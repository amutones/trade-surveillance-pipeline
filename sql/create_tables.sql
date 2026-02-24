-- Trade Surveillance Pipeline
-- Database schema modeled on FIX protocol standards

-- Drop tables if rebuilding (order matters due to foreign keys)
DROP TABLE IF EXISTS executions;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS accounts;
DROP TABLE IF EXISTS firms;

-- Firms table
CREATE TABLE firms (
    firm_id VARCHAR(50) PRIMARY KEY,
    firm_name VARCHAR(100) NOT NULL,
    account_type VARCHAR(50) NOT NULL,  -- proprietary, institutional, retail
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Accounts table
CREATE TABLE accounts (
    account_id VARCHAR(50) PRIMARY KEY,
    firm_id VARCHAR(50) NOT NULL REFERENCES firms(firm_id),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Orders table (FIX tags noted in comments)
CREATE TABLE orders (
    cl_ord_id VARCHAR(36) PRIMARY KEY,      -- Tag 11: unique client order ID
    symbol VARCHAR(10) NOT NULL,             -- Tag 55: ticker symbol
    side CHAR(1) NOT NULL,                   -- Tag 54: 1=buy, 2=sell
    order_type CHAR(1) NOT NULL,             -- Tag 40: 1=market, 2=limit
    quantity INTEGER NOT NULL,               -- Tag 38: order quantity
    transact_time TIMESTAMPTZ NOT NULL,        -- Tag 60: order timestamp
    account_id VARCHAR(50) NOT NULL,
    firm_id VARCHAR(50) NOT NULL REFERENCES firms(firm_id),
    ord_status CHAR(1) NOT NULL,             -- Tag 39: 0=new, 1=partial, 2=filled, 4=canceled
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Executions table (FIX tags noted in comments)
CREATE TABLE executions (
    exec_id VARCHAR(36) PRIMARY KEY,         -- Tag 17: unique execution ID
    cl_ord_id VARCHAR(36) NOT NULL REFERENCES orders(cl_ord_id),  -- Tag 11: links to order
    symbol VARCHAR(10) NOT NULL,             -- Tag 55: ticker symbol
    side CHAR(1) NOT NULL,                   -- Tag 54: 1=buy, 2=sell
    fill_qty INTEGER NOT NULL,               -- Tag 32: filled quantity
    fill_price NUMERIC(18,6) NOT NULL,       -- Tag 31: execution price
    transact_time TIMESTAMPTZ NOT NULL,        -- Tag 60: execution timestamp
    venue VARCHAR(20) NOT NULL,              -- Tag 30: execution venue
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common query patterns
CREATE INDEX idx_orders_symbol ON orders(symbol);
CREATE INDEX idx_orders_firm ON orders(firm_id);
CREATE INDEX idx_orders_transact_time ON orders(transact_time);
CREATE INDEX idx_orders_account ON orders(account_id);

CREATE INDEX idx_executions_cl_ord_id ON executions(cl_ord_id);
CREATE INDEX idx_executions_symbol ON executions(symbol);
CREATE INDEX idx_executions_transact_time ON executions(transact_time);
CREATE INDEX idx_executions_venue ON executions(venue);

CREATE INDEX idx_executions_symbol_time ON executions(symbol, transact_time);
CREATE INDEX idx_orders_symbol_time ON orders(symbol, transact_time);

-- Insert seed data for firms
INSERT INTO firms (firm_id, firm_name, account_type) VALUES
    ('FIRM_A', 'Alpha Capital', 'proprietary'),
    ('FIRM_B', 'Beta Investments', 'institutional'),
    ('FIRM_C', 'Gamma Trading', 'retail');