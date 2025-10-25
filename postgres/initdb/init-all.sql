-- Create tables.
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,   -- Generated using a sequential integer
    name VARCHAR(255),              -- Generated using full name string
    birthday DATE,                  -- Generated as a date (string format converted to date)
    membership_level VARCHAR(50),   -- One of 'free', 'pro', or 'elite'
    shipping_address TEXT,          -- Full address as string
    created_at TIMESTAMP DEFAULT NOW()  -- Timestamp for creation, defaulting to current time
);

CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,               -- Generated using a sequential integer
    customer_id SERIAL REFERENCES customers(customer_id), -- Lookup to customers table
    product VARCHAR(255),                    -- Generated as a product name string
    cost FLOAT,                              -- (better to use DECIMAL for currency, using float for simplicity)
    description TEXT,                        -- Generated as a paragraph
    created_at TIMESTAMP DEFAULT NOW(),      -- Timestamp for creation, defaulting to current time
    credit_card_number VARCHAR(16)           -- String for credit card number
);
