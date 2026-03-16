-- Demo data for pg-warehouse quickstart

CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    country TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id),
    amount NUMERIC(10,2) NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);

INSERT INTO customers (name, email, country) VALUES
    ('Alice Johnson', 'alice@example.com', 'US'),
    ('Bob Smith', 'bob@example.com', 'UK'),
    ('Carlos Garcia', 'carlos@example.com', 'MX'),
    ('Diana Chen', 'diana@example.com', 'CN'),
    ('Erik Larsson', 'erik@example.com', 'SE'),
    ('Fatima Al-Rashid', 'fatima@example.com', 'AE'),
    ('Giovanni Rossi', 'giovanni@example.com', 'IT'),
    ('Hiroko Tanaka', 'hiroko@example.com', 'JP'),
    ('Ines Santos', 'ines@example.com', 'BR'),
    ('Johan van Dijk', 'johan@example.com', 'NL');

INSERT INTO orders (customer_id, amount, status, created_at, updated_at)
SELECT
    (random() * 9 + 1)::int,
    (random() * 500 + 10)::numeric(10,2),
    (ARRAY['pending', 'completed', 'shipped', 'cancelled'])[floor(random() * 4 + 1)::int],
    now() - (random() * interval '90 days'),
    now() - (random() * interval '30 days')
FROM generate_series(1, 100);
