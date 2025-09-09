-- raw table to store CSV ingest
CREATE TABLE IF NOT EXISTS public.raw_orders (
  order_id TEXT,
  order_date TEXT, -- store raw string; transform later to date
  customer_id TEXT,
  region TEXT,
  amount NUMERIC,
  profit NUMERIC,
  quantity INTEGER,
  product_id TEXT,
  category TEXT,
  sub_category TEXT
);

-- final marts schema
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS marts.dim_date (
  date_key SERIAL PRIMARY KEY,
  date DATE UNIQUE,
  year INT,
  month INT,
  day INT,
  quarter INT,
  month_name TEXT,
  day_name TEXT
);

CREATE TABLE IF NOT EXISTS marts.dim_product (
  product_key SERIAL PRIMARY KEY,
  product_id TEXT UNIQUE,
  category TEXT,
  sub_category TEXT
);

CREATE TABLE IF NOT EXISTS marts.fct_orders (
  order_key SERIAL PRIMARY KEY,
  order_id TEXT,
  order_date DATE,
  date_key INT,
  product_key INT,
  customer_id TEXT,
  region TEXT,
  amount NUMERIC,
  profit NUMERIC,
  quantity INT
);
