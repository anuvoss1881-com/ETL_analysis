# etl/transform.py
import argparse
import pandas as pd
from etl.utils import get_engine
from sqlalchemy import text

def build_dim_date(engine):
    # Extract unique dates from raw_orders and populate dim_date
    with engine.begin() as conn:
        # create temporary table of distinct dates
        conn.execute(text("""
            CREATE TEMP TABLE tmp_dates AS
            SELECT DISTINCT (CASE WHEN order_date ~ '^[0-9]' THEN order_date ELSE NULL END) as raw_date
            FROM public.raw_orders
            WHERE order_date IS NOT NULL AND order_date <> '';
        """))
        # Insert parsed dates into dim_date if not exists
        conn.execute(text("""
            INSERT INTO marts.dim_date (date, year, month, day, quarter, month_name, day_name)
            SELECT d::date,
                   EXTRACT(YEAR FROM d)::int,
                   EXTRACT(MONTH FROM d)::int,
                   EXTRACT(DAY FROM d)::int,
                   EXTRACT(QUARTER FROM d)::int,
                   TO_CHAR(d, 'Mon'),
                   TO_CHAR(d, 'Day')
            FROM (
                SELECT DISTINCT TO_TIMESTAMP(raw_date, 'YYYY-MM-DD')::date as d
                FROM tmp_dates
                WHERE raw_date IS NOT NULL
            ) q
            ON CONFLICT (date) DO NOTHING;
        """))

def build_dim_product(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO marts.dim_product (product_id, category, sub_category)
            SELECT DISTINCT product_id, category, sub_category
            FROM public.raw_orders
            WHERE product_id IS NOT NULL
            ON CONFLICT (product_id) DO NOTHING;
        """))

def build_fact_orders(engine):
    with engine.begin() as conn:
        # delete existing fact for idempotency (optional: you may choose incremental logic)
        # We'll insert rows from raw_orders into fct_orders for orders not yet present
        conn.execute(text("""
            INSERT INTO marts.fct_orders (order_id, order_date, date_key, product_key, customer_id, region, amount, profit, quantity)
            SELECT r.order_id,
                   (CASE WHEN r.order_date ~ '^[0-9]' THEN TO_TIMESTAMP(r.order_date, 'YYYY-MM-DD')::date ELSE NULL END) as order_date,
                   d.date_key,
                   p.product_key,
                   r.customer_id,
                   r.region,
                   r.amount::numeric,
                   r.profit::numeric,
                   COALESCE(r.quantity::int,0)
            FROM public.raw_orders r
            LEFT JOIN marts.dim_date d ON d.date = (CASE WHEN r.order_date ~ '^[0-9]' THEN TO_TIMESTAMP(r.order_date, 'YYYY-MM-DD')::date ELSE NULL END)
            LEFT JOIN marts.dim_product p ON p.product_id = r.product_id
            WHERE r.order_id IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM marts.fct_orders f WHERE f.order_id = r.order_id);
        """))

def transform_all(engine):
    print('Building dim_date...')
    build_dim_date(engine)
    print('Building dim_product...')
    build_dim_product(engine)
    print('Building fct_orders...')
    build_fact_orders(engine)
    print('Transforms complete')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--pg-host', default='localhost')
    parser.add_argument('--pg-port', default=5432)
    parser.add_argument('--pg-db', default='postgres')
    parser.add_argument('--pg-user', default='postgres')
    parser.add_argument('--pg-pass', default='postgres')
    args = parser.parse_args()
    engine = get_engine(args.pg_host, args.pg_port, args.pg_db, args.pg_user, args.pg_pass)
    transform_all(engine)
