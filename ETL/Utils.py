# etl/utils.py
import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

def get_engine(host='localhost', port=5432, db='postgres', user='postgres', password='postgres') -> Engine:
    url = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}'
    return create_engine(url, pool_pre_ping=True)

def read_csvs(csv_dir: str):
    orders_path = os.path.join(csv_dir, 'Orders.csv')
    details_path = os.path.join(csv_dir, 'Details.csv')
    orders = pd.read_csv(orders_path, dtype=str, keep_default_na=False)
    details = pd.read_csv(details_path, dtype=str, keep_default_na=False)
    # Normalize columns to lowercase/underscores
    orders.columns = [c.strip().lower().replace(' ', '_') for c in orders.columns]
    details.columns = [c.strip().lower().replace(' ', '_') for c in details.columns]
    return orders, details

def run_sql_file(engine: Engine, sql_path: str):
    with open(sql_path, 'r') as f:
        sql = f.read()
    with engine.begin() as conn:
        conn.execute(text(sql))
