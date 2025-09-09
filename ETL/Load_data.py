# etl/load_data.py
import argparse
import pandas as pd
from etl.utils import get_engine, read_csvs
from sqlalchemy import text

def normalize_and_merge(orders: pd.DataFrame, details: pd.DataFrame) -> pd.DataFrame:
    # Attempt to merge if common order_id exists, otherwise left-join defaults
    if 'order_id' in orders.columns and 'order_id' in details.columns:
        merged = pd.merge(orders, details, on='order_id', how='left', suffixes=('','_d'))
    else:
        merged = orders.copy()
    # Canonical column names expected in DB
    # Try mapping likely column names
    col_map = {}
    for c in merged.columns:
        lc = c.lower()
        if 'order' in lc and 'date' in lc:
            col_map[c] = 'order_date'
        elif lc in ('order_id', 'id'):
            col_map[c] = 'order_id'
        elif 'customer' in lc:
            col_map[c] = 'customer_id'
        elif 'region' in lc:
            col_map[c] = 'region'
        elif 'amount' in lc or 'sales' in lc:
            col_map[c] = 'amount'
        elif 'profit' in lc:
            col_map[c] = 'profit'
        elif 'quantity' in lc or 'qty' in lc:
            col_map[c] = 'quantity'
        elif 'product' in lc and 'id' in lc:
            col_map[c] = 'product_id'
        elif 'category' in lc and 'sub' not in lc:
            col_map[c] = 'category'
        elif 'sub_category' in lc or 'subcategory' in lc or ('sub' in lc and 'category' in lc):
            col_map[c] = 'sub_category'
    merged = merged.rename(columns=col_map)
    # Ensure required columns exist
    for c in ['order_id','order_date','customer_id','region','amount','profit','quantity','product_id','category','sub_category']:
        if c not in merged.columns:
            merged[c] = None
    # Convert numeric columns where possible
    merged['amount'] = pd.to_numeric(merged['amount'], errors='coerce')
    merged['profit'] = pd.to_numeric(merged['profit'], errors='coerce')
    merged['quantity'] = pd.to_numeric(merged['quantity'], errors='coerce').fillna(0).astype(int)
    # Return only required columns in expected order
    return merged[['order_id','order_date','customer_id','region','amount','profit','quantity','product_id','category','sub_category']]

def load_to_postgres(csv_dir: str, engine):
    orders, details = read_csvs(csv_dir)
    merged = normalize_and_merge(orders, details)
    # Use to_sql append into public.raw_orders
    merged.to_sql('raw_orders', engine, schema='public', if_exists='append', index=False, method='multi', chunksize=5000)
    print(f'Loaded {len(merged)} rows into public.raw_orders')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-dir', default='data')
    parser.add_argument('--pg-host', default='localhost')
    parser.add_argument('--pg-port', default=5432)
    parser.add_argument('--pg-db', default='postgres')
    parser.add_argument('--pg-user', default='postgres')
    parser.add_argument('--pg-pass', default='postgres')
    args = parser.parse_args()
    engine = get_engine(args.pg_host, args.pg_port, args.pg_db, args.pg_user, args.pg_pass)
    load_to_postgres(args.csv_dir, engine)
