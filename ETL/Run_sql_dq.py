# etl/run_sql_dq.py
import argparse
import pandas as pd
from etl.utils import get_engine
from sqlalchemy import text
import sys

def run_dq(engine, sql_path: str):
    with engine.begin() as conn:
        result = conn.execute(text(open(sql_path, 'r').read()))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    print('DQ check results:')
    print(df.to_string(index=False))
    failed = df[df['failing_count'].astype(int) > 0]
    if not failed.empty:
        print('DQ FAILED - failing checks found')
        raise SystemExit(2)
    print('All DQ checks passed')
    return df

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--pg-host', default='localhost')
    parser.add_argument('--pg-port', default=5432)
    parser.add_argument('--pg-db', default='postgres')
    parser.add_argument('--pg-user', default='postgres')
    parser.add_argument('--pg-pass', default='postgres')
    parser.add_argument('--sql-path', default='sql/dq_checks.sql')
    args = parser.parse_args()
    engine = get_engine(args.pg_host, args.pg_port, args.pg_db, args.pg_user, args.pg_pass)
    run_dq(engine, args.sql_path)
