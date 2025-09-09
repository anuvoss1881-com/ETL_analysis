#  E-commerce ELT (Postgres + SQL DQ + Python Transforms + Airflow)

## Overview
This repo demonstrates an ELT pipeline:
1. **Python** loads CSVs into Postgres `public.raw_orders`.
2. **SQL** DQ checks run over `raw_orders` (`sql/dq_checks.sql`).
3. **Python transforms** build `marts.dim_date`, `marts.dim_product`, `marts.fct_orders`.
4. **Airflow** DAG orchestrates the steps.
5. **Power BI** connects to Postgres `marts` schema for visualization.

## Quickstart (local, dev)
1. Start Postgres + Airflow (optional):
   ```bash
   docker-compose -f docker/docker-compose.yml up -d
Create tables:

bash
Copy code
psql -h localhost -U postgres -d postgres -f sql/create_tables.sql
Place Orders.csv and Details.csv into data/.

Load raw data:

bash
Copy code
python etl/load_data.py --csv-dir data
Run SQL DQ:

bash
Copy code
python etl/run_sql_dq.py --sql-path sql/dq_checks.sql
Run transforms:

bash
Copy code
python etl/transform.py
Finalize views (optional):

bash
Copy code
psql -h localhost -U postgres -d postgres -f sql/final_views.sql
Power BI
Get Data → PostgreSQL

Host: localhost, Port: 5432, Database: postgres

Select schema: marts (tables: dim_date, dim_product, fct_orders)

Create relationships:

fct_orders.date_key → dim_date.date_key

fct_orders.product_key → dim_product.product_key

Build visuals like time-series profit, pie charts, sub-category profit bars.

CI/Extensions
Add GitHub Actions to run load -> dq -> transform on sample data.

Replace Python transforms with dbt models for SQL-first transforms.

markdown
Copy code

---

# Quick notes & adjustments

- **Date parsing**: The transform expects `order_date` in `YYYY-MM-DD`. If your CSV uses another format (e.g., `MM/DD/YYYY` or `DD-MM-YYYY`) change `TO_TIMESTAMP(..., 'FORMAT')` in `transform.py`.
- **Idempotency**: `transform.py` currently inserts only orders not already in `marts.fct_orders`. For full rebuilds you can `TRUNCATE` the marts before inserts.
- **DQ tuning**: Edit `sql/dq_checks.sql` to adjust thresholds (profit ratio, negative profit threshold etc).
- **Security**: Do **not** keep production DB credentials in code; use environment variables or secrets manager. For Airflow adopt Connections instead of hardcoding.
- **Power BI**: For production, point Power BI to a stable Postgres host; use read-only user.

---

If you want I can now (pick one, and I’ll produce it immediately):

1. Generate a ZIP of this full repo under `/mnt/data/madhav-ecommerce-elt-repo.zip` so you can download and push to GitHub.  
2. Replace Python transforms with dbt models (I’ll give the dbt files and dbt-run Airflow tasks).  
3. Add a small GitHub Actions workflow to run the pipeline on pushes (using the Postgres service).  

Which one do you want me to do next?
