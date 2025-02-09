from utils.duckdb_helper import init_duckdb_gcs
from os import getenv


GCS_PREFIX = getenv("GCS_PREFIX")
con = init_duckdb_gcs()

con.execute(f"""
  COPY (
    SELECT
      payment_id::INTEGER AS payment_id,
      rental_id::INTEGER AS rental_id,
      customer_id::INTEGER AS customer_id,
      staff_id::INTEGER AS staff_id,
      amount::NUMERIC(5, 2) AS amount,
      payment_date::TIMESTAMPTZ AS paid_at,
      loaded_at::TIMESTAMPTZ AS loaded_at
    FROM read_parquet('{GCS_PREFIX}/payment/*/*.parquet')
  ) TO 'out.parquet' (FORMAT PARQUET, CODEC SNAPPY)
""")

con.close()
