from utils.duckdb_helper import init_duckdb_gcs
from os import getenv


GCS_PREFIX = getenv("GCS_PREFIX")
con = init_duckdb_gcs()

con.execute(f"""
  COPY (
    SELECT
      rental_id::INTEGER AS rental_id,
      inventory_id::INTEGER AS inventory_id,
      customer_id::INTEGER AS customer_id,
      staff_id::INTEGER AS staff_id,
      rental_date::TIMESTAMPTZ AS rented_at,
      return_date::TIMESTAMPTZ AS returned_at,
      last_update::TIMESTAMPTZ AS updated_at,
      loaded_at::TIMESTAMPTZ AS loaded_at
    FROM read_parquet('{GCS_PREFIX}/rental/*/*.parquet')
  ) TO 'out.parquet' (FORMAT PARQUET, CODEC SNAPPY)
""")

con.close()
