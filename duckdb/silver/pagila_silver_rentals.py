import duckdb
from os import getenv
from fsspec import filesystem

con = duckdb.connect(":memory:")
con.register_filesystem(filesystem("gcs"))

GCS_PREFIX = getenv("GCS_PREFIX")

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
      current_timestamp::TIMESTAMPTZ AS loaded_at
    FROM read_parquet('{GCS_PREFIX}/rental/*/*.parquet')
  ) TO 'out.parquet' (FORMAT PARQUET, CODEC SNAPPY)
""")

con.close()
