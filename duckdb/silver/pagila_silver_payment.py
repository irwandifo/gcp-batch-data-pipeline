import duckdb
from os import getenv
from fsspec import filesystem

con = duckdb.connect(":memory:")
con.register_filesystem(filesystem("gcs"))

con.execute(f"""
  COPY (
    SELECT
      payment_id::INTEGER AS payment_id,
      customer_id::INTEGER AS customer_id,
      staff_id::INTEGER AS staff_id,
      rental_id::INTEGER AS rental_id,
      amount::NUMERIC(5, 2) AS amount,
      payment_date::TIMESTAMPTZ AS paid_at,
      current_timestamp::TIMESTAMPTZ AS loaded_at
    FROM read_parquet('{getenv('GCS_PREFIX')}/payment/*.parquet')
  ) TO 'out.parquet' (FORMAT PARQUET, CODEC SNAPPY)
""")

con.close()
