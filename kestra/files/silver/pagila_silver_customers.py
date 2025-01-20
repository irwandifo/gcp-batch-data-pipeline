import duckdb
from os import getenv
from fsspec import filesystem

con = duckdb.connect(":memory:")
con.register_filesystem(filesystem("gcs"))

GCS_PREFIX = getenv("GCS_PREFIX")

con.execute(f"""
  COPY (
    SELECT
      customer_id::INTEGER AS customer_id,
      store_id::INTEGER AS store_id,
      address_id::INTEGER AS address_id,
      sha256(concat(first_name, ' ', last_name)) AS name,
      sha256(email) AS email,
      activebool AS is_active,
      create_date::TIMESTAMPTZ AS created_at,
      last_update::TIMESTAMPTZ AS updated_at,
      loaded_at::TIMESTAMPTZ AS loaded_at
    FROM read_parquet('{GCS_PREFIX}/customer/*.parquet')
  ) TO 'out.parquet' (FORMAT PARQUET, CODEC SNAPPY)
""")

con.close()
