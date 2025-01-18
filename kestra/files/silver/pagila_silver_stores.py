import duckdb
from os import getenv
from fsspec import filesystem

con = duckdb.connect(":memory:")
con.register_filesystem(filesystem("gcs"))

GCS_PREFIX = getenv("GCS_PREFIX")

con.execute(f"""
  COPY (
    SELECT
      store_id::INTEGER AS store_id,
      manager_staff_id::INTEGER AS manager_staff_id,
      address_id::INTEGER AS address_id,
      last_update::TIMESTAMPTZ AS updated_at,
      loaded_at::TIMESTAMPTZ AS loaded_at
    FROM read_parquet('{GCS_PREFIX}/store/*.parquet')
  ) TO 'out.parquet' (FORMAT PARQUET, CODEC SNAPPY)
""")

con.close()