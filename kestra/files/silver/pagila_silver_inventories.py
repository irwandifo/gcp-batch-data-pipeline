import duckdb
from os import getenv
from fsspec import filesystem

con = duckdb.connect(":memory:")
con.register_filesystem(filesystem("gcs"))

GCS_PREFIX = getenv("GCS_PREFIX")

con.execute(f"""
  COPY (
    SELECT
      inventory_id::INTEGER AS inventory_id,
      film_id::INTEGER AS film_id,
      store_id::INTEGER AS store_id,
      last_update::TIMESTAMPTZ AS updated_at,
      loaded_at::TIMESTAMPTZ AS loaded_at
    FROM read_parquet('{GCS_PREFIX}/inventory/*.parquet')
  ) TO 'out.parquet' (FORMAT PARQUET, CODEC SNAPPY)
""")

con.close()
