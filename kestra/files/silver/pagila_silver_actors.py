import duckdb
from os import getenv
from fsspec import filesystem

con = duckdb.connect(":memory:")
con.register_filesystem(filesystem("gcs"))

GCS_PREFIX = getenv("GCS_PREFIX")

con.execute(f"""
  COPY (
    SELECT
      actor_id::INTEGER AS actor_id,
      concat(cu.first_name, ' ', cu.last_name) AS name,
      last_update::TIMESTAMPTZ AS updated_at,
      loaded_at::TIMESTAMPTZ AS loaded_at
    FROM read_parquet('{GCS_PREFIX}/actor/*.parquet')
  ) TO 'out.parquet' (FORMAT PARQUET, CODEC SNAPPY)
""")

con.close()
