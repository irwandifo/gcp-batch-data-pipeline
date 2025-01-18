import duckdb
from os import getenv
from fsspec import filesystem

con = duckdb.connect(":memory:")
con.register_filesystem(filesystem("gcs"))

GCS_PREFIX = getenv("GCS_PREFIX")

con.execute(f"""
  COPY (
    SELECT
      category_id::INTEGER AS category_id,
      name,
      last_update::TIMESTAMPTZ AS updated_at,
      loaded_at::TIMESTAMPTZ AS loaded_at
    FROM read_parquet('{GCS_PREFIX}/category/*.parquet')
  ) TO 'out.parquet' (FORMAT PARQUET, CODEC SNAPPY)
""")

con.close()
