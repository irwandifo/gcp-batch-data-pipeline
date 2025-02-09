from utils.duckdb_helper import init_duckdb_gcs
from os import getenv


GCS_PREFIX = getenv("GCS_PREFIX")
con = init_duckdb_gcs()

con.execute(f"""
  COPY (
    SELECT
      actor_id::INTEGER AS actor_id,
      concat(first_name, ' ', last_name) AS name,
      last_update::TIMESTAMPTZ AS updated_at,
      loaded_at::TIMESTAMPTZ AS loaded_at
    FROM read_parquet('{GCS_PREFIX}/actor/*.parquet')
  ) TO 'out.parquet' (FORMAT PARQUET, CODEC SNAPPY)
""")

con.close()
