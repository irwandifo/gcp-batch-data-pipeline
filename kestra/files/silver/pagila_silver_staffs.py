from utils.duckdb_helper import init_duckdb_gcs
from os import getenv


GCS_PREFIX = getenv("GCS_PREFIX")
con = init_duckdb_gcs()

con.execute(f"""
  COPY (
    SELECT
      staff_id::INTEGER AS staff_id,
      store_id::INTEGER AS store_id,
      address_id::INTEGER AS address_id,
      concat(first_name, ' ', last_name) AS name,
      md5(email) AS email,
      active AS is_active,
      last_update::TIMESTAMPTZ AS updated_at,
      loaded_at::TIMESTAMPTZ AS loaded_at
    FROM read_parquet('{GCS_PREFIX}/staff/*.parquet')
  ) TO 'out.parquet' (FORMAT PARQUET, CODEC SNAPPY)
""")

con.close()
