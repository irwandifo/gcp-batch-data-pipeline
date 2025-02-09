from utils.duckdb_helper import init_duckdb_gcs
from os import getenv


GCS_PREFIX = getenv("GCS_PREFIX")
con = init_duckdb_gcs()

con.execute(f"""
  COPY (
    SELECT
      film_id::INTEGER AS film_id,
      title,
      description,
      release_year::INTEGER AS release_year,
      rental_duration,
      length::INTEGER AS length,
      rating::STRING AS rating,
      rental_rate::NUMERIC(4, 2) AS rental_rate,
      replacement_cost::NUMERIC(5, 2) AS replacement_cost,
      last_update::TIMESTAMPTZ AS updated_at,
      loaded_at::TIMESTAMPTZ AS loaded_at,
    FROM read_parquet('{GCS_PREFIX}/film/*.parquet')
  ) TO 'out.parquet' (FORMAT PARQUET, CODEC SNAPPY)
""")

con.close()
