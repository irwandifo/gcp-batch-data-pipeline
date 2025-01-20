import duckdb
from os import getenv
from fsspec import filesystem

con = duckdb.connect(":memory:")
con.register_filesystem(filesystem("gcs"))

GCS_PREFIX = getenv('GCS_PREFIX')

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
      split(replace(replace(special_features, '{', ''), '}', ''), ',')::STRING[] AS special_features,
      rental_rate::NUMERIC(4, 2) AS rental_rate,
      replacement_cost::NUMERIC(5, 2) AS replacement_cost,
      last_update::TIMESTAMPTZ AS updated_at,
      loaded_at::TIMESTAMPTZ AS loaded_at,
    FROM read_parquet('{GCS_PREFIX}/film/*.parquet')
  ) TO 'out.parquet' (FORMAT PARQUET, CODEC SNAPPY)
""")

con.close()
