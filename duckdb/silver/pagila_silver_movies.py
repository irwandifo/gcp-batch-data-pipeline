import duckdb
from os import getenv
from fsspec import filesystem

con = duckdb.connect(":memory:")
con.register_filesystem(filesystem("gcs"))

GCS_PREFIX = getenv('GCS_PREFIX')

con.execute(f"""
  COPY (
    SELECT
      f.film_id::INTEGER AS film_id,
      f.title,
      f.description,
      f.length,
      f.rating,
      f.rental_rate,
      f.replacement_cost,
      string_agg(c.name, ', ') AS categories,
      string_agg(concat(a.first_name, ' ', a.last_name), ', ') AS actors,
      f.last_update::TIMESTAMPTZ AS updated_at,
      current_timestamp::TIMESTAMPTZ AS loaded_at
    FROM read_parquet('{GCS_PREFIX}/film/*.parquet') f
    LEFT JOIN read_parquet('{GCS_PREFIX}/film_category/*.parquet') fc
      ON f.film_id = fc.film_id
    LEFT JOIN read_parquet('{GCS_PREFIX}/category/*.parquet') c
      ON fc.category_id = c.category_id
    LEFT JOIN read_parquet('{GCS_PREFIX}/film_actor/*.parquet') fa
      ON f.film_id = fa.country_id
    LEFT JOIN read_parquet('{GCS_PREFIX}/actor/*.parquet') a
      ON fa.actor_id = a.actor_id
  ) TO 'out.parquet' (FORMAT PARQUET, CODEC SNAPPY)
""")

con.close()
