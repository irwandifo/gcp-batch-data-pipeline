import duckdb
from os import getenv
from fsspec import filesystem

con = duckdb.connect(":memory:")
con.register_filesystem(filesystem("gcs"))

GCS_PREFIX = getenv("GCS_PREFIX")

con.execute(f"""
  COPY (
    SELECT
      a.address_id::INTEGER AS address_id,
      a.address,
      a.address2,
      a.district,
      a.postal_code::STRING AS zip_code,
      ci.city,
      co.country,
      a.phone::STRING AS phone,
      a.last_update::TIMESTAMPTZ AS updated_at,
      a.loaded_at::TIMESTAMPTZ AS loaded_at
    FROM read_parquet('{GCS_PREFIX}/address/*.parquet') a
    LEFT JOIN read_parquet('{GCS_PREFIX}/city/*.parquet') ci
      ON a.city_id = ci.city_id
    LEFT JOIN read_parquet('{GCS_PREFIX}/country/*.parquet') co
      ON ci.country_id = co.country_id
  ) TO 'out.parquet' (FORMAT PARQUET, CODEC SNAPPY)
""")

con.close()
