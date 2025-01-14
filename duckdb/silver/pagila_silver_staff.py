import duckdb
from os import getenv
from fsspec import filesystem

con = duckdb.connect(":memory:")
con.register_filesystem(filesystem("gcs"))

con.execute(f"""
  COPY (
    SELECT
      s.staff_id::INTEGER AS staff_id,
      s.store_id::INTEGER AS store_id,
      concat(s.first_name, ' ', s.last_name) AS name,
      s.email,
      a.phone::STRING AS phone,
      a.address,
      a.postal_code::STRING AS zip_code,
      a.district,
      ci.city,
      co.country,
      s.active AS is_active,
      s.last_update::TIMESTAMPTZ AS updated_at,
      current_timestamp::TIMESTAMPTZ AS loaded_at
    FROM read_parquet('{getenv('GCS_PREFIX')}/staff/*.parquet') s
    LEFT JOIN read_parquet('{getenv('GCS_PREFIX')}/address/*.parquet') a
      ON s.address_id = a.address_id
    LEFT JOIN read_parquet('{getenv('GCS_PREFIX')}/city/*.parquet') ci
      ON a.city_id = ci.city_id
    LEFT JOIN read_parquet('{getenv('GCS_PREFIX')}/country/*.parquet') co
      ON ci.country_id = co.country_id
  ) TO 'out.parquet' (FORMAT PARQUET, CODEC SNAPPY)
""")

con.close()
