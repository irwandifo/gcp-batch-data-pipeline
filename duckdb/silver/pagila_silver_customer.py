import duckdb
from os import getenv
from fsspec import filesystem

con = duckdb.connect(":memory:")
con.register_filesystem(filesystem("gcs"))

con.execute(f"""
  COPY (
    SELECT
      cu.customer_id::INTEGER AS customer_id,
      cu.store_id::INTEGER AS store_id,
      concat(cu.first_name, ' ', cu.last_name) AS full_name,
      cu.email,
      a.phone,
      a.address,
      a.postal_code AS zip_code,
      a.district,
      ci.city,
      co.country,
      cu.activebool AS is_active,
      c.create_date::TIMESTAMPTZ AS created_at,
      c.last_update::TIMESTAMPTZ AS updated_at
    FROM read_parquet('{getenv('GCS_PREFIX')}/customer/*.parquet') cu
    LEFT JOIN read_parquet('{getenv('GCS_PREFIX')}/address/*.parquet') a
      ON c.address_id = a.address_id
    LEFT JOIN read_parquet('{getenv('GCS_PREFIX')}/city/*.parquet') ci
      ON a.city_id = ci.city_id
    LEFT JOIN read_parquet('{getenv('GCS_PREFIX')}/country/*.parquet') co
      ON ci.country_id = co.country_id
  ) TO 'out.parquet' (FORMAT PARQUET, CODEC SNAPPY)
""")

con.close()
