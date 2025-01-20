import duckdb
from os import getenv
from fsspec import filesystem

con = duckdb.connect(":memory:")
con.register_filesystem(filesystem("gcs"))

GCS_PREFIX = getenv("GCS_PREFIX")

con.execute(f"""
  COPY (
    SELECT
      cu.customer_id::INTEGER AS customer_id,
      cu.store_id::INTEGER AS store_id,
      sha256(concat(cu.first_name, ' ', cu.last_name)) AS name,
      sha256(cu.email) AS email,
      sha256(a.phone::STRING) AS phone,
      sha256(a.address) AS address,
      a.postal_code::STRING AS zip_code,
      a.district,
      ci.city,
      co.country,
      cu.activebool AS is_active,
      cu.create_date::TIMESTAMPTZ AS created_at,
      cu.last_update::TIMESTAMPTZ AS updated_at,
      current_timestamp::TIMESTAMPTZ AS loaded_at
    FROM read_parquet('{GCS_PREFIX}/customer/*.parquet') cu
    LEFT JOIN read_parquet('{GCS_PREFIX}/address/*.parquet') a
      ON cu.address_id = a.address_id
    LEFT JOIN read_parquet('{GCS_PREFIX}/city/*.parquet') ci
      ON a.city_id = ci.city_id
    LEFT JOIN read_parquet('{GCS_PREFIX}/country/*.parquet') co
      ON ci.country_id = co.country_id
  ) TO 'out.parquet' (FORMAT PARQUET, CODEC SNAPPY)
""")

con.close()
