SELECT
  actor_id::INTEGER AS actor_id,
  concat(first_name, ' ', last_name) AS name,
  last_update::TIMESTAMPTZ AS updated_at,
  loaded_at::TIMESTAMPTZ AS loaded_at
FROM read_parquet('{GCS_PREFIX}/actor/*.parquet')
