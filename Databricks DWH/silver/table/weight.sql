CREATE TABLE workspace.silver.weight (
  sk_date INTEGER NOT NULL,
  sk_time STRING NOT NULL,
  datetime TIMESTAMP NOT NULL,
  weight_kg DOUBLE NOT NULL,
  fat_mass_kg DOUBLE,
  bone_mass_kg DOUBLE,
  muscle_mass_kg DOUBLE,
  hydration_kg DOUBLE,
  business_key_hash STRING NOT NULL,
  row_hash STRING NOT NULL,
  load_datetime TIMESTAMP NOT NULL,
  update_datetime TIMESTAMP NOT NULL
)
USING DELTA