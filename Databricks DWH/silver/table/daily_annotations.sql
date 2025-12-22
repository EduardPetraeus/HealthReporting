CREATE TABLE health_dw.silver.daily_annotations (
  sk_date          INT NOT NULL,
  annotation_type  STRING NOT NULL,  -- e.g. 'training', 'incident', 'work', 'travel'
  annotation       STRING NOT NULL,
  created_by       STRING,
  is_valid         BOOLEAN NOT NULL
)
USING DELTA;