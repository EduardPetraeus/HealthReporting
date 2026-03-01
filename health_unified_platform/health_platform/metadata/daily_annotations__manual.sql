MERGE INTO health_dw.silver.daily_annotations AS target
USING (
  SELECT * FROM VALUES
    (20240101, 'social',   'Day off', 'user_1', 1),
    (20240201, 'social',   'Weekend trip', 'user_1', 1),
    (20240301, 'social',   'Holiday vacation', 'user_1', 1),
    (20240401, 'training', 'Completed long run', 'user_1', 1),
    (20240501, 'social',   'Social event', 'user_1', 1),
    (20240601, 'social',   'Conference attendance', 'user_1', 1),
    (20240701, 'training', 'Race day', 'user_1', 1),
    (20240801, 'social',   'Family gathering', 'user_1', 1),
    (20240901, 'training', 'Completed marathon', 'user_1', 1),
    (20241001, 'social',   'Travel day', 'user_1', 1)
  AS source (
    sk_date,
    annotation_type,
    annotation,
    created_by,
    is_valid
  )
) AS source
ON  target.sk_date = source.sk_date
WHEN MATCHED THEN
  UPDATE SET
    target.annotation_type = source.annotation_type,
    target.annotation = source.annotation,
    target.created_by = source.created_by,
    target.is_valid = source.is_valid
WHEN NOT MATCHED THEN
  INSERT (
    sk_date,
    annotation_type,
    annotation,
    created_by,
    is_valid
  )
  VALUES (
    source.sk_date,
    source.annotation_type,
    source.annotation,
    source.created_by,
    source.is_valid
  );
