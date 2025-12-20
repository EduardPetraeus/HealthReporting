MERGE INTO workspace.silver.daily_annotations AS target
USING (
  SELECT * FROM VALUES
    (20240624, 'social',   'Attended the 2024 UEFA European Football Championship', 'claus', 1),
    (20240625, 'social',   'Attended the 2024 UEFA European Football Championship', 'claus', 1),
    (20240929, 'social',   'Attended the 2024 UEFA European Football Championship', 'claus', 1),
    (20240330, 'social',   'Easter vacation', 'claus', 1),
    (20240829, 'social',   'Trip to Sweden', 'claus', 1),
    (20240831, 'social',   'Trip to Sweden', 'claus', 1),
    (20240329, 'social',   'Easter lunch', 'claus', 1),
    (20250913, 'training', 'Ran an ultra marathon', 'claus', 1),
    (20240504, 'social',   'Lunch with friends', 'claus', 1),
    (20251009, 'social',   'Trip to Sweden', 'claus', 1)
  AS source (
    sk_date,
    annotation_type,
    annotation,
    created_by,
    is_valid
  )
) AS source
ON  target.sk_date = source.sk_date
AND target.annotation_type = source.annotation_type
AND target.annotation = source.annotation
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
