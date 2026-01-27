CREATE VIEW health_dw.gold.vw_heart_rate_avg_per_day AS
SELECT
  sk_date,
  AVG(bpm) AS avg_bpm
FROM health_dw.silver.heart_rate
GROUP BY sk_date;