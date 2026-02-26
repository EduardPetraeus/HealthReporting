CREATE VIEW [gold].[vw_DailySleepDescription]
AS
SELECT 'BedtimeStart' AS [DailySleep ColumnName],'Datetime for start of sleep period' [DailySleep Description]
UNION ALL
SELECT 'BedtimeEnd','Datetime for end of sleep period'
UNION ALL
SELECT 'Date','Date for end of sleep period'
UNION ALL
SELECT 'AverageBreath (min)','Average respiratory rate pr minute'
UNION ALL
SELECT 'AverageHeartRate','Average Heart Rate during sleep period'
UNION ALL
SELECT 'LowestHeartRate','The lowest heart rate (5 minutes sliding average) registered during the sleep period'
UNION ALL
SELECT 'AverageHeartRateVariability','Average Heart Rate Variability during sleep period'
UNION ALL
SELECT 'DeepSleepDuration (sec)','Total amount of deep (N3) sleep registered during the sleep period'
UNION ALL
SELECT 'LightSleepDuration (sec)','Total amount of light (N1 or N2) sleep registered during the sleep period'
UNION ALL
SELECT 'RemSleepDuration (sec)','Total amount of REM sleep registered during the sleep period'
UNION ALL
SELECT 'RestlessPeriods (sec)','Restlessness of the sleep time, time when the user was moving'
UNION ALL
SELECT 'SleepEfficiency in %','Sleep efficiency is the percentage of the sleep period spent asleep (100% * sleep.total / sleep.duration)'
UNION ALL
SELECT 'SleepLatency (sec)','Detected latency from bedtime start to the beginning of the first five minutes of persistent sleep'
UNION ALL
SELECT 'SleepPeriods','Index of the sleep period among sleep periods with the same date, where 0 = first sleep period of the day'
UNION ALL
SELECT 'SleepScore','Sleep score represents overall sleep quality during the sleep period.
It is calculated as a weighted average of sleep score contributors that represent one aspect of sleep quality each. The sleep score contributor values are also available as separate parameters'
UNION ALL
SELECT 'SleepMidpoint (sec)','The time in seconds from the start of sleep to the midpoint of sleep. The midpoint ignores awake periods'
UNION ALL
SELECT 'TimeInBed (sec)','Total duration of the sleep period (sleep.duration = sleep.bedtime_end - sleep.bedtime_start)'
UNION ALL
SELECT 'TotalSleepDuration (sec)','Total amount of sleep registered during the sleep period (sleep.total = sleep.rem + sleep.light + sleep.deep)'
UNION ALL
SELECT 'AwakeTime (sec)','Total amount of awake time registered during the sleep period'
UNION ALL
SELECT 'SleepType','The type of sleep that occurred'

