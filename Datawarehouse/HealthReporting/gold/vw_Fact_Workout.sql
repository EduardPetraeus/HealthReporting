﻿CREATE VIEW [gold].[vw_Fact_Workout]
AS
SELECT [SK_Date]
      ,[SK_Time]
      ,[SK_ActivityType]
      ,CAST([Max Speed (km/h)]				AS DECIMAL(38,14)) AS [Max Speed (km/h)]
      ,CAST([Average Speed (km/h)]			AS DECIMAL(38,14)) AS [Average Speed (km/h)]
      ,CAST([Average Speed (km/min)]		AS DECIMAL(38,14)) AS [Average Speed (km/min)]
      ,CAST([Average Speed (min/km)]		AS DECIMAL(38,14)) AS [Average Speed (min/km)]
	  ,CAST([Average Elapsed Speed (km/h)] 	AS DECIMAL(38,14)) AS [Average Elapsed Speed (km/h)]
      ,CAST([Elapsed TIME (sec)]			AS DECIMAL(38,14)) AS [Elapsed TIME (sec)]
      ,CAST([Moving TIME (sec)]				AS DECIMAL(38,14)) AS [Moving TIME (sec)]
      ,CAST([Distance (km)]					AS DECIMAL(38,14)) AS [Distance (km)]
      ,CAST([Distance (m)]					AS DECIMAL(38,14)) AS [Distance (m)]
	  ,CAST([Total Steps]					AS DECIMAL(38,14)) AS [Total Steps]
      ,CAST([Max Heart Rate]				AS DECIMAL(38,14)) AS [Max Heart Rate]
      ,CAST([Average Heart Rate]			AS DECIMAL(38,14)) AS [Average Heart Rate]
      ,CAST([Calories]						AS DECIMAL(38,14)) AS [Calories]
      ,CAST([Relative Effort]				AS DECIMAL(38,14)) AS [Relative Effort]
  FROM [silver].[Workout] 