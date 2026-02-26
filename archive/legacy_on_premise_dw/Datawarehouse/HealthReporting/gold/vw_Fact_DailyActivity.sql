CREATE VIEW [gold].[vw_Fact_DailyActivity]
AS 
SELECT 
       
	   DACY.[SK_Date]
	   ,DACY.[DailyCaloriesPassive] AS [DailyCaloriesPassive]
	  --,DALY.[DailyCaloriesPassive] AS [DailyCaloriesPassive LY]
      ,DACY.[DailyCaloriesActive]  AS [DailyCaloriesActive]
	  --,DALY.[DailyCaloriesActive]  AS [DailyCaloriesActive LY]
      ,DACY.[DailyCaloriesTotal]   AS [DailyCaloriesTotal]
	  --,DALY.[DailyCaloriesTotal]   AS [DailyCaloriesTotal LY]
      ,DACY.[DailyDistance (m)]    AS [DailyDistance (m) CY]
	  ,DALY.[DailyDistance (m)]    AS [DailyDistance (m) LY]
      ,DACY.[DailySteps]           AS [DailySteps CY]
	  ,DALY.[DailySteps]           AS [DailySteps LY]
      ,DACY.[DailyActivityScore]   AS [DailyActivityScore CY]
	  ,DALY.[DailyActivityScore]   AS [DailyActivityScore LY]
      ,DACY.[DailyReadinessScore]  AS [DailyReadinessScore CY]
	  ,DALY.[DailyReadinessScore]  AS [DailyReadinessScore LY]
	  ,DALY.[SK_Date] AS [SK_Date LY]

  FROM [silver].[DailyActivity] DACY
  LEFT JOIN [silver].[DailyActivity] DALY
  ON DATEADD(YEAR, -1, DACY.[Date]) = DALY.[Date]