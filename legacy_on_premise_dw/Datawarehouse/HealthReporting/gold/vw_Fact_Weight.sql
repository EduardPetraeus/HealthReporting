CREATE VIEW [gold].[vw_Fact_Weight]
AS
SELECT 
       [SK_Date]
      ,[SK_Time]
      ,[Weight (kg)]
      ,[Fat mass (kg)]
      ,[Bone mass (kg)]
      ,[Muscle mass (kg)]
      ,[Hydration (kg)]
	  ,CAST(([Weight (kg)]/((CAST([Height in CM] AS DECIMAL(18,4))/100) * (CAST([Height in CM] AS DECIMAL(18,4))/100))) AS DECIMAL(18,2)) AS [BMI]
  FROM [silver].[Weight] W
  LEFT JOIN [gold].[vw_MasterData_ClausEduardPetraeus] MD
  ON '1985-08-24' = MD.[Birthday]