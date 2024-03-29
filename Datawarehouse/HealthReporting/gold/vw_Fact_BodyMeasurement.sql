﻿CREATE VIEW [gold].[vw_Fact_BodyMeasurement]
AS
SELECT 
       [SK_Date]
      ,[SK_Time]
      ,[Atrial fibrillation result]
      ,[Basal Metabolic Rate (BMR)]
      ,[BMI]
      ,[VO2 Max]
      ,[Nerve Health Score Feet]
      ,[Nerve Health Score left foot]
      ,[Nerve Health Score right foot]
      ,[Water %]
      ,[Bone %]
      ,[Muscle %]
      ,[Fat Free body mass %]
      ,[Fat %]
      ,[Fat Free body mass (kg)]
      ,[Visceral Fat (kg)]
      ,[Vascular age]
      ,[Metabolic Age]
      ,[Extracellular Water]
      ,[Intracellular Water]
      ,[Fat Free Mass for segments]
      ,[Fat Mass for segments in mass unit]
      ,[Muscle Mass for segments]
      ,[Valvular heart disease result]
  FROM [silver].[BodyMeasurement]