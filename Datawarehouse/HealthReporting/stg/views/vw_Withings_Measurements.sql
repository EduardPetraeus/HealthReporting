CREATE VIEW [stg].[vw_Withings_Measurements]
AS
-- Pivot table
SELECT -- [date], 
[Meta_CreateTime],[date] AS [Date],[Atrial fibrillation result] ,[Basal Metabolic Rate (BMR)] ,[BIA Error] ,[BMI] ,[BONE_PERCENT] ,[Corrected QT interval duration] ,[Extracellular Water] ,[Fat Free Mass for segments] ,[Fat Free Percent] ,[Fat Mass for segments in mass unit] ,[FAT_FREE_MASS] ,[FAT_PERCENT] ,[HR PNN 50] ,[HR RMS SD] ,[HR SD NN] ,[Impedence 250kHz for segments] ,[Impedence 50kHz for segments] ,[Impedence 5kHz for segments] ,[Impedence 6.25kHz for segments] ,[Intracellular Water] ,[Metabolic Age] ,[Muscle Mass for segments] ,[MUSCLE_PERCENT] ,[Nerve Health Score Feet] ,[Nerve Health Score left foot] ,[Nerve Health Score right foot] ,[Phase  250kHz segment] ,[Phase  50kHz segment] ,[Phase  5kHz segment] ,[Phase  6.25kHz segment] ,[PR interval duration] ,[PWV Reached count] ,[QRS interval duration] ,[QT interval duration] ,[VA Reached count] ,[Valvular heart disease result] ,[Vascular age] ,[Visceral Fat] ,[VO2MAX] ,[WATER_PERCENT]
FROM  
(
  SELECT REPLACE([date],'"','') AS [date], REPLACE([type],'"','') AS [type],REPLACE(REPLACE([value],'"',''),',','') AS [value],[Meta_CreateTime]   
  FROM [stg].[Withings_Measurements]
) AS TableToPivot 
PIVOT  
(  
  MAX([value])-- , MAX([date])
  FOR [type] IN ([Atrial fibrillation result] ,[Basal Metabolic Rate (BMR)] ,[BIA Error] ,[BMI] ,[BONE_PERCENT] ,[Corrected QT interval duration] ,[Extracellular Water] ,[Fat Free Mass for segments] ,[Fat Free Percent] ,[Fat Mass for segments in mass unit] ,[FAT_FREE_MASS] ,[FAT_PERCENT] ,[HR PNN 50] ,[HR RMS SD] ,[HR SD NN] ,[Impedence 250kHz for segments] ,[Impedence 50kHz for segments] ,[Impedence 5kHz for segments] ,[Impedence 6.25kHz for segments] ,[Intracellular Water] ,[Metabolic Age] ,[Muscle Mass for segments] ,[MUSCLE_PERCENT] ,[Nerve Health Score Feet] ,[Nerve Health Score left foot] ,[Nerve Health Score right foot] ,[Phase  250kHz segment] ,[Phase  50kHz segment] ,[Phase  5kHz segment] ,[Phase  6.25kHz segment] ,[PR interval duration] ,[PWV Reached count] ,[QRS interval duration] ,[QT interval duration] ,[VA Reached count] ,[Valvular heart disease result] ,[Vascular age] ,[Visceral Fat] ,[VO2MAX] ,[WATER_PERCENT])  
) AS PivotTable