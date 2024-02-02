CREATE VIEW [gold].[vw_Fact_BodyTemperature]
AS
SELECT 
       [SK_Date]
      ,[SK_Time]
      ,[BodyTemperature (°C)]
  FROM [silver].[BodyTemperature]