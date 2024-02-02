CREATE VIEW [gold].[vw_Fact_BloodOxygenLevel]
AS
SELECT 
       [SK_Date]
      ,[Oxygen saturation (SpO2)]
  FROM [silver].[BloodOxygenLevel]