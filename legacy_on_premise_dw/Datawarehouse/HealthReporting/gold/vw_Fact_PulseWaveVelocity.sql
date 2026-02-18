CREATE VIEW [gold].[vw_Fact_PulseWaveVelocity]
AS
SELECT 
       [SK_Date]
      ,[SK_Time]
      ,[PulseWaveVelocity]
  FROM [silver].[PulseWaveVelocity]