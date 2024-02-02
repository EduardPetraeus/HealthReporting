CREATE VIEW [gold].[vw_Fact_HeartRate]
AS
SELECT [SK_Date]
      ,[SK_Time]
      ,[HeartRate]
  FROM [silver].[HeartRate]