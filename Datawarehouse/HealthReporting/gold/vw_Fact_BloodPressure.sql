CREATE VIEW [gold].[vw_Fact_BloodPressure]
AS
SELECT 
       [SK_Date]
      ,[SK_Time]
      ,[Heart rate]
      ,[Systolic]
      ,[Diastolic]
  FROM [silver].[BloodPressure]