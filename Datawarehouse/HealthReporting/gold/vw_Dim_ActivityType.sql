CREATE VIEW [gold].[vw_Dim_ActivityType]
AS
SELECT [SK_ActivityType]
      ,[Activity Name]
      ,[Activity Type]
  FROM [silver].[ActivityType]