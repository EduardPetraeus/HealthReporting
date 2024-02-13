CREATE VIEW [gold].[vw_Dim_ActivityType]
AS
SELECT [SK_ActivityType]
      ,REPLACE([Activity Name],'Workout','Meditation') AS [Activity Name]
      ,REPLACE([Activity Type],'Workout','Meditation') AS [Activity Type]
  FROM [silver].[ActivityType]