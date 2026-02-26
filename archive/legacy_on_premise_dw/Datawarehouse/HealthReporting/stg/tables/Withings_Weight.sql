CREATE TABLE [stg].[Withings_Weight]
(
	  [Date]             VARCHAR(50) NULL
     ,[Weight (kg)]      VARCHAR(50) NULL
     ,[Fat mass (kg)]    VARCHAR(50) NULL
     ,[Bone mass (kg)]   VARCHAR(50) NULL
     ,[Muscle mass (kg)] VARCHAR(50) NULL
     ,[Hydration (kg)]   VARCHAR(50) NULL
     ,[Comments]		 VARCHAR(50) NULL,

	/* Metadata */
	[Meta_Id]           BIGINT IDENTITY (1,1)        NOT NULL,
	[Meta_CreateTime]   DATETIME DEFAULT GETDATE()   NOT NULL,
	[Meta_CreateJob]	BIGINT		                 NOT NULL

	CONSTRAINT PK_Withings_Weight_Id PRIMARY KEY CLUSTERED ([Meta_Id])
)
GO