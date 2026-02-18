CREATE TABLE [stg].[Withings_BloodPressure]
(
	 [Date]		  VARCHAR(50) NULL
    ,[Heart rate] VARCHAR(50) NULL
    ,[Systolic]   VARCHAR(50) NULL
    ,[Diastolic]  VARCHAR(50) NULL
    ,[Comments]   VARCHAR(50) NULL,

	/* Metadata */
	[Meta_Id]           BIGINT IDENTITY (1,1)        NOT NULL,
	[Meta_CreateTime]   DATETIME DEFAULT GETDATE()   NOT NULL,
	[Meta_CreateJob]	BIGINT		                 NOT NULL

	CONSTRAINT PK_Withings_BloodPressure_Id PRIMARY KEY CLUSTERED ([Meta_Id])
)
GO