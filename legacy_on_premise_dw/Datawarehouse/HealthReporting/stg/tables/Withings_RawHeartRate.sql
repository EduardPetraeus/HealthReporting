CREATE TABLE [stg].[Withings_RawHeartRate]
(
	 start      VARCHAR(1000) NULL
	,duration   VARCHAR(1000) NULL
	,value      VARCHAR(1000) NULL,

	/* Metadata */
	[Meta_Id]           BIGINT IDENTITY (1,1)        NOT NULL,
	[Meta_CreateTime]   DATETIME DEFAULT GETDATE()   NOT NULL,
	[Meta_CreateJob]	BIGINT		                 NOT NULL

	CONSTRAINT PK_Withings_RawHeartRate_Id PRIMARY KEY CLUSTERED ([Meta_Id])
)
GO