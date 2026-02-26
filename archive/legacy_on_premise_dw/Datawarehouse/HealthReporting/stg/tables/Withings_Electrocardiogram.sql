CREATE TABLE [stg].[Withings_Electrocardiogram]
(
	 date		  VARCHAR(MAX) NULL
	,type		  VARCHAR(MAX) NULL
	,frequency    VARCHAR(MAX) NULL
	,duration     VARCHAR(MAX) NULL
	,wearposition VARCHAR(MAX) NULL
	,signal       VARCHAR(MAX) NULL,

	/* Metadata */
	[Meta_Id]           BIGINT IDENTITY (1,1)        NOT NULL,
	[Meta_CreateTime]   DATETIME DEFAULT GETDATE()   NOT NULL,
	[Meta_CreateJob]	BIGINT		                 NOT NULL

	CONSTRAINT PK_Withings_Electrocardiogram_Id PRIMARY KEY CLUSTERED ([Meta_Id])
)
GO