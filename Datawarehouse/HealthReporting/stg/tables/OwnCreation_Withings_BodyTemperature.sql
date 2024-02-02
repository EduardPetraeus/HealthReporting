CREATE TABLE [stg].[OwnCreation_Withings_BodyTemperature]
(
	 date             VARCHAR(50) NULL
    ,value            VARCHAR(50) NULL,

	/* Metadata */
	[Meta_Id]           BIGINT IDENTITY (1,1)        NOT NULL,
	[Meta_CreateTime]   DATETIME DEFAULT GETDATE()   NOT NULL,
	[Meta_CreateJob]	BIGINT		                 NULL

	CONSTRAINT PK_OwnCreation_Withings_BodyTemperature_Id PRIMARY KEY CLUSTERED ([Meta_Id])
)
GO