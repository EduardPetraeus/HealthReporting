CREATE TABLE [stg].[Oura_BloodOxygenLevel]
(
	[Day]			    VARCHAR(50)                 NULL,
	[spo2_percentage]   VARCHAR(50)                 NULL,

	/* Metadata */
	[Meta_Id]           BIGINT IDENTITY (1,1)        NOT NULL,
	[Meta_CreateTime]   DATETIME DEFAULT GETDATE()   NOT NULL,
	[Meta_CreateJob]	BIGINT		                 NOT NULL

	CONSTRAINT PK_Oura_BloodOxygenLevel_Id PRIMARY KEY CLUSTERED ([Meta_Id])
)
GO