CREATE TABLE [stg].[Oura_HeartRate]
(
	[timestamp]		VARCHAR(50) NULL,
	[bpm]		    VARCHAR(50) NULL,
	[source]        VARCHAR(50) NULL,
	[quality]	    VARCHAR(50) NULL,
	[restorative]   VARCHAR(50) NULL,

	/* Metadata */
	[Meta_Id]           BIGINT IDENTITY (1,1)        NOT NULL,
	[Meta_CreateTime]   DATETIME DEFAULT GETDATE()   NOT NULL,
	[Meta_CreateJob]	BIGINT		                 NOT NULL

	CONSTRAINT PK_Oura_HeartRate_Id PRIMARY KEY CLUSTERED ([Meta_Id])
)
GO