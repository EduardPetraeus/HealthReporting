CREATE TABLE [stg].[Withings_Activities]
(
     [from]			  VARCHAR(MAX) NULL
    ,[to]             VARCHAR(MAX) NULL
    ,[from (manual)]  VARCHAR(MAX) NULL
    ,[to (manual)]    VARCHAR(MAX) NULL
    ,[Timezone]       VARCHAR(MAX) NULL
    ,[Activity type]  VARCHAR(MAX) NULL
    ,[Data]           VARCHAR(MAX) NULL
    ,[GPS]            VARCHAR(MAX) NULL
    ,[Modified]       VARCHAR(MAX) NULL,

	/* Metadata */
	[Meta_Id]           BIGINT IDENTITY (1,1)        NOT NULL,
	[Meta_CreateTime]   DATETIME DEFAULT GETDATE()   NOT NULL,
	[Meta_CreateJob]	BIGINT		                 NOT NULL

	CONSTRAINT PK_Withings_Activities_Id PRIMARY KEY CLUSTERED ([Meta_Id])
)
GO