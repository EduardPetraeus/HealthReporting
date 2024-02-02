CREATE TABLE [stg].[Oura_DailySleep]
(
	 score                    VARCHAR(50) NULL
	,day                      VARCHAR(50) NULL
	,contributors_timing      VARCHAR(50) NULL
	,contributors_deep_sleep  VARCHAR(50) NULL
	,contributors_restfulness VARCHAR(50) NULL
	,contributors_efficiency  VARCHAR(50) NULL
	,contributors_latency     VARCHAR(50) NULL
	,contributors_rem_sleep   VARCHAR(50) NULL
	,contributors_total_sleep VARCHAR(50) NULL,

	/* Metadata */
	[Meta_Id]           BIGINT IDENTITY (1,1)        NOT NULL,
	[Meta_CreateTime]   DATETIME DEFAULT GETDATE()   NOT NULL,
	[Meta_CreateJob]	BIGINT		                 NOT NULL

	CONSTRAINT PK_Oura_DailySleep_Id PRIMARY KEY CLUSTERED ([Meta_Id])
)
GO