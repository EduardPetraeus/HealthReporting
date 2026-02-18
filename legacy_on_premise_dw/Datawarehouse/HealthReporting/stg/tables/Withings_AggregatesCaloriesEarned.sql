CREATE TABLE [stg].[Withings_AggregatesCaloriesEarned]
(
     [date] 	VARCHAR(50) NULL
    ,[value]    VARCHAR(50) NULL,

	/* Metadata */
	[Meta_Id]           BIGINT IDENTITY (1,1)        NOT NULL,
	[Meta_CreateTime]   DATETIME DEFAULT GETDATE()   NOT NULL,
	[Meta_CreateJob]	BIGINT		                 NOT NULL

	CONSTRAINT PK_Withings_AggregatesCaloriesEarned_Id PRIMARY KEY CLUSTERED ([Meta_Id])
)
GO