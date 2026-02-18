CREATE TABLE [stg].[Oura_SmoothedLocation]
(
	 timestamp            VARCHAR(200) NULL
	,latitude             VARCHAR(200) NULL
	,longitude            VARCHAR(200) NULL
	,altitude             VARCHAR(200) NULL
	,course               VARCHAR(200) NULL
	,course_accuracy      VARCHAR(200) NULL
	,horizontal_accuracy  VARCHAR(200) NULL
	,vertical_accuracy    VARCHAR(200) NULL
	,speed                VARCHAR(200) NULL
	,speed_accuracy       VARCHAR(200) NULL,

	/* Metadata */
	[Meta_Id]           BIGINT IDENTITY (1,1)        NOT NULL,
	[Meta_CreateTime]   DATETIME DEFAULT GETDATE()   NOT NULL,
	[Meta_CreateJob]	BIGINT		                 NOT NULL

	CONSTRAINT PK_Oura_SmoothedLocation_Id PRIMARY KEY CLUSTERED ([Meta_Id])
)
GO