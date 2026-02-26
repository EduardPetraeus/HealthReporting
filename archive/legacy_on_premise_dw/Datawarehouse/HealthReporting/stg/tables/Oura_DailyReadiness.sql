CREATE TABLE [stg].[Oura_DailyReadiness]
(
	 score								 VARCHAR(50) NULL
	,day                                 VARCHAR(50) NULL
	,temperature_deviation               VARCHAR(50) NULL
	,temperature_trend_deviation         VARCHAR(50) NULL
	,contributors_activity_balance       VARCHAR(50) NULL
	,contributors_hrv_balance            VARCHAR(50) NULL
	,contributors_previous_day_activity  VARCHAR(50) NULL
	,contributors_previous_night         VARCHAR(50) NULL
	,contributors_recovery_index         VARCHAR(50) NULL
	,contributors_resting_heart_rate     VARCHAR(50) NULL
	,contributors_sleep_balance          VARCHAR(50) NULL
	,contributors_body_temperature       VARCHAR(50) NULL,

	/* Metadata */
	[Meta_Id]           BIGINT IDENTITY (1,1)        NOT NULL,
	[Meta_CreateTime]   DATETIME DEFAULT GETDATE()   NOT NULL,
	[Meta_CreateJob]	BIGINT		                 NOT NULL

	CONSTRAINT PK_Oura_DailyReadiness_Id PRIMARY KEY CLUSTERED ([Meta_Id])
)
GO