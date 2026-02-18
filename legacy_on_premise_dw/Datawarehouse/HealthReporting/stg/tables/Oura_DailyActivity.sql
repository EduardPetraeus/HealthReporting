CREATE TABLE [stg].[Oura_DailyActivity]
(
	 active_calories                    VARCHAR(MAX) NULL
	,average_met_minutes                VARCHAR(MAX) NULL
	,day                                VARCHAR(MAX) NULL
	,equivalent_walking_distance        VARCHAR(MAX) NULL
	,high_activity_met_minutes          VARCHAR(MAX) NULL
	,high_activity_time                 VARCHAR(MAX) NULL
	,inactivity_alerts                  VARCHAR(MAX) NULL
	,low_activity_met_minutes           VARCHAR(MAX) NULL
	,low_activity_time                  VARCHAR(MAX) NULL
	,medium_activity_met_minutes        VARCHAR(MAX) NULL
	,medium_activity_time               VARCHAR(MAX) NULL
	,meters_to_target                   VARCHAR(MAX) NULL
	,non_wear_time                      VARCHAR(MAX) NULL
	,resting_time                       VARCHAR(MAX) NULL
	,sedentary_met_minutes              VARCHAR(MAX) NULL
	,sedentary_time                     VARCHAR(MAX) NULL
	,steps                              VARCHAR(MAX) NULL
	,target_calories                    VARCHAR(MAX) NULL
	,target_meters                      VARCHAR(MAX) NULL
	,total_calories                     VARCHAR(MAX) NULL
	,score                              VARCHAR(MAX) NULL
	,class_5_min                        VARCHAR(MAX) NULL
	,contributors_meet_daily_targets    VARCHAR(MAX) NULL
	,contributors_move_every_hour       VARCHAR(MAX) NULL
	,contributors_recovery_time         VARCHAR(MAX) NULL
	,contributors_stay_active           VARCHAR(MAX) NULL
	,contributors_training_frequency    VARCHAR(MAX) NULL
	,contributors_training_volume       VARCHAR(MAX) NULL
	,met_1_min                          VARCHAR(MAX) NULL
	,ring_met_1_min                     VARCHAR(MAX) NULL,

	/* Metadata */
	[Meta_Id]           BIGINT IDENTITY (1,1)        NOT NULL,
	[Meta_CreateTime]   DATETIME DEFAULT GETDATE()   NOT NULL,
	[Meta_CreateJob]	BIGINT		                 NOT NULL

	CONSTRAINT PK_Oura_DailyActivity_Id PRIMARY KEY CLUSTERED ([Meta_Id])
)
GO