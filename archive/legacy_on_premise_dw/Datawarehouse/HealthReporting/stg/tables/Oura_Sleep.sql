CREATE TABLE [stg].[Oura_Sleep]
(
	 average_breath                                VARCHAR(2000) NULL
	,average_heart_rate                            VARCHAR(2000) NULL
	,average_hrv                                   VARCHAR(2000) NULL
	,awake_time                                    VARCHAR(2000) NULL
	,bedtime_end                                   VARCHAR(2000) NULL
	,bedtime_start                                 VARCHAR(2000) NULL
	,day                                           VARCHAR(2000) NULL
	,deep_sleep_duration                           VARCHAR(2000) NULL
	,efficiency                                    VARCHAR(2000) NULL
	,latency                                       VARCHAR(2000) NULL
	,light_sleep_duration                          VARCHAR(2000) NULL
	,lowest_heart_rate                             VARCHAR(2000) NULL
	,movement_30_sec                               VARCHAR(2000) NULL
	,period                                        VARCHAR(2000) NULL
	,rem_sleep_duration                            VARCHAR(2000) NULL
	,restless_periods                              VARCHAR(2000) NULL
	,score                                         VARCHAR(2000) NULL
	,segment_state                                 VARCHAR(2000) NULL
	,sleep_midpoint                                VARCHAR(2000) NULL
	,time_in_bed                                   VARCHAR(2000) NULL
	,total_sleep_duration                          VARCHAR(2000) NULL
	,type                                          VARCHAR(2000) NULL
	,sleep_phase_5_min                             VARCHAR(2000) NULL
	,timezone                                      VARCHAR(2000) NULL
	,bedtime_start_delta                           VARCHAR(2000) NULL
	,bedtime_end_delta                             VARCHAR(2000) NULL
	,midpoint_at_delta                             VARCHAR(2000) NULL
	,heart_rate_5_min                              VARCHAR(2000) NULL
	,hrv_5_min                                     VARCHAR(2000) NULL
	,contributors_total_sleep                      VARCHAR(2000) NULL
	,contributors_deep_sleep                       VARCHAR(2000) NULL
	,contributors_rem_sleep                        VARCHAR(2000) NULL
	,contributors_efficiency                       VARCHAR(2000) NULL
	,contributors_latency                          VARCHAR(2000) NULL
	,contributors_restfulness                      VARCHAR(2000) NULL
	,contributors_timing                           VARCHAR(2000) NULL
	,readiness_contributors_activity_balance       VARCHAR(2000) NULL
	,readiness_contributors_body_temperature       VARCHAR(2000) NULL
	,readiness_contributors_hrv_balance            VARCHAR(2000) NULL
	,readiness_contributors_previous_day_activity  VARCHAR(2000) NULL
	,readiness_contributors_previous_night         VARCHAR(2000) NULL
	,readiness_contributors_recovery_index         VARCHAR(2000) NULL
	,readiness_contributors_resting_heart_rate     VARCHAR(2000) NULL
	,readiness_contributors_sleep_balance          VARCHAR(2000) NULL
	,readiness_score                               VARCHAR(2000) NULL
	,readiness_temperature_deviation               VARCHAR(2000) NULL
	,readiness_temperature_trend_deviation         VARCHAR(2000) NULL,

	/* Metadata */
	[Meta_Id]           BIGINT IDENTITY (1,1)        NOT NULL,
	[Meta_CreateTime]   DATETIME DEFAULT GETDATE()   NOT NULL,
	[Meta_CreateJob]	BIGINT		                 NOT NULL

	CONSTRAINT PK_Oura_Sleep_Id PRIMARY KEY CLUSTERED ([Meta_Id])
)
GO