CREATE TABLE [stg].[Strava_Training]
(
	 [Activity ID1] 				VARCHAR(200) NULL
	,[Activity DATE1]               VARCHAR(200) NULL
	,[Activity Name1]               VARCHAR(200) NULL
	,[Activity Type1]               VARCHAR(200) NULL
	,[Activity Description]         VARCHAR(200) NULL
	,[Elapsed TIME1]                VARCHAR(200) NULL
	,[Distance1]                    VARCHAR(200) NULL
	,[Max Heart Rate1]              VARCHAR(200) NULL
	,[Relative Effort1]             VARCHAR(200) NULL
	,[Commute1]                     VARCHAR(200) NULL
	,[Activity Private Note]        VARCHAR(200) NULL
	,[Activity Gear]                VARCHAR(200) NULL
	,[Filename]                     VARCHAR(200) NULL
	,[Athlete Weight]               VARCHAR(200) NULL
	,[Bike Weight]                  VARCHAR(200) NULL
	,[Elapsed TIME]                 VARCHAR(200) NULL
	,[Moving TIME]                  VARCHAR(200) NULL
	,[Distance]                     VARCHAR(200) NULL
	,[Max Speed]                    VARCHAR(200) NULL
	,[Average Speed]                VARCHAR(200) NULL
	,[Elevation Gain]               VARCHAR(200) NULL
	,[Elevation Loss]               VARCHAR(200) NULL
	,[Elevation Low]                VARCHAR(200) NULL
	,[Elevation High]               VARCHAR(200) NULL
	,[Max Grade]                    VARCHAR(200) NULL
	,[Average Grade]                VARCHAR(200) NULL
	,[Average Positive Grade]       VARCHAR(200) NULL
	,[Average Negative Grade]       VARCHAR(200) NULL
	,[Max Cadence]                  VARCHAR(200) NULL
	,[Average Cadence]              VARCHAR(200) NULL
	,[Max Heart Rate]               VARCHAR(200) NULL
	,[Average Heart Rate]           VARCHAR(200) NULL
	,[Max Watts]                    VARCHAR(200) NULL
	,[Average Watts]                VARCHAR(200) NULL
	,[Calories]                     VARCHAR(200) NULL
	,[Max Temperature]              VARCHAR(200) NULL
	,[Average Temperature]          VARCHAR(200) NULL
	,[Relative Effort]              VARCHAR(200) NULL
	,[Total WORK]                   VARCHAR(200) NULL
	,[Number OF Runs]               VARCHAR(200) NULL
	,[Uphill TIME]                  VARCHAR(200) NULL
	,[Downhill TIME]                VARCHAR(200) NULL
	,[Other TIME]                   VARCHAR(200) NULL
	,[Perceived Exertion]           VARCHAR(200) NULL
	,[Type]                         VARCHAR(200) NULL
	,[Start TIME]                   VARCHAR(200) NULL
	,[Weighted Average Power]       VARCHAR(200) NULL
	,[Power Count]                  VARCHAR(200) NULL
	,[Prefer Perceived Exertion]    VARCHAR(200) NULL
	,[Perceived Relative Effort]    VARCHAR(200) NULL
	,[Commute]                      VARCHAR(200) NULL
	,[Total Weight Lifted]          VARCHAR(200) NULL
	,[FROM Upload]                  VARCHAR(200) NULL
	,[Grade Adjusted Distance]      VARCHAR(200) NULL
	,[Weather Observation TIME]     VARCHAR(200) NULL
	,[Weather Condition]            VARCHAR(200) NULL
	,[Weather Temperature]          VARCHAR(200) NULL
	,[Apparent Temperature]         VARCHAR(200) NULL
	,[Dewpoint]                     VARCHAR(200) NULL
	,[Humidity]                     VARCHAR(200) NULL
	,[Weather Pressure]             VARCHAR(200) NULL
	,[Wind Speed]                   VARCHAR(200) NULL
	,[Wind Gust]                    VARCHAR(200) NULL
	,[Wind Bearing]                 VARCHAR(200) NULL
	,[Precipitation Intensity]      VARCHAR(200) NULL
	,[Sunrise TIME]                 VARCHAR(200) NULL
	,[Sunset TIME]                  VARCHAR(200) NULL
	,[Moon Phase]                   VARCHAR(200) NULL
	,[Bike]                         VARCHAR(200) NULL
	,[Gear]                         VARCHAR(200) NULL
	,[Precipitation Probability]    VARCHAR(200) NULL
	,[Precipitation Type]           VARCHAR(200) NULL
	,[Cloud Cover]                  VARCHAR(200) NULL
	,[Weather Visibility]           VARCHAR(200) NULL
	,[UV INDEX]                     VARCHAR(200) NULL
	,[Weather Ozone]                VARCHAR(200) NULL
	,[Jump Count]                   VARCHAR(200) NULL
	,[Total Grit]                   VARCHAR(200) NULL
	,[Average Flow]                 VARCHAR(200) NULL
	,[Flagged]                      VARCHAR(200) NULL
	,[Average Elapsed Speed]        VARCHAR(200) NULL
	,[Dirt Distance]                VARCHAR(200) NULL
	,[Newly Explored Distance]      VARCHAR(200) NULL
	,[Newly Explored Dirt Distance] VARCHAR(200) NULL
	,[Activity Count]               VARCHAR(200) NULL
	,[Total Steps]                  VARCHAR(200) NULL
	,[Media]                        VARCHAR(200) NULL,

	/* Metadata */
	[Meta_Id]           BIGINT IDENTITY (1,1)        NOT NULL,
	[Meta_CreateTime]   DATETIME DEFAULT GETDATE()   NOT NULL,
	[Meta_CreateJob]	BIGINT		                 NOT NULL

	CONSTRAINT PK_Strava_Training_Id PRIMARY KEY CLUSTERED ([Meta_Id])
)
GO