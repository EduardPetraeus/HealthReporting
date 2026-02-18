CREATE TABLE [silver].[Workout] (
	[Activity month-day-year] VARCHAR(200) NULL
	,[Activity DateTime] DATETIME NULL
	,[SK_Date] INT NOT NULL
	,[SK_Time] BIGINT NOT NULL
	,[Activity Name] VARCHAR(200) NULL
	,[Activity Type] VARCHAR(200) NULL
	,[UniqueActivityTypeKey] BIGINT NULL
	,[SK_ActivityType] INT NOT NULL
	,[Max Speed (km/h)] NUMERIC(21, 9) NULL
	,[Average Elapsed Speed (km/h)] NUMERIC(21, 9) NULL
	,[Average Speed (km/h)] DECIMAL(38, 14) NULL
	,[Average Speed (km/min)] DECIMAL(38, 17) NOT NULL
	,[Average Speed (min/km)] DECIMAL(38, 15) NOT NULL
	,[Total Steps] VARCHAR(200) NULL
	,[Elapsed TIME (sec)] VARCHAR(200) NULL
	,[Moving TIME (sec)] VARCHAR(200) NULL
	,[Distance (km)] VARCHAR(200) NULL
	,[Distance (m)] VARCHAR(200) NULL
	,[Max Heart Rate] VARCHAR(200) NULL
	,[Average Heart Rate] VARCHAR(200) NULL
	,[Calories] VARCHAR(200) NULL
	,[Relative Effort] VARCHAR(200) NULL
	,[Commute] VARCHAR(200) NULL
	,[Filename] VARCHAR(200) NULL
	,[Athlete Weight] VARCHAR(200) NULL
	,[Activity Description] VARCHAR(200) NULL
	,[Elapsed TIME extra column (sec)] VARCHAR(200) NULL
	,[Max Heart Rate extra column] VARCHAR(200) NULL
	,[Elevation Gain] VARCHAR(200) NULL
	,[Elevation Loss] VARCHAR(200) NULL
	,[Elevation Low] VARCHAR(200) NULL
	,[Elevation High] VARCHAR(200) NULL
	,[Max Grade] VARCHAR(200) NULL
	,[Average Grade] VARCHAR(200) NULL
	,[Average Positive Grade] VARCHAR(200) NULL
	,[Average Negative Grade] VARCHAR(200) NULL
	,[Max Cadence] VARCHAR(200) NULL
	,[Average Cadence] VARCHAR(200) NULL
	,[Max Watts] VARCHAR(200) NULL
	,[Average Watts] VARCHAR(200) NULL
	,[Max Temperature] VARCHAR(200) NULL
	,[Average Temperature] VARCHAR(200) NULL
	,[Relative Effort extra column] VARCHAR(200) NULL
	,[Total WORK] VARCHAR(200) NULL
	,[Number OF Runs] VARCHAR(200) NULL
	,[Uphill TIME] VARCHAR(200) NULL
	,[Downhill TIME] VARCHAR(200) NULL
	,[Other TIME] VARCHAR(200) NULL
	,[Perceived Exertion] VARCHAR(200) NULL
	,[Type] VARCHAR(200) NULL
	,[Start TIME] VARCHAR(200) NULL
	,[Weighted Average Power] VARCHAR(200) NULL
	,[Power Count] VARCHAR(200) NULL
	,[Prefer Perceived Exertion] VARCHAR(200) NULL
	,[Perceived Relative Effort] VARCHAR(200) NULL
	,[Commute extra column] VARCHAR(200) NULL
	,[Total Weight Lifted] VARCHAR(200) NULL
	,[FROM Upload] VARCHAR(200) NULL
	,[Grade Adjusted Distance] VARCHAR(200) NULL
	,[Weather Observation TIME] VARCHAR(200) NULL
	,[Weather Condition] VARCHAR(200) NULL
	,[Weather Temperature] VARCHAR(200) NULL
	,[Apparent Temperature] VARCHAR(200) NULL
	,[Dewpoint] VARCHAR(200) NULL
	,[Humidity] VARCHAR(200) NULL
	,[Weather Pressure] VARCHAR(200) NULL
	,[Wind Speed] VARCHAR(200) NULL
	,[Wind Gust] VARCHAR(200) NULL
	,[Wind Bearing] VARCHAR(200) NULL
	,[Precipitation Intensity] VARCHAR(200) NULL
	,[Sunrise TIME] VARCHAR(200) NULL
	,[Sunset TIME] VARCHAR(200) NULL
	,[Moon Phase] VARCHAR(200) NULL
	,[Bike] VARCHAR(200) NULL
	,[Gear] VARCHAR(200) NULL
	,[Precipitation Probability] VARCHAR(200) NULL
	,[Precipitation Type] VARCHAR(200) NULL
	,[Cloud Cover] VARCHAR(200) NULL
	,[Weather Visibility] VARCHAR(200) NULL
	,[UV INDEX] VARCHAR(200) NULL
	,[Weather Ozone] VARCHAR(200) NULL
	,[Jump Count] VARCHAR(200) NULL
	,[Total Grit] VARCHAR(200) NULL
	,[Average Flow] VARCHAR(200) NULL
	,[Flagged] VARCHAR(200) NULL
	,[Dirt Distance] VARCHAR(200) NULL
	,[Newly Explored Distance] VARCHAR(200) NULL
	,[Newly Explored Dirt Distance] VARCHAR(200) NULL
	,[Activity Count] VARCHAR(200) NULL
	,[Media] VARCHAR(200) NULL
	,[DW_DateTime_Load_Insert] DATETIME2(7) NULL
	,[DW_Pipeline_run_ID_Insert] UNIQUEIDENTIFIER NULL
);
GO

  CREATE NONCLUSTERED INDEX IX_silver_Workout
  ON [silver].[Workout]([SK_Date] ASC,[SK_Time] ASC,[SK_ActivityType]ASC)
  GO