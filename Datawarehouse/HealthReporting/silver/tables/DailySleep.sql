CREATE TABLE [silver].[DailySleep] (
	 [BedtimeStart] DATETIME NULL
	,[BedtimeEnd] DATETIME NULL
	,[SK_Date_BedtimeStart] INT NOT NULL
	,[SK_Time_BedtimeStart] BIGINT NOT NULL
	,[SK_Date_BedtimeEnd] INT NOT NULL
	,[SK_Time_BedtimeEnd] BIGINT NOT NULL
	,[Date] DATE NULL
	,[AverageBreath (min)] DECIMAL(18, 4) NULL
	,[AverageHeartRate] DECIMAL(18, 4) NULL
	,[LowestHeartRate] DECIMAL(18, 4) NULL
	,[AverageHeartRateVariability] DECIMAL(18, 4) NULL
	,[DeepSleepDuration (sec)] DECIMAL(18, 4) NULL
	,[LightSleepDuration (sec)] DECIMAL(18, 4) NULL
	,[RemSleepDuration (sec)] DECIMAL(18, 4) NULL
	,[RestlessPeriods (sec)] DECIMAL(18, 4) NULL
	,[TemperatureDeviation] DECIMAL(18,4) NULL
	,[TemperatureTrendDeviation] DECIMAL(18,4) NULL
	,[SleepEfficiency in %] INT NULL
	,[SleepLatency (sec)] DECIMAL(18, 4) NULL
	,[SleepPeriods] BIT NULL
	,[SleepScore] INT NULL
	,[SleepSegmentState] NVARCHAR(30) NULL
	,[SleepMidpoint (sec)] DECIMAL(18, 4) NULL
	,[TimeInBed (sec)] DECIMAL(18, 4) NULL
	,[TotalSleepDuration (sec)] DECIMAL(18, 4) NULL
	,[AwakeTime (sec)] DECIMAL(18, 4) NULL
	,[SleepType] NVARCHAR(30) NULL
	,[DW_DateTime_Load_Insert]   DATETIME2 (7)    NULL
    ,[DW_Pipeline_run_ID_Insert] UNIQUEIDENTIFIER NULL
);
GO

  CREATE NONCLUSTERED INDEX IX_silver_DailySleep
  ON [silver].[DailySleep]([SK_Date_BedtimeStart] ASC,[SK_Time_BedtimeStart] ASC,[SK_Date_BedtimeEnd] ASC,[SK_Time_BedtimeEnd] ASC)
  GO