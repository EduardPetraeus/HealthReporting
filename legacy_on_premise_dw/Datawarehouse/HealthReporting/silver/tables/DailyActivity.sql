  CREATE TABLE [silver].[DailyActivity] (
	[Date]                      DATE             NULL,
	[SK_Date]                   INT              NOT NULL,
	[DailyDistance (m)]         DECIMAL (18, 4)  NULL,
	[DailySteps]	            INT				 NULL,
	[DailyCaloriesPassive]      DECIMAL (38, 6)  NULL,
	[DailyCaloriesActive]       DECIMAL (18, 4)  NULL,
	[DailyCaloriesTotal]        DECIMAL (38, 6)  NULL,
	[DailyActivityScore]        INT			     NULL,
	[DailyReadinessScore]       INT			     NULL,
    [DW_DateTime_Load_Insert]   DATETIME2 (7)    NULL,
    [DW_Pipeline_run_ID_Insert] UNIQUEIDENTIFIER NULL
);
GO

  CREATE NONCLUSTERED INDEX IX_silver_DailyActivity
  ON [silver].[DailyActivity]([SK_Date] ASC)
  GO