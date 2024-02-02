  CREATE TABLE [silver].[HeartRate] (
	[DateTime]                  DATETIME         NULL,
	[SK_Date]                   INT              NOT NULL,
	[SK_Time]                   BIGINT           NOT NULL,
	[HeartRate]                 INT				 NULL,
	[DataSource]				NVARCHAR(8)     NULL,
    [DW_DateTime_Load_Insert]   DATETIME2 (7)    NULL,
    [DW_Pipeline_run_ID_Insert] UNIQUEIDENTIFIER NULL
);
GO

  CREATE NONCLUSTERED INDEX IX_silver_HeartRate
  ON [silver].[HeartRate]([SK_Date] ASC,[SK_Time] ASC)
  GO