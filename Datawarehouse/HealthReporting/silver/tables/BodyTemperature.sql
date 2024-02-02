  CREATE TABLE [silver].[BodyTemperature] (
	[DateTime]                  DATETIME         NULL,
	[SK_Date]                   INT              NOT NULL,
	[SK_Time]                   BIGINT           NOT NULL,
	[BodyTemperature (°C)]      DECIMAL (18,4)   NULL,
    [DW_DateTime_Load_Insert]   DATETIME2 (7)    NULL,
    [DW_Pipeline_run_ID_Insert] UNIQUEIDENTIFIER NULL
);
GO

  CREATE NONCLUSTERED INDEX IX_silver_BodyTemperature
  ON [silver].[BodyTemperature]([SK_Date] ASC,[SK_Time] ASC)
  GO