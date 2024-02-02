  CREATE TABLE [silver].[PulseWaveVelocity] (
	[DateTime]          DATETIME         NULL,
	[SK_Date]           INT              NOT NULL,
	[SK_Time]           BIGINT           NOT NULL,
	[PulseWaveVelocity] DECIMAL (18,4)   NULL,
    [DW_DateTime_Load_Insert]   DATETIME2 (7)    NULL,
    [DW_Pipeline_run_ID_Insert] UNIQUEIDENTIFIER NULL
);
GO

  CREATE NONCLUSTERED INDEX IX_silver_PulseWaveVelocity
  ON [silver].[PulseWaveVelocity]([SK_Date] ASC,[SK_Time] ASC)
  GO