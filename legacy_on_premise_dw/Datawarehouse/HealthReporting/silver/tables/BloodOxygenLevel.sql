  CREATE TABLE [silver].[BloodOxygenLevel] (
	[Date]                      DATE             NULL,
	[SK_Date]                   INT              NOT NULL,
	[Oxygen saturation (SpO2)]  DECIMAL (18,4)   NULL,
    [DW_DateTime_Load_Insert]   DATETIME2 (7)    NULL,
    [DW_Pipeline_run_ID_Insert] UNIQUEIDENTIFIER NULL
);
GO

  CREATE NONCLUSTERED INDEX IX_silver_BloodOxygenLevel
  ON [silver].[BloodOxygenLevel]([SK_Date] ASC)
  GO