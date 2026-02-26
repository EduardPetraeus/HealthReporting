CREATE TABLE [silver].[BloodPressure] (
	[DateTime]         DATETIME         NULL,
	[SK_Date]          INT              NOT NULL,
	[SK_Time]          BIGINT           NOT NULL,
	[Heart rate]       INT				NULL,
    [Systolic]         INT				NULL,
    [Diastolic]        INT				NULL,	 		 
	[Comments]         NVARCHAR(50)     NULL,
    [DW_DateTime_Load_Insert]   DATETIME2 (7)    NULL,
    [DW_Pipeline_run_ID_Insert] UNIQUEIDENTIFIER NULL
);
GO

  CREATE NONCLUSTERED INDEX IX_silver_BloodPressure
  ON [silver].[BloodPressure]([SK_Date] ASC,[SK_Time] ASC)
  GO