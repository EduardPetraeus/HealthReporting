CREATE TABLE [silver].[Weight] (
	[DateTime]         DATETIME         NULL,
	[SK_Date]          INT              NOT NULL,
	[SK_Time]          BIGINT           NOT NULL,
	[Weight (kg)]      DECIMAL (18, 4)  NULL,
    [Fat mass (kg)]    DECIMAL (18, 4)  NULL,
    [Bone mass (kg)]   DECIMAL (18, 4)  NULL,
    [Muscle mass (kg)] DECIMAL (18, 4)  NULL,
    [Hydration (kg)]   DECIMAL (18, 4)  NULL,		 		 
	[Comments]         NVARCHAR(50)     NULL,
    [DW_DateTime_Load_Insert]   DATETIME2 (7)    NULL,
    [DW_Pipeline_run_ID_Insert] UNIQUEIDENTIFIER NULL
);
GO

  CREATE NONCLUSTERED INDEX IX_silver_Weight
  ON [silver].[Weight]([SK_Date] ASC,[SK_Time] ASC)
  GO