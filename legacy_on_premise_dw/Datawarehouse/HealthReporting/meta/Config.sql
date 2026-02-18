CREATE TABLE [meta].[Config] (
	 [ConfigID] INT IDENTITY(1, 1) NOT NULL
	,[Object_Name] NVARCHAR(200) NOT NULL
	,[FilterColumnName] NVARCHAR(50) NULL
	,[DeltaValue] NVARCHAR(500) NULL
	,[DeltaType] NVARCHAR(10) NULL
	,[WaterMarkDate] DATETIME2(7) NULL
	,[CutOffYear] INT NULL
	,[IsActive] BIT NULL
	,[PurgingRequired] BIT NULL
	,[PurgingPeriod(M)] INT NULL
	,[Description] NVARCHAR(500) NULL
	,[DW_DateTime_Load] DATETIME2(7) NOT NULL
	,[DW_Pipeline_run_ID] UNIQUEIDENTIFIER NOT NULL
	,CONSTRAINT [PK_Config] PRIMARY KEY CLUSTERED ([ConfigID] ASC)
	)