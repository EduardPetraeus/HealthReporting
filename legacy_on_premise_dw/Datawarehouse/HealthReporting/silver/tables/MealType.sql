CREATE TABLE [silver].[MealType] (
	[SK_MealType]               INT IDENTITY (1,1)  NOT NULL,
    [Meal type]	                NVARCHAR(50)        NULL,	
	[DW_Datetime_Load]          DATETIME            NOT NULL,
	[DW_Pipeline_run_ID]        UNIQUEIDENTIFIER	NULL

 CONSTRAINT PK_SK_MealType PRIMARY KEY CLUSTERED ([SK_MealType])
);
GO
