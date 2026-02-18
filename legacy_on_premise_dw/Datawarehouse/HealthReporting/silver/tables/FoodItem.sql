CREATE TABLE [silver].[FoodItem] (
	[SK_FoodItem]               INT IDENTITY (1,1)  NOT NULL,
    [Food item]	                NVARCHAR(200)       NULL,	
    [UniqueFoodItemKey]         BIGINT              NOT NULL,
	[DW_Datetime_Load]          DATETIME            NOT NULL,
	[DW_Pipeline_run_ID]        UNIQUEIDENTIFIER	NULL

 CONSTRAINT PK_SK_FoodItem PRIMARY KEY CLUSTERED ([SK_FoodItem])
);
GO
