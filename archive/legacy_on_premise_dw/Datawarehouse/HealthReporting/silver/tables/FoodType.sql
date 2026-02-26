CREATE TABLE [silver].[FoodType] (
	[SK_FoodType]               INT								NOT NULL,
    [Food item]	                NVARCHAR(200)					NULL,	
	[Food type]	                NVARCHAR(200)					NULL,	
	[Food group]	            NVARCHAR(200)					NULL,	
    [UniqueFoodItemKey]         BIGINT							NOT NULL,
	[DW_Datetime_Load]          DATETIME  DEFAULT GETDATE()     NOT NULL,
	[DW_Pipeline_run_ID]        BIGINT							NULL

 CONSTRAINT PK_SK_FoodType PRIMARY KEY CLUSTERED ([SK_FoodType])
);
GO
