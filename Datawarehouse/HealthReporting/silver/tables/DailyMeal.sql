CREATE TABLE [silver].[DailyMeal] (
	[Date]             DATE          NULL,
	[SK_Date]          INT           NOT NULL,
	[Meal type]        VARCHAR(50)   NULL,
    [SK_MealType]      INT           NOT NULL,
    [Food item]	       NVARCHAR(200) NULL,	
    [SK_FoodItem]      INT           NOT NULL,
    [Amount]           DECIMAL(18,7) NULL,
    [Serving]	       VARCHAR(50)   NULL,
    [Amount in grams]  DECIMAL(18,7) NULL,
    [Calories]		   DECIMAL(18,7) NULL,
    [Carbs]			   DECIMAL(18,7) NULL,
    [Carbs fiber]	   DECIMAL(18,7) NULL,
    [Carbs sugar]	   DECIMAL(18,7) NULL,
    [Fat]			   DECIMAL(18,7) NULL,
    [Fat saturated]	   DECIMAL(18,7) NULL,
    [Fat unsaturated]  DECIMAL(18,7) NULL,
    [Cholesterol]	   DECIMAL(18,7) NULL,
    [Protein]		   DECIMAL(18,7) NULL,
    [Potassium]		   DECIMAL(18,7) NULL,
    [Sodium]		   DECIMAL(18,7) NULL,
	[UniqueTableKey]            BIGINT NOT NULL,
	[DW_Hash_Column]            BIGINT NOT NULL,
	[DW_Datetime_Load]          DATETIME NOT NULL,
	[DW_Pipeline_run_ID]        UNIQUEIDENTIFIER NULL,
	[DW_DateTime_Update]        DATETIME NULL,
    [DW_Pipeline_run_ID_Update] UNIQUEIDENTIFIER NULL
);
GO

  CREATE NONCLUSTERED INDEX IX_silver_DailyMeal
  ON [silver].[DailyMeal]([SK_Date] ASC,[SK_FoodItem] ASC,[SK_MealType] ASC)
  GO