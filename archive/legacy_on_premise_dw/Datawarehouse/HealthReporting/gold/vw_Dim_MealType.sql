CREATE VIEW [gold].[vw_Dim_MealType]
AS
SELECT [SK_MealType]
      ,CASE 
		 WHEN [Meal type] = 'dinner' THEN 'Dinner'
		 WHEN [Meal type] = 'lunch'  THEN 'Lunch'
		 WHEN [Meal type] = 'snack'  THEN 'Snack'
		ELSE [Meal type] END AS [Meal type]
  FROM [silver].[MealType]