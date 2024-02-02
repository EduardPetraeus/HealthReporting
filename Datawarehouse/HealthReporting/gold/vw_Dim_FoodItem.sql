CREATE VIEW [gold].[vw_Dim_FoodItem]
AS
SELECT 
	   RTRIM(LTRIM(FI.[SK_FoodItem]))   AS [SK_FoodItem]
      ,RTRIM(LTRIM(FI.[Food item]))     AS [Food item]
      ,RTRIM(LTRIM(FT.[Food type]))     AS [Food type]
      ,RTRIM(LTRIM(FT.[Food group]))    AS [Food group]
  FROM [silver].[FoodItem] FI
  LEFT JOIN [silver].[FoodType] FT
  ON COALESCE(FI.[UniqueFoodItemKey],FI.[SK_FoodItem]) = COALESCE(FT.[UniqueFoodItemKey],FT.[SK_FoodType]) 