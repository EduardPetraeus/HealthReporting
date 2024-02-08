CREATE PROC [silver].[upsert_DailyMeal]  AS
/*******************************************************************************************
Procedure to load Meal data into the silver layer
*********************************************************************************************/
BEGIN

	/*Declare the variables*/	
	DECLARE @DW_DateTime_Load		datetime2(7),
			@DW_StartDatetime		datetime2(7),
			@EntityName				varchar(100),
			@SubEntityName			varchar(100),
			@TableName				varchar(100),
			@AuditID				bigint,
			@RecsRead				bigint = 0,
			@RecsUpdated			bigint = 0,
			@RecsDeleted			bigint = 0,
			@RecsInserted			bigint = 0,
			@CutOffDate				date,
			@CutOffDateKey			int,
			@CurrentUser			nvarchar(100),
			@Today					Int,
			@DW_Pipeline_run_ID [UNIQUEIDENTIFIER] = NEWID()
			;

	/*Set the variables*/
	SET		DATEFORMAT ymd;
	SET		NOCOUNT ON;
	SET		@DW_DateTime_Load	= SYSDATETIME()
	SET		@EntityName			= N'DailyMeal'
	SET		@SubEntityName		= N'silver.upsert_DailyMeal'
	SET		@TableName			= N'silver.DailyMeal'
	SET		@DW_StartDatetime	= SYSDATETIME()
	SET		@Today				= CONVERT(INT,REPLACE(CONVERT(DATE,SYSDATETIME()),'-',''))
	;

	SELECT	@CurrentUser  =  CAST(suser_name() AS nvarchar(100)) 
	-- SELECT original_login(),suser_name(), suser_sname(), system_user, session_user,  current_user, user_name()

	/*Insert the Audit entry for Load start*/
	INSERT INTO [meta].[Audit] 
		([EntityName], [SubEntityName], [TableName], [LoadStatus], [StartDateTime], [ExecutionID], [ExecutedBy])
	VALUES 
		(@EntityName, @SubEntityName, @TableName, 'InProgress', @DW_StartDatetime, @DW_Pipeline_run_ID, @CurrentUser);

	SELECT @AuditID = MAX(AuditID) 
	FROM [meta].[Audit] 
	WHERE TableName = @TableName
		AND [SubEntityName] = @SubEntityName
		AND [ExecutionID]	= @DW_Pipeline_run_ID
		AND [EndDateTime] IS NULL;


	 
	BEGIN TRY
	
	-- This code part can be used in Synapse and Fabric
	--/*Drop the working tables if they exist*/
	--	BEGIN TRY DROP TABLE [<schema>].[<table>] END TRY BEGIN CATCH END CATCH;
	--
	--	/*Insert the records in the Intermediate working table*/		
	--	CREATE TABLE [<schema>].[<table>]
	--	WITH
	--	(
	--		DISTRIBUTION = HASH (SK_Product),
	--		CLUSTERED COLUMNSTORE INDEX
	--	)
	--	AS
	--	SELECT 
	--		<column>,
	--		<column>,
	--		<column>
	--	FROM [<schema>].[<table>] bs
	/***** Drop the temp Intermediate table if exists*****/
	BEGIN TRY DROP TABLE IF EXISTS [stg].[DailyMeal_Clean]  END TRY BEGIN CATCH END CATCH;
	
	/***** Create the temp Intermediate table*****/
	WITH CTE_DailyMeal AS(
	SELECT 
       CAST(NULLIF([Date],'') AS DATE) AS [Date]
	  ,CAST(SUBSTRING(REPLACE(REPLACE(CAST([Date] AS DATE),'"',''),'-',''),1,8)AS INT) AS [DateCode]
      ,NULLIF([Meal type],'') AS [Meal type]
	  ,RTRIM(LTRIM(NULLIF([Title],''))) AS [Food item]
      ,CAST(HASHBYTES('SHA2_256',CAST(UPPER(RTRIM(LTRIM(ISNULL([Title], N'#')))) AS NVARCHAR(200))) AS BIGINT) AS [UniqueFoodItemKey]
      ,CAST(NULLIF([Amount],'') AS DECIMAL(18,7)) AS [Amount]
      ,NULLIF([Serving],'') AS [Serving] 
      ,CAST(NULLIF([Amount in grams],'') AS DECIMAL(18,7)) AS [Amount in grams]
      ,CAST(NULLIF([Calories],'') AS DECIMAL(18,7)) AS [Calories]
      ,CAST(NULLIF([Carbs],'') AS DECIMAL(18,7)) AS [Carbs]
      ,CAST(NULLIF([Carbs fiber],'') AS DECIMAL(18,7)) AS [Carbs fiber]
      ,CAST(NULLIF([Carbs sugar],'') AS DECIMAL(18,7)) AS [Carbs sugar]
      ,CAST(NULLIF([Fat],'') AS DECIMAL(18,7))			AS [Fat]	
      ,CAST(NULLIF([Fat saturated],'') AS DECIMAL(18,7)) AS [Fat saturated]
      ,CAST(NULLIF([Fat unsaturated],'') AS DECIMAL(18,7)) AS [Fat unsaturated]
      ,CAST(NULLIF([Cholesterol],'') AS DECIMAL(18,7)) AS [Cholesterol]
      ,CAST(NULLIF([Protein],'') AS DECIMAL(18,7)) AS [Protein]
      ,CAST(NULLIF([Potassium],'') AS DECIMAL(18,7)) AS [Potassium]
      ,CAST(NULLIF(REPLACE(REPLACE(Sodium,CHAR(10),''),CHAR(13),''),'') AS DECIMAL(18,7)) AS [Sodium]
	  ,ROW_NUMBER() OVER ( PARTITION BY [Date] ,[Meal type] ,[Title] ,[Amount] ,[Serving] ,[Amount in grams] ,[Calories] ,[Carbs] 
	  ,[Carbs fiber] ,[Carbs sugar] ,[Fat] ,[Fat saturated] ,[Fat unsaturated] ,[Cholesterol] ,[Protein] ,[Potassium] ,[Sodium] ORDER BY [Meta_CreateTime] DESC ) row_num
  FROM [stg].[Lifesum_DailyMeals]
	)
	SELECT * INTO [stg].[DailyMeal_Clean] 
	FROM CTE_DailyMeal
	WHERE 1=1
	AND row_num = 1
	AND [Date] IS NOT NULL
	/***** Drop the temp Intermediate table if exists*****/
	BEGIN TRY DROP TABLE IF EXISTS [stg].[Intermediate_DailyMeal]  END TRY BEGIN CATCH END CATCH;


	/***** Create the temp Intermediate table*****/

	
	SELECT 
	   DMC.[Date]
	  ,ISNULL(DD.[SK_Date],0) AS [SK_Date]
      ,DMC.[Meal type]
	  ,ISNULL(MT.[SK_MealType],0) AS [SK_MealType]
      ,DMC.[Food item]
      ,ISNULL(FI.[SK_FoodItem],0) AS [SK_FoodItem]
      ,DMC.[Amount]
      ,DMC.[Serving]
      ,DMC.[Amount in grams]
      ,DMC.[Calories]
      ,DMC.[Carbs]
      ,DMC.[Carbs fiber]
      ,DMC.[Carbs sugar]
      ,DMC.[Fat]
      ,DMC.[Fat saturated]
      ,DMC.[Fat unsaturated]
      ,DMC.[Cholesterol]
      ,DMC.[Protein]
      ,DMC.[Potassium]
      ,DMC.[Sodium] 
	  ,CAST(
		HASHBYTES('SHA2_256', CONCAT (
				ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Date]))) AS NVARCHAR), N'#')
				,'|'				
				,ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Meal type]))) AS NVARCHAR), N'#')
				,'|'
				,ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Food item]))) AS NVARCHAR), N'#')
				)) 
		AS BIGINT) 
		AS [UniqueTableKey]
	  ,CAST(
		HASHBYTES('SHA2_256', CONCAT (
				ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Date]))) AS NVARCHAR), N'#')
				,'|'				
				,ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Meal type]))) AS NVARCHAR), N'#')
				,'|'
				,ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Food item]))) AS NVARCHAR), N'#')
				,'|'
				,ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Amount]))) AS NVARCHAR), N'#')
				,'|'							
				,ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Serving]))) AS NVARCHAR), N'#')
				,'|'
				,ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Amount in grams]))) AS NVARCHAR), N'#')
				,'|'
				,ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Calories]))) AS NVARCHAR), N'#')
				,'|'
				,ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Carbs]))) AS NVARCHAR), N'#')
				,'|'
				,ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Carbs fiber]))) AS NVARCHAR), N'#')
				,'|'
				,ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Carbs sugar]))) AS NVARCHAR), N'#')
				,'|'
				,ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Fat]))) AS NVARCHAR), N'#')
				,'|'
				,ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Fat saturated]))) AS NVARCHAR), N'#')
				,'|'
				,ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Fat unsaturated]))) AS NVARCHAR), N'#')
				,'|'
				,ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Cholesterol]))) AS NVARCHAR), N'#')
				,'|'
				,ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Protein]))) AS NVARCHAR), N'#')
				,'|'
				,ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Potassium]))) AS NVARCHAR), N'#')
				,'|'
				,ISNULL(CAST(UPPER(RTRIM(LTRIM(DMC.[Sodium]))) AS NVARCHAR), N'#')
				)) 
		AS BIGINT) 
		AS [DW_Hash_Column]
	INTO [stg].[Intermediate_DailyMeal] 
	FROM [stg].[DailyMeal_Clean] DMC
	
	LEFT JOIN [silver].[Dim_Date] DD 
    ON DMC.[DateCode] = DD.[SK_Date]	

	LEFT JOIN [silver].[MealType] MT
	ON DMC.[Meal type] = MT.[Meal type]
	
	LEFT JOIN [silver].[FoodItem] FI
	ON DMC.[UniqueFoodItemKey] = FI.[UniqueFoodItemKey]


	/*Count the number of read rows*/
			SET	@RecsRead = @@ROWCOUNT ;



   /*Insert the new entries to the silver table */
BEGIN TRANSACTION

/*Update existing records that has changed*/	

UPDATE [silver].[DailyMeal]
	SET  
	   [Date]            = src.[Date]           
      ,[SK_Date]         = src.[SK_Date]        
      ,[Meal type]       = src.[Meal type]      
      ,[SK_MealType]     = src.[SK_MealType]    
      ,[Food item]       = src.[Food item]      
      ,[SK_FoodItem]     = src.[SK_FoodItem]    
      ,[Amount]          = src.[Amount]         
      ,[Serving]         = src.[Serving]        
      ,[Amount in grams] = src.[Amount in grams]
      ,[Calories]        = src.[Calories]       
      ,[Carbs]           = src.[Carbs]          
      ,[Carbs fiber]     = src.[Carbs fiber]    
      ,[Carbs sugar]     = src.[Carbs sugar]    
      ,[Fat]             = src.[Fat]            
      ,[Fat saturated]   = src.[Fat saturated]  
      ,[Fat unsaturated] = src.[Fat unsaturated]
      ,[Cholesterol]     = src.[Cholesterol]    
      ,[Protein]         = src.[Protein]        
      ,[Potassium]       = src.[Potassium]      
      ,[Sodium]			 = src.[Sodium]
      ,[UniqueTableKey]			 = src.[UniqueTableKey]
	  ,[DW_Hash_Column]			 = src.[DW_Hash_Column]
	  ,[DW_DateTime_Update]		 = @DW_DateTime_Load
	  ,[DW_Pipeline_run_ID_Update] = @DW_Pipeline_run_ID 

	FROM [silver].[DailyMeal] dest 
	INNER JOIN  [stg].[Intermediate_DailyMeal] src	
	ON dest.[UniqueTableKey] = src.[UniqueTableKey]
	AND dest.[DW_Hash_Column] <> src.[DW_Hash_Column]

	SET	@RecsUpdated = @@ROWCOUNT ;

/*Insert new records */	
	INSERT INTO [silver].[DailyMeal] 
	(  
	    [Date]
	   ,[SK_Date]
	   ,[Meal type]
	   ,[SK_MealType]
	   ,[Food item]
	   ,[SK_FoodItem]
	   ,[Amount]
	   ,[Serving]
	   ,[Amount in grams]
	   ,[Calories]
	   ,[Carbs]
	   ,[Carbs fiber]
	   ,[Carbs sugar]
	   ,[Fat]
	   ,[Fat saturated]
	   ,[Fat unsaturated]
	   ,[Cholesterol]
	   ,[Protein]
	   ,[Potassium]
	   ,[Sodium]
	   ,[UniqueTableKey]
	   ,[DW_Hash_Column]
	   ,[DW_Datetime_Load]
	   ,[DW_Pipeline_run_ID]
	   ,[DW_DateTime_Update]
	   ,[DW_Pipeline_run_ID_Update]
	  )

	SELECT  
	   stg.[Date]           
      ,stg.[SK_Date]        
      ,stg.[Meal type]      
      ,stg.[SK_MealType]    
      ,stg.[Food item]      
      ,stg.[SK_FoodItem]    
      ,stg.[Amount]         
      ,stg.[Serving]        
      ,stg.[Amount in grams]
      ,stg.[Calories]       
      ,stg.[Carbs]          
      ,stg.[Carbs fiber]    
      ,stg.[Carbs sugar]    
      ,stg.[Fat]            
      ,stg.[Fat saturated]  
      ,stg.[Fat unsaturated]
      ,stg.[Cholesterol]    
      ,stg.[Protein]        
      ,stg.[Potassium]      
      ,stg.[Sodium]			
	  ,stg.[UniqueTableKey]
	  ,stg.DW_Hash_Column
	  ,@DW_DateTime_Load as DW_DateTime_Load 
	  ,@DW_Pipeline_run_ID as DW_Pipeline_run_ID
	  ,@DW_DateTime_Load as DW_DateTime_Update 
	  ,@DW_Pipeline_run_ID as DW_Pipeline_run_ID_Update
	FROM [stg].[Intermediate_DailyMeal] stg	
	LEFT JOIN [silver].[DailyMeal] dest 
	ON stg.[UniqueTableKey] = dest.[UniqueTableKey]
	WHERE dest.[UniqueTableKey] IS NULL  

SET	@RecsInserted = @@ROWCOUNT ;

 COMMIT TRANSACTION

			
		
	/*Log the data row count details in the Audit table*/
	UPDATE [meta].[Audit]
	SET	   [LoadStatus]		= 'Completed'
		  ,[EndDateTime]	= SYSDATETIME()
		  ,[RowsRead]		= @RecsRead
		  ,[RowsInserted]	= @RecsInserted
		  ,[RowsUpdated]	= @RecsUpdated
		  ,[RowsDeleted]	= @RecsDeleted
	WHERE AuditID = @AuditID;


END TRY

BEGIN CATCH
	IF @@TRANCOUNT>0
	/*Update the Audit table with the error ID from the Error Log*/
	BEGIN
	 Rollback Transaction
	END
	/*Update the Audit table with the error ID from the Error Log*/
	UPDATE [meta].[Audit]
	SET	   [LoadStatus]		= 'Failed'
		  ,[EndDateTime]	= SYSDATETIME()
		  ,[RowsRead]		= @RecsRead
		  ,[RowsInserted]	= 0
		  ,[RowsUpdated]	= 0
		  ,[RowsDeleted]	= 0
		  ,[ErrorCode]		= ERROR_NUMBER()
		  ,[ErrorMessage]	= ERROR_MESSAGE()
	WHERE AuditID = @AuditID;
	
	/*Throw the run time error for better visibility*/
	;THROW

END CATCH


BEGIN /*Maintenance and cleanup*/

	/*update the statistics on the table*/
	UPDATE STATISTICS [silver].[DailyMeal] ; 
	
	/*Rebuild indexes on the silver table*/
	ALTER INDEX ALL ON [silver].[DailyMeal]  REBUILD;

	/*Drop the working tables*/
	BEGIN TRY DROP TABLE IF EXISTS [stg].[DailyMeal_Clean]  END TRY BEGIN CATCH END CATCH;
	BEGIN TRY DROP TABLE IF EXISTS [stg].[Intermediate_DailyMeal]  END TRY BEGIN CATCH END CATCH;

END

END

/************ TEST HARNESS *****************************************************

--DECLARE @DW_Pipeline_run_ID [UNIQUEIDENTIFIER] = NEWID();
EXEC [silver].[upsert_DailyMeal] --@DW_Pipeline_run_ID;

*********************************************************************************
--AUDIT 

SELECT *
FROM   meta.[Audit] aud
WHERE  subentityname ='silver.upsert_DailyMeal'
ORDER  BY aud.startdatetime DESC 
**********************************************************************************/