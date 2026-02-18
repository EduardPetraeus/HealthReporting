CREATE PROC [silver].[insert_MealType]  AS
/*******************************************************************************************

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
	SET		@EntityName			= N'MealType'
	SET		@SubEntityName		= N'silver.insert_MealType'
	SET		@TableName			= N'silver.MealType'
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
	BEGIN TRY DROP TABLE IF EXISTS [stg].[Intermediate_MealType]  END TRY BEGIN CATCH END CATCH;


	/***** Create the temp Intermediate table*****/

	WITH CTE_MealType AS(
	SELECT
	 RTRIM(LTRIM(ISNULL([Meal type],''))) AS [Meal type]
    --,HASHBYTES('SHA2_256',CAST(RTRIM(LTRIM(ISNULL([Meal type], N'#'))) AS NVARCHAR(32))) AS [UniqueMealTypeHashKey]

	,ROW_NUMBER() OVER ( PARTITION BY RTRIM(LTRIM(ISNULL([Meal type],'#'))) ORDER BY [Meta_CreateTime] DESC ) row_num
	FROM [stg].[Lifesum_DailyMeals]
	)
	SELECT * INTO [stg].[Intermediate_MealType] 
	FROM CTE_MealType
	WHERE 1=1
	AND row_num = 1
	AND [Meal type] <> ''

	/*Count the number of read rows*/
			SET	@RecsRead = @@ROWCOUNT ;

/***** Insert technical unknown record into the main Silver table if it doesn't exist *****/
	IF NOT EXISTS(Select 0 from [silver].[MealType] WHERE [SK_MealType] = 0)
	BEGIN
		SET IDENTITY_INSERT [silver].[MealType] ON

		INSERT INTO [silver].[MealType] 
		(  [SK_MealType]
		  ,[Meal type]
		  ,[DW_Datetime_Load]
		  ,[DW_Pipeline_run_ID]
		  )
			VALUES	 (0,'Unknown',@DW_DateTime_Load,@DW_Pipeline_run_ID)

		SET IDENTITY_INSERT [silver].[MealType] OFF
	END

   /*Insert the new entries to the silver table */
BEGIN TRANSACTION
/*Insert new records */	
	INSERT INTO [silver].[MealType]
	(  [Meal type]
      ,[DW_Datetime_Load]
      ,[DW_Pipeline_run_ID]
	  )

	SELECT  
	   stg.[Meal type]
	  ,@DW_DateTime_Load as DW_DateTime_Load 
	  ,@DW_Pipeline_run_ID as DW_Pipeline_run_ID
	FROM [stg].[Intermediate_MealType] stg	
	LEFT JOIN [silver].[MealType] dest
	ON stg.[Meal type] = dest.[Meal type]
	WHERE dest.[Meal type] IS NULL  

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
	UPDATE STATISTICS [silver].[MealType] ; 
	
	/*Rebuild indexes on the silver table*/
	ALTER INDEX ALL ON [silver].[MealType]  REBUILD;

	/*Drop the working tables*/
	BEGIN TRY DROP TABLE IF EXISTS [stg].[Intermediate_MealType] END TRY BEGIN CATCH END CATCH;

END

END

/************ TEST HARNESS *****************************************************

--DECLARE @DW_Pipeline_run_ID [UNIQUEIDENTIFIER] = NEWID();
EXEC [silver].[insert_MealType] --@DW_Pipeline_run_ID;

*********************************************************************************
--AUDIT 

SELECT *
FROM   meta.[Audit] aud
WHERE  subentityname ='silver.insert_MealType'
ORDER  BY aud.startdatetime DESC 
**********************************************************************************/