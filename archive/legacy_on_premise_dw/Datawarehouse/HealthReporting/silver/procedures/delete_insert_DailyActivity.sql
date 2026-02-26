CREATE PROC [silver].[delete_insert_DailyActivity]  AS
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
			@CutOffDateInt			int,
			@DeltaType				nvarchar(10),
			@DeltaValue				int,
			@WaterMarkDate			datetime2(7),
			@IncrementalLoadDateTime	datetime,
			@CutOffYear					int,
			@PurgingRequired			int,
			@IsCompleted			    bit
			,@DW_Pipeline_run_ID [UNIQUEIDENTIFIER] = NEWID()
			;

	/*Set the variables*/
	SET		DATEFORMAT ymd;
	SET		NOCOUNT ON;
	SET		@DW_DateTime_Load	= SYSDATETIME()
	SET		@EntityName			= N'DailyActivity'
	SET		@SubEntityName		= N'silver.delete_insert_DailyActivity'
	SET		@TableName			= N'silver.DailyActivity'
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

   /*Number of delta years/days/months to go back is specified in the [meta].[Config]*/
	SELECT @DeltaType =ISNULL(DeltaType,'YEAR') ,@DeltaValue= ISNULL(DeltaValue,100), @WaterMarkDate= CAST(ISNULL(WaterMarkDate,getdate()) as Datetime),@CutOffYear= ISNULL(CutOffYear,100)  FROM [meta].[Config] WHERE [Object_Name] ='silver.DailyActivity' and IsActive=1

	--SELECT @DeltaType
	--SELECT @DeltaValue
	--SELECT @WaterMarkDate
	--SELECT @CutOffYear

	/*We take data from the start of the month specified in the previous step*/
	SELECT @CutOffDate= CONVERT(Nvarchar(30),DATEADD(YEAR, -@CutOffYear, DATEADD(YEAR, DATEDIFF(YEAR, 0, GETDATE()), 0)),23)

	SELECT @CutOffDateInt=CONVERT(INT,REPLACE(TRY_CONVERT(DATE,@CutOffDate),'-',''));

	SELECT @IncrementalLoadDateTime= CASE 
				WHEN UPPER(@DeltaType) ='HOURLY' THEN CAST(DATEADD(HH,-CAST(@DeltaValue as int),@WaterMarkDate )as Datetime) 
				WHEN UPPER(@DeltaType) ='MONTH' THEN CAST(DATEADD(MONTH, -CAST(@DeltaValue as int), DATEADD(MONTH, DATEDIFF(MONTH, 0, @WaterMarkDate), 0))as Datetime)
				WHEN UPPER(@DeltaType) ='YEAR' THEN CAST(DATEADD(YEAR, -CAST(@DeltaValue as int), DATEADD(YEAR, DATEDIFF(YEAR, 0, @WaterMarkDate), 0)) as Datetime)
				WHEN UPPER(@DeltaType) ='DAY' THEN CAST(DATEADD(DD,-CAST(@DeltaValue as int),@WaterMarkDate )as Datetime)
				END;
   --SELECT @CutOffDate
   --SELECT @IncrementalLoadDateTime
   --SELECT @CutOffYear	
	 
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

	
/*Drop the working table if they exist and insert into new clean table*/
	BEGIN TRY DROP TABLE IF EXISTS [stg].[Withings_AggregatesSteps_Clean] END TRY BEGIN CATCH END CATCH;

	WITH CTE_Withings_AggregatesSteps 
	AS(
	SELECT 
	 CAST([date] AS DATE) AS [Date]
	,CAST(SUBSTRING(REPLACE(REPLACE([date],'"',''),'-',''),1,8)AS INT) AS [DateCode]
	,CAST(NULLIF([value],'') AS INT) AS [Withings_DailySteps]
	,ROW_NUMBER() OVER (
		PARTITION BY [date]
		,[value]
		 ORDER BY [Meta_CreateTime] DESC
		) row_num
FROM [stg].[Withings_AggregatesSteps]
	)

	SELECT * INTO [stg].[Withings_AggregatesSteps_Clean]
   FROM CTE_Withings_AggregatesSteps
   WHERE row_num = 1
   ;	
/*Finished creating new clean table*/


/*Drop the working table if they exist and insert into new clean table*/
	BEGIN TRY DROP TABLE IF EXISTS [stg].[Withings_AggregatesDistance_Clean] END TRY BEGIN CATCH END CATCH;

	WITH CTE_Withings_AggregatesDistance 
	AS(
	SELECT 
	 CAST([date] AS DATE) AS [Date]
	,CAST(SUBSTRING(REPLACE(REPLACE([date],'"',''),'-',''),1,8)AS INT) AS [DateCode]
	,CAST(NULLIF([value],'') AS DECIMAL(18,4)) AS [Withings_DailyDistance in meter]
	,ROW_NUMBER() OVER (
		PARTITION BY [date]
		,[value]
		 ORDER BY [Meta_CreateTime] DESC
		) row_num
FROM [stg].[Withings_AggregatesDistance]
	)

	SELECT * INTO [stg].[Withings_AggregatesDistance_Clean]
   FROM CTE_Withings_AggregatesDistance
   WHERE row_num = 1
   ;	
/*Finished creating new clean table*/

/*Drop the working table if they exist and insert into new clean table*/
	BEGIN TRY DROP TABLE IF EXISTS [stg].[Withings_AggregatesCaloriesPassive_Clean] END TRY BEGIN CATCH END CATCH;

	WITH CTE_Withings_AggregatesCaloriesPassive 
	AS(
	SELECT 
	 CAST([date] AS DATE) AS [Date]
	,CAST(SUBSTRING(REPLACE(REPLACE([date],'"',''),'-',''),1,8)AS INT) AS [DateCode]
	,COALESCE(NULLIF(CAST([value] AS DECIMAL(18,4)),0),(SELECT AVG(CAST([value] AS DECIMAL(18,4))) FROM [HealthReporting].[stg].[Withings_AggregatesCaloriesPassive])) AS [Withings_DailyCaloriesPassive]
	,ROW_NUMBER() OVER (
		PARTITION BY [date]
		,[value]
		 ORDER BY [Meta_CreateTime] DESC
		) row_num
FROM [stg].[Withings_AggregatesCaloriesPassive]
	)

	SELECT * INTO [stg].[Withings_AggregatesCaloriesPassive_Clean]
   FROM CTE_Withings_AggregatesCaloriesPassive
   WHERE row_num = 1
   ;	
/*Finished creating new clean table*/

/*Drop the working table if they exist and insert into new clean table*/
	BEGIN TRY DROP TABLE IF EXISTS [stg].[Withings_AggregatesCaloriesEarned_Clean] END TRY BEGIN CATCH END CATCH;

	WITH CTE_Withings_AggregatesCaloriesEarned 
	AS(
	SELECT 
	 CAST([date] AS DATE) AS [Date]
	,CAST(SUBSTRING(REPLACE(REPLACE([date],'"',''),'-',''),1,8)AS INT) AS [DateCode]
	,CAST(NULLIF([value],'') AS DECIMAL(18,4)) AS [Withings_DailyCaloriesActive]
	,ROW_NUMBER() OVER (
		PARTITION BY [date]
		,[value]
		 ORDER BY [Meta_CreateTime] DESC
		) row_num
FROM [stg].[Withings_AggregatesCaloriesEarned]
	)

	SELECT * INTO [stg].[Withings_AggregatesCaloriesEarned_Clean]
   FROM CTE_Withings_AggregatesCaloriesEarned
   WHERE row_num = 1
   ;	
/*Finished creating new clean table*/

/*Drop the working table if they exist and insert into new clean table*/
	BEGIN TRY DROP TABLE IF EXISTS [stg].[Oura_DailyReadiness_Clean] END TRY BEGIN CATCH END CATCH;

	WITH CTE_Oura_DailyReadiness 
	AS(
	SELECT 
	 CAST([day] AS DATE) AS [Date]
	,CAST(SUBSTRING(REPLACE(REPLACE([day],'"',''),'-',''),1,8)AS INT) AS [DateCode]
	,CAST(NULLIF([score],'') AS INT) AS [Oura_DailyReadiness]
	,ROW_NUMBER() OVER (
		PARTITION BY [day]
		,[score]
		 ORDER BY [Meta_CreateTime] DESC
		) row_num
FROM [stg].[Oura_DailyReadiness]
	)

	SELECT * INTO [stg].[Oura_DailyReadiness_Clean]
   FROM CTE_Oura_DailyReadiness
   WHERE row_num = 1
   ;	
/*Finished creating new clean table*/

/*Drop the working table if they exist and insert into new clean table*/
	BEGIN TRY DROP TABLE IF EXISTS [stg].[Oura_DailyActivity_Clean] END TRY BEGIN CATCH END CATCH;

	WITH CTE_Oura_DailyActivity 
	AS(
	SELECT 
	 CAST([day] AS DATE) AS [Date]
	,CAST(SUBSTRING(REPLACE(REPLACE([day],'"',''),'-',''),1,8)AS INT) AS [DateCode]
	,CAST(NULLIF([equivalent_walking_distance],'') AS DECIMAL(18,4)) AS [Oura_DailyDistance in meter]
	,CAST(NULLIF([steps],'') AS INT) AS [Oura_DailySteps]
	,CAST(NULLIF([active_calories],'') AS DECIMAL(18,4)) AS [Oura_DailyCaloriesActive]
	,CAST(NULLIF([total_calories],'') AS DECIMAL(18,4))  AS [Oura_DailyCaloriesTotal]
	,CAST([score] AS INT) AS [Oura_DailyActivityScore]
	,ROW_NUMBER() OVER (
		PARTITION BY [day]
		,[equivalent_walking_distance]
		,[steps]
        ,[active_calories]
        ,[total_calories]
        ,[score]
		 ORDER BY [Meta_CreateTime] DESC
		) row_num
FROM [stg].[Oura_DailyActivity]
	)

	SELECT * INTO [stg].[Oura_DailyActivity_Clean]
   FROM CTE_Oura_DailyActivity
   WHERE row_num = 1
   ;	
/*Finished creating new clean table*/

    /*Drop the working table if they exist and insert into new intermediate table*/
	BEGIN TRY DROP TABLE IF EXISTS [silver].[IntermediateDailyActivity] END TRY BEGIN CATCH END CATCH;
		
		
	SELECT    
	   COALESCE(AD.[Date],DA.[Date]) AS [Date]
	  ,ISNULL(DD.[SK_Date],0) AS [SK_Date]
	  --,[Withings_DailyDistance in meter]
	  --,[Oura_DailyDistance in meter]
	  ,CASE -- From SQL Server 2022 the function GREATEST can be used instead
        WHEN [Oura_DailyDistance in meter] >= ISNULL([Withings_DailyDistance in meter],0) THEN [Oura_DailyDistance in meter]
        ELSE [Withings_DailyDistance in meter]
       END AS [DailyDistance (m)]
	  --,[Oura_DailySteps]
	  --,[Withings_DailySteps]
	  ,CASE -- From SQL Server 2022 the function GREATEST can be used instead
        WHEN [Oura_DailySteps] >= ISNULL([Withings_DailySteps],0) THEN [Oura_DailySteps]
        ELSE [Withings_DailySteps]
       END AS [DailySteps]
	  ,COALESCE([Withings_DailyCaloriesPassive],(SELECT AVG([Withings_DailyCaloriesPassive]) FROM [stg].[Withings_AggregatesCaloriesPassive_Clean])) AS [DailyCaloriesPassive]
   --   ,[Oura_DailyCaloriesActive]
	  --,[Withings_DailyCaloriesActive]

	  ,CASE -- From SQL Server 2022 the function GREATEST can be used instead
        WHEN [Oura_DailyCaloriesActive] >= ISNULL([Withings_DailyCaloriesActive],0) THEN [Oura_DailyCaloriesActive]
        ELSE [Withings_DailyCaloriesActive]
       END AS [DailyCaloriesActive]

      --,[Oura_DailyCaloriesTotal] AS [DailyCaloriesTotal]
	  ,((CASE -- From SQL Server 2022 the function GREATEST can be used instead

        WHEN [Oura_DailyCaloriesActive] >= ISNULL([Withings_DailyCaloriesActive],0) THEN [Oura_DailyCaloriesActive]
        ELSE [Withings_DailyCaloriesActive]
       END ) + COALESCE([Withings_DailyCaloriesPassive],(SELECT AVG([Withings_DailyCaloriesPassive]) FROM [stg].[Withings_AggregatesCaloriesPassive_Clean]))) AS [DailyCaloriesTotal]
	   ,[Oura_DailyActivityScore] AS [DailyActivityScore]
	   ,[Oura_DailyReadiness]     AS [DailyReadinessScore]
	   ,@DW_DateTime_Load AS [DW_DateTime_Load_Insert]
	   ,@DW_Pipeline_run_ID AS [DW_Pipeline_run_ID_Insert]
	
   INTO [silver].[IntermediateDailyActivity]
   
   FROM [silver].[Dim_Date] DD 

   LEFT JOIN [stg].[Withings_AggregatesDistance_Clean] AD	 
   ON DD.[SK_Date] = AD.[DateCode]	

   LEFT JOIN [stg].[Oura_DailyActivity_Clean] DA
   ON DD.[SK_Date] = DA.[DateCode]

   LEFT JOIN [stg].[Withings_AggregatesSteps_Clean] AGS
   ON DD.[SK_Date] = AGS.[DateCode]
	
   LEFT JOIN [stg].[Withings_AggregatesCaloriesPassive_Clean] ACPC
   ON DD.[SK_Date] = ACPC.[DateCode]

   LEFT JOIN [stg].[Withings_AggregatesCaloriesEarned_Clean] ACEC
   ON DD.[SK_Date] = ACEC.[DateCode]

   LEFT JOIN [stg].[Oura_DailyReadiness_Clean] DRC
   ON DD.[SK_Date] = DRC.[DateCode]


  WHERE 1 = 1 

  AND COALESCE(AD.[Date],DA.[Date]) IS NOT NULL

	/*Count the number of read rows*/
			SET	@RecsRead = @@ROWCOUNT 
			;

   /*Rename the old and new silver tables to switch in the data*/
BEGIN TRANSACTION

SET @IsCompleted = 1

	
	IF @IsCompleted = 1 AND EXISTS (SELECT 1 FROM [silver].[IntermediateDailyActivity])
	BEGIN
		
		EXEC sp_rename 'silver.DailyActivity', 'DailyActivityOld';
		EXEC sp_rename 'silver.IntermediateDailyActivity', 'DailyActivity';
	END

	
  DROP INDEX IF EXISTS IX_silver_DailyActivity ON [silver].[DailyActivity]


  CREATE NONCLUSTERED INDEX IX_silver_DailyActivity
  ON [silver].[DailyActivity]([SK_Date] ASC)

 COMMIT TRANSACTION

			
		
	/*Log the data row count details in the Audit table*/
	UPDATE [meta].[Audit]
	SET	   [LoadStatus]		= 'Completed'
		  ,[EndDateTime]	= SYSDATETIME()
		  ,[RowsRead]		= @RecsRead
		  ,[RowsInserted]	= @RecsRead
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
	UPDATE STATISTICS [silver].[DailyActivity] ; 
	
	/*Rebuild indexes on the silver table*/
	ALTER INDEX ALL ON [silver].[DailyActivity]  REBUILD;

	/*Drop the working tables*/
	BEGIN TRY DROP TABLE IF EXISTS [stg].[Oura_DailyActivity_Clean] END TRY BEGIN CATCH END CATCH;
	BEGIN TRY DROP TABLE IF EXISTS [stg].[Withings_AggregatesCaloriesEarned_Clean] END TRY BEGIN CATCH END CATCH;
	BEGIN TRY DROP TABLE IF EXISTS [stg].[Withings_AggregatesCaloriesPassive_Clean] END TRY BEGIN CATCH END CATCH;
	BEGIN TRY DROP TABLE IF EXISTS [stg].[Withings_AggregatesDistance_Clean] END TRY BEGIN CATCH END CATCH;
	BEGIN TRY DROP TABLE IF EXISTS [stg].[Withings_AggregatesSteps_Clean] END TRY BEGIN CATCH END CATCH;
	BEGIN TRY DROP TABLE IF EXISTS [stg].[Oura_DailyReadiness_Clean] END TRY BEGIN CATCH END CATCH;
	BEGIN TRY DROP TABLE IF EXISTS [silver].[DailyActivityOld] END TRY BEGIN CATCH END CATCH;

END

END

/************ TEST HARNESS *****************************************************

--DECLARE @DW_Pipeline_run_ID [UNIQUEIDENTIFIER] = NEWID();
EXEC [silver].[delete_insert_DailyActivity] --@DW_Pipeline_run_ID;

*********************************************************************************
--AUDIT 

SELECT *
FROM   meta.[Audit] aud
WHERE  subentityname ='silver.delete_insert_DailyActivity'
ORDER  BY aud.startdatetime DESC 
**********************************************************************************/