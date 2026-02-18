CREATE PROC [silver].[delete_insert_Workout]  AS
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
	SET		@EntityName			= N'Workout'
	SET		@SubEntityName		= N'silver.delete_insert_Workout'
	SET		@TableName			= N'silver.Workout'
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
	SELECT @DeltaType =ISNULL(DeltaType,'YEAR') ,@DeltaValue= ISNULL(DeltaValue,100), @WaterMarkDate= CAST(ISNULL(WaterMarkDate,getdate()) as Datetime),@CutOffYear= ISNULL(CutOffYear,100)  FROM [meta].[Config] WHERE [Object_Name] ='silver.Workout' and IsActive=1

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
	BEGIN TRY DROP TABLE IF EXISTS [stg].[Workout_Clean] END TRY BEGIN CATCH END CATCH;

	WITH CTE_Workout
	AS(
SELECT 
      [Activity DATE1] AS [Activity month-day-year]
	  ,NULLIF(CAST(REPLACE([Activity DATE1],',','') AS DATETIME),'') AS [Activity DateTime]
      ,NULLIF([Activity Name1],'') AS [Activity Name]
      ,NULLIF([Activity Type1],'')  AS [Activity Type]
	  ,CAST(
		HASHBYTES('SHA2_256', CONCAT (
				ISNULL(CAST(UPPER(RTRIM(LTRIM([Activity Name1]))) AS NVARCHAR), N'#')
				,'|'				
				,ISNULL(CAST(UPPER(RTRIM(LTRIM([Activity Type1]))) AS NVARCHAR), N'#')
				)) 
		AS BIGINT) 
		AS [UniqueActivityTypeKey]
	  ,CAST(NULLIF([Max Speed],'') AS DECIMAL(18,8)) * 3.6 AS [Max Speed (km/h)]
      --,CAST(NULLIF([Average Speed],'')  AS DECIMAL(18,8)) * 3.6 AS [Average Speed (km/h)]
	  ,CAST(NULLIF([Average Elapsed Speed],'') AS DECIMAL(18,8)) * 3.6  AS [Average Elapsed Speed (km/h)]
	  ,COALESCE((NULLIF(CAST(NULLIF([Distance1],'')AS DECIMAL(18,8)),0)/1000) / ((CAST(NULLIF([Moving TIME],'') AS DECIMAL(18,8))/60)/60),CAST(NULLIF([Average Speed],'')  AS DECIMAL(18,8)) * 3.6) AS [Average Speed (km/h)]
	  ,ISNULL((NULLIF(CAST(NULLIF([Distance1],'')AS DECIMAL(18,8)),0)/1000) / (CAST(NULLIF([Moving TIME],'') AS DECIMAL(18,8))/60),0) AS [Average Speed (km/min)]	   
	  ,ISNULL((CAST(NULLIF([Moving TIME],'') AS DECIMAL(18,8))/60)/ (NULLIF(CAST(NULLIF([Distance1],'')AS DECIMAL(18,8)),0)/1000),0) AS [Average Speed (min/km)]
     --,(NULLIF(CAST(NULLIF([Distance1],'')AS DECIMAL(18,8)),0)/1000) / ((CAST(NULLIF([Moving TIME],'') AS DECIMAL(18,8))/60)/60) AS [Average Speed (km/h)	 
	  ,NULLIF([Total Steps],'')  AS [Total Steps]
	  ,NULLIF([Elapsed TIME1],'') AS [Elapsed TIME (sec)]
	  ,NULLIF([Moving TIME],'') AS [Moving TIME (sec)]
      ,NULLIF([Distance],'') AS [Distance (km)]
      ,NULLIF([Distance1],'') AS [Distance (m)]
      ,NULLIF([Max Heart Rate1],'') AS [Max Heart Rate]
      ,NULLIF([Average Heart Rate],'')  AS [Average Heart Rate]
	  ,NULLIF([Calories],'')  AS [Calories]
      ,NULLIF([Relative Effort1],'') AS [Relative Effort]
      ,NULLIF([Commute1],'') AS [Commute]
      ,NULLIF([Filename],'') AS [Filename]
      ,NULLIF([Athlete Weight],'') AS [Athlete Weight]
	  ,NULLIF([Activity Description],'') AS [Activity Description]
      ,NULLIF([Elapsed TIME],'') AS [Elapsed TIME extra column (sec)]
	  ,NULLIF([Max Heart Rate]				 ,'')  AS [Max Heart Rate extra column]
      ,NULLIF([Elevation Gain]				 ,'')  AS [Elevation Gain]
      ,NULLIF([Elevation Loss]				 ,'')  AS [Elevation Loss]
      ,NULLIF([Elevation Low]				 ,'')  AS [Elevation Low]
      ,NULLIF([Elevation High]				 ,'')  AS [Elevation High]
      ,NULLIF([Max Grade]					 ,'')  AS [Max Grade]
      ,NULLIF([Average Grade]				 ,'')  AS [Average Grade]
      ,NULLIF([Average Positive Grade]		 ,'')  AS [Average Positive Grade]
      ,NULLIF([Average Negative Grade]		 ,'')  AS [Average Negative Grade]
      ,NULLIF([Max Cadence]				     ,'')  AS [Max Cadence]
      ,NULLIF([Average Cadence]			     ,'')  AS [Average Cadence]
      ,NULLIF([Max Watts]					 ,'')  AS [Max Watts]
      ,NULLIF([Average Watts]				 ,'')  AS [Average Watts]
      ,NULLIF([Max Temperature]			     ,'')  AS [Max Temperature]
      ,NULLIF([Average Temperature]		     ,'')  AS [Average Temperature]
      ,NULLIF([Relative Effort]			     ,'')  AS [Relative Effort extra column]
      ,NULLIF([Total WORK]					 ,'')  AS [Total WORK]
      ,NULLIF([Number OF Runs]				 ,'')  AS [Number OF Runs]
      ,NULLIF([Uphill TIME]				     ,'')  AS [Uphill TIME]
      ,NULLIF([Downhill TIME]				 ,'')  AS [Downhill TIME]
      ,NULLIF([Other TIME]					 ,'')  AS [Other TIME]
      ,NULLIF([Perceived Exertion]			 ,'')  AS [Perceived Exertion]
      ,NULLIF([Type]						 ,'')  AS [Type]
      ,NULLIF([Start TIME]					 ,'')  AS [Start TIME]
      ,NULLIF([Weighted Average Power]		 ,'')  AS [Weighted Average Power]
      ,NULLIF([Power Count]				     ,'')  AS [Power Count]
      ,NULLIF([Prefer Perceived Exertion]	 ,'')  AS [Prefer Perceived Exertion]
      ,NULLIF([Perceived Relative Effort]	 ,'')  AS [Perceived Relative Effort]
      ,NULLIF([Commute]					     ,'')  AS [Commute extra column]
      ,NULLIF([Total Weight Lifted]		     ,'')  AS [Total Weight Lifted]
      ,NULLIF([FROM Upload]				     ,'')  AS [FROM Upload]
      ,NULLIF([Grade Adjusted Distance]	     ,'')  AS [Grade Adjusted Distance]
      ,NULLIF([Weather Observation TIME]	 ,'')  AS [Weather Observation TIME]
      ,NULLIF([Weather Condition]			 ,'')  AS [Weather Condition]
      ,NULLIF([Weather Temperature]		     ,'')  AS [Weather Temperature]
      ,NULLIF([Apparent Temperature]		 ,'')  AS [Apparent Temperature]
      ,NULLIF([Dewpoint]					 ,'')  AS [Dewpoint]
      ,NULLIF([Humidity]					 ,'')  AS [Humidity]
      ,NULLIF([Weather Pressure]			 ,'')  AS [Weather Pressure]
      ,NULLIF([Wind Speed]					 ,'')  AS [Wind Speed]
      ,NULLIF([Wind Gust]					 ,'')  AS [Wind Gust]
      ,NULLIF([Wind Bearing]				 ,'')  AS [Wind Bearing]
      ,NULLIF([Precipitation Intensity]		 ,'')  AS [Precipitation Intensity]
      ,NULLIF([Sunrise TIME]				 ,'')  AS [Sunrise TIME]
      ,NULLIF([Sunset TIME]					 ,'')  AS [Sunset TIME]
      ,NULLIF([Moon Phase]					 ,'')  AS [Moon Phase]
      ,NULLIF([Bike]						 ,'')  AS [Bike]
      ,NULLIF([Gear]						 ,'')  AS [Gear]
      ,NULLIF([Precipitation Probability]	 ,'')  AS [Precipitation Probability]
      ,NULLIF([Precipitation Type]			 ,'')  AS [Precipitation Type]
      ,NULLIF([Cloud Cover]				     ,'')  AS [Cloud Cover]
      ,NULLIF([Weather Visibility]			 ,'')  AS [Weather Visibility]
      ,NULLIF([UV INDEX]					 ,'')  AS [UV INDEX]
      ,NULLIF([Weather Ozone]				 ,'')  AS [Weather Ozone]
      ,NULLIF([Jump Count]					 ,'')  AS [Jump Count]
      ,NULLIF([Total Grit]					 ,'')  AS [Total Grit]
      ,NULLIF([Average Flow]				 ,'')  AS [Average Flow]
      ,NULLIF([Flagged]					     ,'')  AS [Flagged]
      ,NULLIF([Dirt Distance]				 ,'')  AS [Dirt Distance]
      ,NULLIF([Newly Explored Distance]	     ,'')  AS [Newly Explored Distance]
      ,NULLIF([Newly Explored Dirt Distance] ,'')  AS [Newly Explored Dirt Distance]
      ,NULLIF([Activity Count]				 ,'')  AS [Activity Count]
      ,NULLIF([Media]						 ,'')  AS [Media]
      ,ROW_NUMBER() OVER (
		PARTITION BY 
		[Activity ID1]
      ,[Activity DATE1]
      ,[Activity Name1]
      ,[Activity Type1]
      ,[Activity Description]
      ,[Elapsed TIME1]
      ,[Distance1]
      ,[Max Heart Rate1]
      ,[Relative Effort1]
      ,[Commute1]
      ,[Activity Private Note]
      ,[Activity Gear]
      ,[Filename]
      ,[Athlete Weight]
      ,[Bike Weight]
      ,[Elapsed TIME]
      ,[Moving TIME]
      ,[Distance]
      ,[Max Speed]
      ,[Average Speed]
      ,[Elevation Gain]
      ,[Elevation Loss]
      ,[Elevation Low]
      ,[Elevation High]
      ,[Max Grade]
      ,[Average Grade]
      ,[Average Positive Grade]
      ,[Average Negative Grade]
      ,[Max Cadence]
      ,[Average Cadence]
      ,[Max Heart Rate]
      ,[Average Heart Rate]
      ,[Max Watts]
      ,[Average Watts]
      ,[Calories]
      ,[Max Temperature]
      ,[Average Temperature]
      ,[Relative Effort]
      ,[Total WORK]
      ,[Number OF Runs]
      ,[Uphill TIME]
      ,[Downhill TIME]
      ,[Other TIME]
      ,[Perceived Exertion]
      ,[Type]
      ,[Start TIME]
      ,[Weighted Average Power]
      ,[Power Count]
      ,[Prefer Perceived Exertion]
      ,[Perceived Relative Effort]
      ,[Commute]
      ,[Total Weight Lifted]
      ,[FROM Upload]
      ,[Grade Adjusted Distance]
      ,[Weather Observation TIME]
      ,[Weather Condition]
      ,[Weather Temperature]
      ,[Apparent Temperature]
      ,[Dewpoint]
      ,[Humidity]
      ,[Weather Pressure]
      ,[Wind Speed]
      ,[Wind Gust]
      ,[Wind Bearing]
      ,[Precipitation Intensity]
      ,[Sunrise TIME]
      ,[Sunset TIME]
      ,[Moon Phase]
      ,[Bike]
      ,[Gear]
      ,[Precipitation Probability]
      ,[Precipitation Type]
      ,[Cloud Cover]
      ,[Weather Visibility]
      ,[UV INDEX]
      ,[Weather Ozone]
      ,[Jump Count]
      ,[Total Grit]
      ,[Average Flow]
      ,[Flagged]
      ,[Average Elapsed Speed]
      ,[Dirt Distance]
      ,[Newly Explored Distance]
      ,[Newly Explored Dirt Distance]
      ,[Activity Count]
      ,[Total Steps]
		 ORDER BY [Meta_CreateTime] DESC
		) row_num
  FROM [stg].[Strava_Training]
  	)
	SELECT 
	CAST(REPLACE(CAST([Activity DateTime] AS DATE),'-','')AS INT) AS [DateCode]
	,SUBSTRING(REPLACE(CAST(REPLACE([Activity DateTime],',','')AS time),':',''),1,4)  AS [TimeCode]
	,* 
	INTO [stg].[Workout_Clean]
   FROM CTE_Workout
   WHERE row_num = 1
   ;		

   /***** Insert technical unknown record into the main Silver table if it doesn't exist *****/
	IF NOT EXISTS(Select 0 from [silver].[ActivityType] WHERE [SK_ActivityType] = 0)
	BEGIN
		SET IDENTITY_INSERT [silver].[ActivityType] ON

		INSERT INTO [silver].[ActivityType] 
		(  [SK_ActivityType]
		  ,[Activity Name]
		  ,[Activity Type]
		  ,[UniqueActivityTypeKey]
		  ,[DW_Datetime_Load]
		  ,[DW_Pipeline_run_ID]
		  )
			VALUES	 (0,'Unknown','Unknown',0,@DW_DateTime_Load,@DW_Pipeline_run_ID)

		SET IDENTITY_INSERT [silver].[ActivityType] OFF
	END

   /*Insert the new entries to the silver table */
	INSERT INTO [silver].[ActivityType]
	(  [Activity Name]
      ,[Activity Type]
      ,[UniqueActivityTypeKey]
      ,[DW_Datetime_Load]
      ,[DW_Pipeline_run_ID]
	  )

	SELECT  DISTINCT 
	   stg.[Activity Name]
      ,stg.[Activity Type]
      ,stg.[UniqueActivityTypeKey]
	  ,@DW_DateTime_Load as DW_DateTime_Load 
	  ,@DW_Pipeline_run_ID as DW_Pipeline_run_ID
	FROM [stg].[Workout_Clean] stg	
	LEFT JOIN [silver].[ActivityType] dest
	ON stg.[UniqueActivityTypeKey] = dest.[UniqueActivityTypeKey]
	WHERE dest.[UniqueActivityTypeKey] IS NULL 

   /*Drop the working table if they exist and insert into new intermediate table*/
	BEGIN TRY DROP TABLE IF EXISTS [silver].[IntermediateWorkout] END TRY BEGIN CATCH END CATCH;
		
		
	/*Drop the working table if they exist and insert into new intermediate table*/
	BEGIN TRY DROP TABLE IF EXISTS [silver].[IntermediateWorkout] END TRY BEGIN CATCH END CATCH;
		
		
	SELECT 
	   WC.[Activity month-day-year]
      ,WC.[Activity DateTime]
	  ,ISNULL(DD.[SK_Date],0) AS [SK_Date]
	  ,ISNULL(DT.[SK_Time] ,0) AS [SK_Time]
      ,WC.[Activity Name]
      ,WC.[Activity Type]
      ,WC.[UniqueActivityTypeKey]
	  ,ISNULL(AT.[SK_ActivityType] ,0) AS [SK_ActivityType]
      ,WC.[Max Speed (km/h)]
      ,WC.[Average Elapsed Speed (km/h)]
      ,WC.[Average Speed (km/h)]
      ,WC.[Average Speed (km/min)]
      ,WC.[Average Speed (min/km)]
      ,WC.[Total Steps]
      ,WC.[Elapsed TIME (sec)]
      ,WC.[Moving TIME (sec)]
      ,WC.[Distance (km)]
      ,WC.[Distance (m)]
      ,WC.[Max Heart Rate]
      ,WC.[Average Heart Rate]
      ,WC.[Calories]
      ,WC.[Relative Effort]
      ,WC.[Commute]
      ,WC.[Filename]
      ,WC.[Athlete Weight]
      ,WC.[Activity Description]
      ,WC.[Elapsed TIME extra column (sec)]
      ,WC.[Max Heart Rate extra column]
      ,WC.[Elevation Gain]
      ,WC.[Elevation Loss]
      ,WC.[Elevation Low]
      ,WC.[Elevation High]
      ,WC.[Max Grade]
      ,WC.[Average Grade]
      ,WC.[Average Positive Grade]
      ,WC.[Average Negative Grade]
      ,WC.[Max Cadence]
      ,WC.[Average Cadence]
      ,WC.[Max Watts]
      ,WC.[Average Watts]
      ,WC.[Max Temperature]
      ,WC.[Average Temperature]
      ,WC.[Relative Effort extra column]
      ,WC.[Total WORK]
      ,WC.[Number OF Runs]
      ,WC.[Uphill TIME]
      ,WC.[Downhill TIME]
      ,WC.[Other TIME]
      ,WC.[Perceived Exertion]
      ,WC.[Type]
      ,WC.[Start TIME]
      ,WC.[Weighted Average Power]
      ,WC.[Power Count]
      ,WC.[Prefer Perceived Exertion]
      ,WC.[Perceived Relative Effort]
      ,WC.[Commute extra column]
      ,WC.[Total Weight Lifted]
      ,WC.[FROM Upload]
      ,WC.[Grade Adjusted Distance]
      ,WC.[Weather Observation TIME]
      ,WC.[Weather Condition]
      ,WC.[Weather Temperature]
      ,WC.[Apparent Temperature]
      ,WC.[Dewpoint]
      ,WC.[Humidity]
      ,WC.[Weather Pressure]
      ,WC.[Wind Speed]
      ,WC.[Wind Gust]
      ,WC.[Wind Bearing]
      ,WC.[Precipitation Intensity]
      ,WC.[Sunrise TIME]
      ,WC.[Sunset TIME]
      ,WC.[Moon Phase]
      ,WC.[Bike]
      ,WC.[Gear]
      ,WC.[Precipitation Probability]
      ,WC.[Precipitation Type]
      ,WC.[Cloud Cover]
      ,WC.[Weather Visibility]
      ,WC.[UV INDEX]
      ,WC.[Weather Ozone]
      ,WC.[Jump Count]
      ,WC.[Total Grit]
      ,WC.[Average Flow]
      ,WC.[Flagged]
      ,WC.[Dirt Distance]
      ,WC.[Newly Explored Distance]
      ,WC.[Newly Explored Dirt Distance]
      ,WC.[Activity Count]
      ,WC.[Media]
	  ,@DW_DateTime_Load AS [DW_DateTime_Load_Insert]
	  ,@DW_Pipeline_run_ID AS [DW_Pipeline_run_ID_Insert]
	
   INTO [silver].[IntermediateWorkout]
   
   FROM [stg].[Workout_Clean] WC	
   INNER JOIN [silver].[Dim_Date] DD 
   ON WC.[DateCode] = DD.[SK_Date]	

   INNER JOIN [silver].[Dim_Time] DT
   ON WC.[TimeCode] = DT.[Ekey_Time]	

   INNER JOIN [silver].[ActivityType] AT
   ON WC.[UniqueActivityTypeKey] = AT.[UniqueActivityTypeKey]
	
	/*Count the number of read rows*/
			SET	@RecsRead = @@ROWCOUNT 
			;

   /*Rename the old and new silver tables to switch in the data*/
BEGIN TRANSACTION

SET @IsCompleted = 1

	
	IF @IsCompleted = 1 AND EXISTS (SELECT 1 FROM [silver].[IntermediateWorkout])
	BEGIN
		
		EXEC sp_rename 'silver.Workout', 'WorkoutOld';
		EXEC sp_rename 'silver.IntermediateWorkout', 'Workout';
	END

	
  DROP INDEX IF EXISTS IX_silver_Workout ON [silver].[Workout]


  CREATE NONCLUSTERED INDEX IX_silver_Workout
  ON [silver].[Workout]([SK_Date] ASC,[SK_Time] ASC,[SK_ActivityType] ASC)

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
	UPDATE STATISTICS [silver].[Workout] ; 
	
	/*Rebuild indexes on the silver table*/
	ALTER INDEX ALL ON [silver].[Workout]  REBUILD;

	/*Drop the working tables*/
	BEGIN TRY DROP TABLE IF EXISTS [stg].[Workout_Clean] END TRY BEGIN CATCH END CATCH;
	BEGIN TRY DROP TABLE IF EXISTS [silver].[WorkoutOld] END TRY BEGIN CATCH END CATCH;

END

END

/************ TEST HARNESS *****************************************************

--DECLARE @DW_Pipeline_run_ID [UNIQUEIDENTIFIER] = NEWID();
EXEC [silver].[delete_insert_Workout] --@DW_Pipeline_run_ID;

*********************************************************************************
--AUDIT 

SELECT *
FROM   meta.[Audit] aud
WHERE  subentityname ='silver.delete_insert_Workout'
ORDER  BY aud.startdatetime DESC 
**********************************************************************************/