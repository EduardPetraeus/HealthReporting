CREATE PROC [silver].[delete_insert_DailySleep]  AS
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
	SET		@EntityName			= N'DailySleep'
	SET		@SubEntityName		= N'silver.delete_insert_DailySleep'
	SET		@TableName			= N'silver.DailySleep'
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
	SELECT @DeltaType =ISNULL(DeltaType,'YEAR') ,@DeltaValue= ISNULL(DeltaValue,100), @WaterMarkDate= CAST(ISNULL(WaterMarkDate,getdate()) as Datetime),@CutOffYear= ISNULL(CutOffYear,100)  FROM [meta].[Config] WHERE [Object_Name] ='silver.DailySleep' and IsActive=1

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
	BEGIN TRY DROP TABLE IF EXISTS [stg].[DailySleep_Clean] END TRY BEGIN CATCH END CATCH;

	WITH CTE_DailySleep 
	AS(
 SELECT 
       CAST(NULLIF([average_breath],'')AS DECIMAL(18,4)) AS [AverageBreath (min)] -- decimal
	   --Average respiratory rate pr minute.
      ,CAST(NULLIF([average_heart_rate],'')AS DECIMAL(18,4)) AS [AverageHeartRate] -- decimal 
	  -- Average Heart Rate during sleep period
	  ,CAST(NULLIF([lowest_heart_rate],'')AS DECIMAL(18,4)) AS [LowestHeartRate] -- decimal
	  --The lowest heart rate (5 minutes sliding average) registered during the sleep period.
      ,CAST(NULLIF([average_hrv],'')AS DECIMAL(18,4)) AS [AverageHeartRateVariability] -- 
	  --Average Heart Rate Variability during sleep period
	  ,CAST(SUBSTRING(REPLACE([bedtime_start],'T',' '),1,19) AS DATETIME) AS [BedtimeStart]
	  ,CAST(SUBSTRING(REPLACE(REPLACE([bedtime_start],'T',''),'-',''),1,8)AS INT) AS [DateCodeStart]
	  ,SUBSTRING(REPLACE(REPLACE([bedtime_start],'T',''),':',''),11,4) as [TimeCodeStart]
	  ,CAST(SUBSTRING(REPLACE([bedtime_end],'T',' '),1,19) AS DATETIME) AS [BedtimeEnd]
	  ,CAST(SUBSTRING(REPLACE(REPLACE([bedtime_end],'T',''),'-',''),1,8)AS INT) AS [DateCodeEnd]
	  ,SUBSTRING(REPLACE(REPLACE([bedtime_end],'T',''),':',''),11,4) as [TimeCodeEnd]
      ,CAST([day] AS DATE) AS [Date]-- date
      ,CAST([deep_sleep_duration] AS DECIMAL(18,4)) AS [DeepSleepDuration (sec)] -- 
	  --Total amount of deep (N3) sleep registered during the sleep period.
	  ,CAST([light_sleep_duration] AS DECIMAL(18,4)) AS [LightSleepDuration (sec)] -- 
	  --Total amount of light (N1 or N2) sleep registered during the sleep period.
	  ,CAST([rem_sleep_duration] AS DECIMAL(18,4)) AS [RemSleepDuration (sec)] 
	  --Total amount of REM sleep registered during the sleep period.
      ,CAST([restless_periods] AS DECIMAL(18,4)) AS [RestlessPeriods (sec)]
	  --Restlessness of the sleep time, i.e. percentage of sleep time when the user was moving.
      ,CAST(NULLIF([readiness_temperature_deviation],'') AS DECIMAL(18,4)) AS [TemperatureDeviation]
	  ,CAST(NULLIF([readiness_temperature_trend_deviation],'') AS DECIMAL(18,4)) AS [TemperatureTrendDeviation]
	  ,CAST([efficiency] AS INT) AS [SleepEfficiency in %] -- int
	  --Sleep efficiency is the percentage of the sleep period spent asleep (100% * sleep.total / sleep.duration).
      ,CAST([latency] AS DECIMAL(18,4)) AS [SleepLatency (sec)] -- int
	  --Detected latency from bedtime_start to the beginning of the first five minutes of persistent sleep.
      ,CAST([period] AS BIT) AS [SleepPeriods] -- bit
	  --Index of the sleep period among sleep periods with the same summary_date, where 0 = first sleep period of the day.
      ,CAST([score] AS INT) AS [SleepScore] -- int
	  --Sleep score represents overall sleep quality during the sleep period. 
	  --It is calculated as a weighted average of sleep score contributors that represent one aspect of sleep quality each. 
	  --The sleep score contributor values are also available as separate parameters.
      ,CAST([segment_state] AS NVARCHAR(30)) AS [SleepSegmentState]-- string
      ,CAST([sleep_midpoint] AS DECIMAL(18,4)) AS [SleepMidpoint (sec)] -- int
	  --The time in seconds from the start of sleep to the midpoint of sleep. The midpoint ignores awake periods.
      ,CAST([time_in_bed] AS DECIMAL(18,4)) AS [TimeInBed (sec)] -- int
	  --Total duration of the sleep period (sleep.duration = sleep.bedtime_end - sleep.bedtime_start).
      ,CAST([total_sleep_duration] AS DECIMAL(18,4)) AS [TotalSleepDuration (sec)] -- int
	  --Total amount of sleep registered during the sleep period (sleep.total = sleep.rem + sleep.light + sleep.deep).
	  ,CAST([awake_time] AS DECIMAL(18,4)) AS [AwakeTime (sec)] -- int
	  --Total amount of awake time registered during the sleep period.
      --,CAST(REPLACE([type],'  ','time_in_bed') AS NVARCHAR(30)) AS [SleepType] -- string
	  ,CAST(CASE 
	     WHEN [type] = '' THEN 'Time in bed'
		 WHEN [type] = 'long_sleep' THEN 'Long sleep'
		 WHEN [type] = 'late_nap' THEN 'Late nap'
		 WHEN [type] = 'rest' THEN 'Rest'
		 WHEN [type] = 'sleep' THEN 'Sleep'
		 ELSE [type]
	   END AS NVARCHAR(30))AS [SleepType]
	  ,ROW_NUMBER() OVER (
		PARTITION BY 
		 [average_breath]
        ,[average_heart_rate]
        ,[lowest_heart_rate]
        ,[average_hrv]
        ,[bedtime_start]
        ,[bedtime_end]
        ,[day]
        ,[deep_sleep_duration]
        ,[light_sleep_duration]
        ,[rem_sleep_duration]
        ,[restless_periods]
        ,[efficiency]
        ,[latency]
        ,[period]
        ,[score]
        ,[segment_state]
        ,[sleep_midpoint]
        ,[time_in_bed]
        ,[total_sleep_duration]
        ,[awake_time]
        ,[type]
		,[readiness_temperature_deviation]
		,[readiness_temperature_trend_deviation]
		 ORDER BY [Meta_CreateTime] DESC
		) row_num

  FROM [HealthReporting].[stg].[Oura_Sleep]
  )
	SELECT * INTO [stg].[DailySleep_Clean]
   FROM CTE_DailySleep
   WHERE row_num = 1
   ;	

   /*Drop the working table if they exist and insert into new intermediate table*/
	BEGIN TRY DROP TABLE IF EXISTS [silver].[IntermediateDailySleep] END TRY BEGIN CATCH END CATCH;
		
		
	SELECT 
	   [BedtimeStart]
      ,[BedtimeEnd]
	  ,ISNULL(DD.[SK_Date],0) AS [SK_Date_BedtimeStart]
      ,ISNULL(DT.[SK_Time],0) AS [SK_Time_BedtimeStart]
	  ,ISNULL(DDEND.[SK_Date],0) AS [SK_Date_BedtimeEnd]
	  ,ISNULL(DTEND.[SK_Time],0) AS [SK_Time_BedtimeEnd]
	  ,[Date]
	  ,[AverageBreath (min)]
      ,[AverageHeartRate]
      ,[LowestHeartRate]
      ,[AverageHeartRateVariability]
      ,[DeepSleepDuration (sec)]
      ,[LightSleepDuration (sec)]
      ,[RemSleepDuration (sec)]
      ,[RestlessPeriods (sec)]
	  ,[TemperatureDeviation]
      ,[TemperatureTrendDeviation]
      ,[SleepEfficiency in %]
      ,[SleepLatency (sec)]
      ,[SleepPeriods]
      ,[SleepScore]
      ,[SleepSegmentState]
      ,[SleepMidpoint (sec)]
      ,[TimeInBed (sec)]
      ,[TotalSleepDuration (sec)]
      ,[AwakeTime (sec)]
      ,[SleepType]
	  ,@DW_DateTime_Load AS [DW_DateTime_Load_Insert]
	  ,@DW_Pipeline_run_ID AS [DW_Pipeline_run_ID_Insert]
	
   INTO [silver].[IntermediateDailySleep]
   
   FROM [stg].[DailySleep_Clean] WC	
   INNER JOIN [silver].[Dim_Date] DD 
   ON WC.[DateCodeStart] = DD.[SK_Date]	

   INNER JOIN [silver].[Dim_Time] DT
   ON WC.[TimeCodeStart] = DT.[Ekey_Time]	

   INNER JOIN [silver].[Dim_Date] DDEND 
   ON WC.[DateCodeEnd] = DDEND.[SK_Date]	

   INNER JOIN [silver].[Dim_Time] DTEND
   ON WC.[TimeCodeEnd] = DTEND.[Ekey_Time]	
	
	/*Count the number of read rows*/
			SET	@RecsRead = @@ROWCOUNT 
			;

   /*Rename the old and new silver tables to switch in the data*/
BEGIN TRANSACTION

SET @IsCompleted = 1

	
	IF @IsCompleted = 1 AND EXISTS (SELECT 1 FROM [silver].[IntermediateDailySleep])
	BEGIN
		
		EXEC sp_rename 'silver.DailySleep', 'DailySleepOld';
		EXEC sp_rename 'silver.IntermediateDailySleep', 'DailySleep';
	END

	
  DROP INDEX IF EXISTS IX_silver_DailySleep ON [silver].[DailySleep]


  CREATE NONCLUSTERED INDEX IX_silver_DailySleep
  ON [silver].[DailySleep]([SK_Date_BedtimeStart] ASC,[SK_Time_BedtimeStart] ASC,[SK_Date_BedtimeEnd] ASC,[SK_Time_BedtimeEnd] ASC)
  

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
	UPDATE STATISTICS [silver].[DailySleep] ; 
	
	/*Rebuild indexes on the silver table*/
	ALTER INDEX ALL ON [silver].[DailySleep]  REBUILD;

	/*Drop the working tables*/
	BEGIN TRY DROP TABLE IF EXISTS [stg].[DailySleep_Clean] END TRY BEGIN CATCH END CATCH;
	BEGIN TRY DROP TABLE IF EXISTS [silver].[DailySleepOld] END TRY BEGIN CATCH END CATCH;

END

END

/************ TEST HARNESS *****************************************************

--DECLARE @DW_Pipeline_run_ID [UNIQUEIDENTIFIER] = NEWID();
EXEC [silver].[delete_insert_DailySleep] --@DW_Pipeline_run_ID;

*********************************************************************************
--AUDIT 

SELECT *
FROM   meta.[Audit] aud
WHERE  subentityname ='silver.delete_insert_DailySleep'
ORDER  BY aud.startdatetime DESC 
**********************************************************************************/