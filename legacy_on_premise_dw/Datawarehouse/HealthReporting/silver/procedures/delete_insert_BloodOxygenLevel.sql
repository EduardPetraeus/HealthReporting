CREATE PROC [silver].[delete_insert_BloodOxygenLevel]  AS
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
	SET		@EntityName			= N'BloodOxygenLevel'
	SET		@SubEntityName		= N'silver.delete_insert_BloodOxygenLevel'
	SET		@TableName			= N'silver.BloodOxygenLevel'
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
	SELECT @DeltaType =ISNULL(DeltaType,'YEAR') ,@DeltaValue= ISNULL(DeltaValue,100), @WaterMarkDate= CAST(ISNULL(WaterMarkDate,getdate()) as Datetime),@CutOffYear= ISNULL(CutOffYear,100)  FROM [meta].[Config] WHERE [Object_Name] ='silver.BloodOxygenLevel' and IsActive=1

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
	BEGIN TRY DROP TABLE IF EXISTS [stg].[BloodOxygenLevel_Clean] END TRY BEGIN CATCH END CATCH;

	WITH CTE_BloodOxygenLevel 
	AS(
	  SELECT 
	 CAST(REPLACE([Day],'"','') AS DATE) AS [Date]
	,CAST(SUBSTRING(REPLACE(REPLACE([Day],'"',''),'-',''),1,8)AS INT) AS [DateCode]
	,CAST(NULLIF([spo2_percentage],'') AS DECIMAL(18,4)) AS [Oxygen saturation (SpO2)]
	,ROW_NUMBER() OVER (
		PARTITION BY [Day]
		,[spo2_percentage]
		 ORDER BY [Meta_CreateTime] DESC
		) row_num
FROM [stg].[Oura_BloodOxygenLevel]
	)

	SELECT * INTO [stg].[BloodOxygenLevel_Clean]
   FROM CTE_BloodOxygenLevel
   WHERE row_num = 1
   ;	

   /*Drop the working table if they exist and insert into new intermediate table*/
	BEGIN TRY DROP TABLE IF EXISTS [silver].[IntermediateBloodOxygenLevel] END TRY BEGIN CATCH END CATCH;
				
	SELECT 
	   [Date]
	  ,ISNULL(DD.[SK_Date],0) AS [SK_Date]
      ,[Oxygen saturation (SpO2)]
	  ,@DW_DateTime_Load AS [DW_DateTime_Load_Insert]
	  ,@DW_Pipeline_run_ID AS [DW_Pipeline_run_ID_Insert]
	
   INTO [silver].[IntermediateBloodOxygenLevel]
   
   FROM [stg].[BloodOxygenLevel_Clean] WC	
   INNER JOIN [silver].[Dim_Date] DD 
   ON WC.[DateCode] = DD.[SK_Date]		
	
	/*Count the number of read rows*/
			SET	@RecsRead = @@ROWCOUNT 
			;

   /*Rename the old and new silver tables to switch in the data*/
BEGIN TRANSACTION

SET @IsCompleted = 1

	
	IF @IsCompleted = 1 AND EXISTS (SELECT 1 FROM [silver].[IntermediateBloodOxygenLevel])
	BEGIN
		
		EXEC sp_rename 'silver.BloodOxygenLevel', 'BloodOxygenLevelOld';
		EXEC sp_rename 'silver.IntermediateBloodOxygenLevel', 'BloodOxygenLevel';
	END

	
  DROP INDEX IF EXISTS IX_silver_BloodOxygenLevel ON [silver].[BloodOxygenLevel]


  CREATE NONCLUSTERED INDEX IX_silver_BloodOxygenLevel
  ON [silver].[BloodOxygenLevel]([SK_Date] ASC)

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
	UPDATE STATISTICS [silver].[BloodOxygenLevel] ; 
	
	/*Rebuild indexes on the silver table*/
	ALTER INDEX ALL ON [silver].[BloodOxygenLevel]  REBUILD;

	/*Drop the working tables*/
	BEGIN TRY DROP TABLE IF EXISTS [stg].[BloodOxygenLevel_Clean] END TRY BEGIN CATCH END CATCH;
	BEGIN TRY DROP TABLE IF EXISTS [silver].[BloodOxygenLevelOld] END TRY BEGIN CATCH END CATCH;

END

END

/************ TEST HARNESS *****************************************************

--DECLARE @DW_Pipeline_run_ID [UNIQUEIDENTIFIER] = NEWID();
EXEC [silver].[delete_insert_BloodOxygenLevel] --@DW_Pipeline_run_ID;

*********************************************************************************
--AUDIT 

SELECT *
FROM   meta.[Audit] aud
WHERE  subentityname ='silver.delete_insert_BloodOxygenLevel'
ORDER  BY aud.startdatetime DESC 
**********************************************************************************/