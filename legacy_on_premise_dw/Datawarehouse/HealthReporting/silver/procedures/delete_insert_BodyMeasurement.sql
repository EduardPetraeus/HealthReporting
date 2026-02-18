CREATE PROC [silver].[delete_insert_BodyMeasurement]  AS
/*******************************************************************************************

*********************************************************************************************/
BEGIN

	/*Declare the variables*/	
	DECLARE @DW_DateTime_Load		    datetime2(7),
			@DW_StartDatetime		    datetime2(7),
			@EntityName				    varchar(100),
			@SubEntityName			    varchar(100),
			@TableName				    varchar(100),
			@AuditID				    bigint,
			@RecsRead				    bigint = 0,
			@RecsUpdated			    bigint = 0,
			@RecsDeleted			    bigint = 0,
			@RecsInserted			    bigint = 0,
			@CutOffDate				    date,
			@CutOffDateKey			    int,
			@CurrentUser			    nvarchar(100),
			@Today					    Int,
			@CutOffDateInt			    int,
			@DeltaType				    nvarchar(10),
			@DeltaValue				    int,
			@WaterMarkDate			    datetime2(7),
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
	SET		@EntityName			= N'BodyMeasurement'
	SET		@SubEntityName		= N'silver.delete_insert_BodyMeasurement'
	SET		@TableName			= N'silver.BodyMeasurement'
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
	SELECT @DeltaType =ISNULL(DeltaType,'YEAR') ,@DeltaValue= ISNULL(DeltaValue,100), @WaterMarkDate= CAST(ISNULL(WaterMarkDate,getdate()) as Datetime),@CutOffYear= ISNULL(CutOffYear,100)  FROM [meta].[Config] WHERE [Object_Name] ='silver.BodyMeasurement' and IsActive=1

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
	BEGIN TRY DROP TABLE IF EXISTS [stg].[BodyMeasurement_Clean] END TRY BEGIN CATCH END CATCH;

	WITH CTE_BodyMeasurement 
	AS(
	SELECT 
	   CAST([Date]								      AS DATETIME)  AS [DateTime]
	  ,CAST(SUBSTRING(REPLACE(REPLACE([Date],'"',''),'-',''),1,8)AS INT) AS [DateCode]
	  ,SUBSTRING(REPLACE(REPLACE([Date],'"',''),':',''),12,4)            AS [TimeCode]
      ,CAST(NULLIF([Atrial fibrillation result],0)    AS SMALLINT)  AS [Atrial fibrillation result] 
      ,CAST([Basal Metabolic Rate (BMR)]	          AS SMALLINT)  AS [Basal Metabolic Rate (BMR)]
      ,CAST([BMI]  						              AS SMALLINT)  AS [BMI]
	  ,CAST([VO2MAX] 								  AS SMALLINT)  AS [VO2 Max]
	  --VO2 max is a numerical measurement of your body’s ability to consume oxygen (ml/min/kg). 
	  ,CAST([Nerve Health Score Feet]	              AS SMALLINT)  AS [Nerve Health Score Feet]
      ,CAST([Nerve Health Score left foot]	          AS SMALLINT)  AS [Nerve Health Score left foot]
      ,CAST([Nerve Health Score right foot]	          AS SMALLINT)  AS [Nerve Health Score right foot]
	  ,CAST([WATER_PERCENT] 				          AS SMALLINT)  AS [Water %]
      ,CAST([BONE_PERCENT]					          AS SMALLINT)  AS [Bone %]
      ,CAST([MUSCLE_PERCENT]				          AS SMALLINT)  AS [Muscle %]
      ,CAST([Fat Free Percent]  			          AS SMALLINT)  AS [Fat Free body mass %]
	  ,CAST([FAT_PERCENT]					          AS SMALLINT)  AS [Fat %]
	  ,CAST([FAT_FREE_MASS]					          AS SMALLINT)  AS [Fat Free body mass (kg)]
      ,CAST([Visceral Fat] 					          AS SMALLINT)  AS [Visceral Fat (kg)]
	  ,CAST([Vascular age] 					          AS SMALLINT)  AS [Vascular age]
	  ,CAST([Metabolic Age] 				          AS SMALLINT)  AS [Metabolic Age]
      ,CAST([Extracellular Water] 			          AS SMALLINT)  AS [Extracellular Water]
	  ,CAST([Intracellular Water]  			          AS SMALLINT)  AS [Intracellular Water]
      ,CAST([Fat Free Mass for segments] 	          AS SMALLINT)  AS [Fat Free Mass for segments]
      ,CAST([Fat Mass for segments in mass unit]      AS SMALLINT)  AS [Fat Mass for segments in mass unit]
	  ,CAST([Muscle Mass for segments] 			      AS SMALLINT)  AS [Muscle Mass for segments]
	  ,CAST(NULLIF([Valvular heart disease result],0) AS SMALLINT) AS [Valvular heart disease result]
      ,ROW_NUMBER() OVER (
		PARTITION BY 
	   [Date] 
      ,[Atrial fibrillation result]
      ,[Basal Metabolic Rate (BMR)]
      ,[BMI]  						    
	  ,[VO2MAX] 						
	  ,[Nerve Health Score Feet]	    
      ,[Nerve Health Score left foot]	
      ,[Nerve Health Score right foot]	
	  ,[WATER_PERCENT] 				    
      ,[BONE_PERCENT]					
      ,[MUSCLE_PERCENT]				    
      ,[Fat Free Percent]  			    
	  ,[FAT_PERCENT]					
	  ,[FAT_FREE_MASS]					
      ,[Visceral Fat] 					
	  ,[Vascular age] 					
	  ,[Metabolic Age] 				    
      ,[Extracellular Water] 			
	  ,[Intracellular Water]  			
      ,[Fat Free Mass for segments] 	
      ,[Fat Mass for segments in mass unit]  
	  ,[Muscle Mass for segments] 			  
	  ,[Valvular heart disease result]
	  ORDER BY [Meta_CreateTime] DESC
		) row_num

  FROM [stg].[vw_Withings_Measurements]
	)

	SELECT * INTO [stg].[BodyMeasurement_Clean]
   FROM CTE_BodyMeasurement
   WHERE row_num = 1
   ;	

   /*Drop the working table if they exist and insert into new intermediate table*/
	BEGIN TRY DROP TABLE IF EXISTS [silver].[IntermediateBodyMeasurement] END TRY BEGIN CATCH END CATCH;
		
		
	SELECT 
	   [DateTime]
	  ,ISNULL(DD.[SK_Date],0) AS [SK_Date]
      ,ISNULL(DT.[SK_Time],0) AS [SK_Time]
      ,[Atrial fibrillation result]
      ,[Basal Metabolic Rate (BMR)]
      ,[BMI]
      ,[VO2 Max]
      ,[Nerve Health Score Feet]
      ,[Nerve Health Score left foot]
      ,[Nerve Health Score right foot]
      ,[Water %]
      ,[Bone %]
      ,[Muscle %]
      ,[Fat Free body mass %]
      ,[Fat %]
      ,[Fat Free body mass (kg)]
      ,[Visceral Fat (kg)]
      ,[Vascular age]
      ,[Metabolic Age]
      ,[Extracellular Water]
      ,[Intracellular Water]
      ,[Fat Free Mass for segments]
      ,[Fat Mass for segments in mass unit]
      ,[Muscle Mass for segments]
      ,[Valvular heart disease result]
	  ,@DW_DateTime_Load AS [DW_DateTime_Load_Insert]
	  ,@DW_Pipeline_run_ID AS [DW_Pipeline_run_ID_Insert]
	
   INTO [silver].[IntermediateBodyMeasurement]
   
   FROM [stg].[BodyMeasurement_Clean] WC	
   INNER JOIN [silver].[Dim_Date] DD 
   ON WC.[DateCode] = DD.[SK_Date]	

   INNER JOIN [silver].[Dim_Time] DT
   ON WC.[TimeCode] = DT.[Ekey_Time]	
	
	/*Count the number of read rows*/
			SET	@RecsRead = @@ROWCOUNT 
			;

   /*Rename the old and new silver tables to switch in the data*/
BEGIN TRANSACTION

SET @IsCompleted = 1

	
	IF @IsCompleted = 1 AND EXISTS (SELECT 1 FROM [silver].[IntermediateBodyMeasurement])
	BEGIN
		
		EXEC sp_rename 'silver.BodyMeasurement', 'BodyMeasurementOld';
		EXEC sp_rename 'silver.IntermediateBodyMeasurement', 'BodyMeasurement';
	END

	
  DROP INDEX IF EXISTS IX_silver_BodyMeasurement ON [silver].[BodyMeasurement]


  CREATE NONCLUSTERED INDEX IX_silver_BodyMeasurement
  ON [silver].[BodyMeasurement]([SK_Date] ASC,[SK_Time] ASC)

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
	UPDATE STATISTICS [silver].[BodyMeasurement] ; 
	
	/*Rebuild indexes on the silver table*/
	ALTER INDEX ALL ON [silver].[BodyMeasurement]  REBUILD;

	/*Drop the working tables*/
	BEGIN TRY DROP TABLE IF EXISTS [stg].[BodyMeasurement_Clean] END TRY BEGIN CATCH END CATCH;
	BEGIN TRY DROP TABLE IF EXISTS [silver].[BodyMeasurementOld] END TRY BEGIN CATCH END CATCH;

END

END

/************ TEST HARNESS *****************************************************

--DECLARE @DW_Pipeline_run_ID [UNIQUEIDENTIFIER] = NEWID();
EXEC [silver].[delete_insert_BodyMeasurement] --@DW_Pipeline_run_ID;

*********************************************************************************
--AUDIT 

SELECT *
FROM   meta.[Audit] aud
WHERE  subentityname ='silver.delete_insert_BodyMeasurement'
ORDER  BY aud.startdatetime DESC 
**********************************************************************************/