CREATE PROCEDURE [silver].[data_Dim_Time]
	AS
SET NOCOUNT ON;  

TRUNCATE TABLE [silver].[Dim_Time]

DECLARE @hour int,@minute int

SET @hour=0

WHILE @hour<24

BEGIN

	SET @minute=0

		while @minute<60

		BEGIN

				INSERT INTO [silver].[Dim_Time]
				(
				 [Ekey_Time]
				,[TimeCode]
				,[Hour12Code]	
				,[Hour12Key]		
				,[MinuteCode]	
				,[MinuteKey]		
				,[AMPMCode]		
				,[Hour24Code]	
				,[Hour24Key]		
				,[Minute15Code]  
				,[Minute15Key]	
				)

				select 
						right('0'+convert(varchar(2),@hour),2)+right('0'+convert(varchar(2),@minute),2)  AS DW_EK_TIME,
						right('0'+convert(varchar(2),@hour),2)+':'+right('0'+convert(varchar(2),@minute),2)  AS TimeCode,
						right('0'+convert(varchar(2),@hour%12),2) AS [Hour12Code],
						@hour%12 AS [Hour12Key],
						right('0'+convert(varchar(2),@minute),2)  AS [MinuteCode],
						@minute  AS [MinuteKey],
						case when @hour<12 then 'AM' else 'PM' end AS AMPMCode,
						right('0'+convert(varchar(2),@hour),2) AS [Hour24Code],
						@hour AS [Hour24Key],
						CASE 
							 WHEN @minute<15 THEN right('0'+convert(varchar(2),@hour),2)+':'+'00-'+right('0'+convert(varchar(2),@hour),2)+':15' 
							 WHEN @minute>=15 and @minute<30 THEN right('0'+convert(varchar(2),@hour),2)+':'+'15-'+right('0'+convert(varchar(2),@hour),2)+':30'
							 WHEN @minute>=30 and @minute<45 THEN right('0'+convert(varchar(2),@hour),2)+':'+'30-'+right('0'+convert(varchar(2),@hour),2)+':45'
							 WHEN @minute>=45  THEN right('0'+convert(varchar(2),@hour),2)+':'+'45-'+CASE WHEN (@hour+1)=24 THEN '00' ELSE right('0'+convert(varchar(2),@hour+1),2) END+':00'
							  END AS [Minute15Code],
						CAST(CASE 
								  WHEN @minute<15 THEN CAST(@hour as varchar(2))+'00'
								  WHEN @minute>=15 and @minute<30 THEN CAST(@hour as varchar(2))+'15'
								  WHEN @minute>=30 and @minute<45 THEN CAST(@hour as varchar(2))+'30'
								  WHEN @minute>=45 THEN CAST(@hour as varchar(2))+'45' 
								  END as INT) AS [Minute15Key]
	 
	  

		SET @minute=@minute+1

		END

	SET @hour=@hour+1

END


SET @hour=0

WHILE @hour<24

BEGIN

	SET @minute=0
	INSERT INTO [silver].[Dim_Time]
	(
	 [Ekey_Time]
	,[TimeCode]
	,[Hour12Code]	
	,[Hour12Key]		
	,[MinuteCode]	
	,[MinuteKey]		
	,[AMPMCode]		
	,[Hour24Code]	
	,[Hour24Key]		
	,[Minute15Code]  
	,[Minute15Key]	
	)

	select 
			CASE WHEN @minute=0 THEN right('0'+convert(varchar(2),@hour),2)+'00'+CASE WHEN (@hour+1)=24 THEN '00' ELSE right('0'+convert(varchar(2),@hour+1),2) END+'00'  
			END AS DW_EK_TIME,
			CASE WHEN @minute=0 THEN right('0'+convert(varchar(2),@hour),2)+':00-'+CASE WHEN (@hour+1)=24 THEN '00' ELSE right('0'+convert(varchar(2),@hour+1),2) END+':00' 
			END AS TimeCode,
			right('0'+convert(varchar(2),@hour%12),2) AS [Hour12Code],
			@hour%12 AS [Hour12Key],
			CASE WHEN @minute=0 THEN 'INVALID' END AS [MinuteCode],
			CASE WHEN @minute=0 THEN -1 END AS [MinuteKey],
			case when @hour<12 then 'AM' else 'PM' end AS AMPMCode,
			right('0'+convert(varchar(2),@hour),2) AS [Hour24Code],
			@hour AS [Hour24Key],
			CASE 
				 WHEN @minute=0 THEN 'INVALID'
				 END AS [Minute15Code],
			CAST(CASE 
					  WHEN @minute=0 THEN -1
					  END as INT) AS [Minute15Key]
	 
	  

	SET @hour=@hour+1

END
GO


