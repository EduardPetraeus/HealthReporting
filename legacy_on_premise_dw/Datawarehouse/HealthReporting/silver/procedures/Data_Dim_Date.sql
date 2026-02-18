CREATE PROCEDURE [silver].[data_Dim_Date]

	 @Dato DATETIME
    ,@Slut_Dato DATETIME
	,@ExtractId BIGINT = 0
AS
BEGIN
    SET NOCOUNT ON

    -- Set første dag til mandag.
    SET DATEFIRST 1

	DECLARE @Dato_Kode INT
	DECLARE @Aar INT
	DECLARE @Kvartal INT
	DECLARE @Maaned	INT
	DECLARE @Uge INT
	DECLARE @Dag_Nr_I_Uge INT
	DECLARE @Dag_Nr_I_Aar INT
	DECLARE @Dag_Nr_I_Maaned	INT
	DECLARE @Maaned_Navn NVARCHAR (20) 
	DECLARE @Dag_Navn NVARCHAR (20)       
    DECLARE	@Skudaar BIT
	DECLARE @Uge_Nr_I_Maaned INT
	DECLARE @Maaned_Nr_I_Kvartal INT
	DECLARE @Aar_Maaned_Nr INT
    
	-- Variables bruges ikke til insert i tabel, kun i ST.
	DECLARE @IsLastDayInMonth BIT
	DECLARE	@DaysFromPalmSunday BIGINT
    DECLARE @PalmSunday DATETIME
    DECLARE @ThursdayInWeek DATETIME
    DECLARE @Sidste_Aar INT = NULL
	DECLARE @RecordsSelected BIGINT = 0
	DECLARE @RecordsInserted BIGINT = 0
	DECLARE @RecordsFailed BIGINT = 0
	DECLARE @RecordsDiscarded BIGINT = 0
	DECLARE @Status NVARCHAR (20) = 'Succeeded'

    -- Bygg kalender
    WHILE (@Dato <= @Slut_Dato)
    BEGIN

        /* -------------------------------------- *\
        If year changed recalculate palm Sunday
        http://www.assa.org.au/edm.html#Computer
        \* -------------------------------------- */
        IF @Sidste_Aar IS NULL OR @Sidste_Aar <> DATEPART(YEAR, @Dato)
        BEGIN
	        -- Intermediate results
	        DECLARE @FirstDig bigint
	        DECLARE @Remain19 bigint
	        DECLARE @temp bigint 
	   
	        -- Table A to E results
	        DECLARE @tA bigint
	        DECLARE @tB bigint
	        DECLARE @tC bigint
	        DECLARE @tD bigint
	        DECLARE @tE bigint        
	   
	        DECLARE @d bigint
	        DECLARE @m bigint  
	   
            -- Calculate Paschal Full Moon (PFM) date
	        SET @FirstDig = DATEPART(YEAR, @Dato) / 100 -- First 2 digits of year
	        SET @Remain19 = DATEPART(YEAR, @Dato) % 19  -- Remainder of year / 19

	        SET @temp = (@FirstDig - 15) / 2 + 202 - 11 * @Remain19    
	    
	        SET @temp = CASE @FirstDig 
				WHEN 21 THEN @temp - 1
				WHEN 24 THEN @temp - 1
				WHEN 25 THEN @temp - 1
				WHEN 27 THEN @temp - 1
				WHEN 28 THEN @temp - 1
				WHEN 29 THEN @temp - 1
				WHEN 30 THEN @temp - 1
				WHEN 31 THEN @temp - 1
				WHEN 32 THEN @temp - 1
				WHEN 34 THEN @temp - 1
				WHEN 35 THEN @temp - 1
				WHEN 38 THEN @temp - 1
				WHEN 33 THEN @temp - 2
				WHEN 36 THEN @temp - 2
				WHEN 37 THEN @temp - 2
				WHEN 39 THEN @temp - 2
				WHEN 40 THEN @temp - 2
				ELSE @temp
			END    
	   
	        SET @temp = @temp % 30

	        SET @tA = @temp + 21
		    IF (@temp = 29)
                SET @tA = @tA - 1
		    IF (@temp = 28 And @Remain19 > 10)
                SET @tA = @tA - 1
	   
	        -- Find the next Sunday
	        SET @tB = (@tA - 19) % 7
	    
	        SET @tC = (40 - @FirstDig) % 4
		    IF (@tC = 3)  
			    SET @tC = @tC + 1
		    IF (@tC > 1)  
			    SET @tC = @tC + 1
	       
	        SET @temp = DATEPART(YEAR, @Dato) % 100
	        SET @tD = (@temp + @temp / 4) % 7
	    
	        SET @tE = ((20 - @tB - @tC - @tD) % 7) + 1
	        SET @d = @tA + @tE

	        -- Return the date
	        IF (@d > 31) 
	        BEGIN
		      SET @d = @d - 31
		      SET @m = 4
	        END
	        ELSE
	        BEGIN
		      SET @m = 3
	        END

	        SET @PalmSunday = CONVERT(DATETIME, CONVERT(NVARCHAR, DATEPART(YEAR, @Dato)) +
                                                RIGHT('0' + CONVERT(NVARCHAR, @m), 2) + 
                                                RIGHT('0' + CONVERT(NVARCHAR, @d), 2), 112) - 7
        END
        /* -------------------------------------- *\
        DONE!
        \* -------------------------------------- */

		SET  @ThursdayInWeek = @Dato - DATEPART(DW, @Dato) + 1 + 3	

		-- Date key
	    SET @Dato_Kode = DATEPART (YEAR, @Dato) * 10000 +
	                   DATEPART (MONTH, @Dato) * 100 +
	                   DATEPART (DAY, @Dato)
		
		-- Get vital date informations
		
		SET Language Danish

		SET @Aar = DATEPART(YEAR, @Dato)
		

		SET @Kvartal = DATEPART(QUARTER, @Dato) 
		
		SET @Maaned = DATEPART(MONTH, @Dato)
		
        SET @Uge = DATEPART(ISO_WEEK, @Dato)

		SET @Dag_Nr_I_Uge= DATEPART(DW, @Dato)

		SET @Dag_Nr_I_Aar = DATEPART(DY, @Dato)
		
		SET @Dag_Nr_I_Maaned = DATEPART(DAY, @Dato)

		SET @Maaned_Navn = DATENAME(MONTH, @Dato)
		
		SET @Dag_Navn = DATENAME (WEEKDAY, @Dato)

        SET @DaysFromPalmSunday = DATEDIFF(DAY, @PalmSunday, @Dato)

		SET @Uge_Nr_I_Maaned = DATEPART(DAY, DATEDIFF(DAY, 0, @Dato)/7 * 7)/7 + 1

		SET @Maaned_Nr_I_Kvartal = CASE WHEN @Maaned % 3 = 0 THEN 3 ELSE @Maaned % 3 END

		SET @Aar_Maaned_Nr = CONCAT(@Aar, RIGHT('00' + CAST(@Maaned AS NVARCHAR(2)), 2))

        -- Sidste dagen i maaned.
        IF DATEPART(DAY, DATEADD(DAY, 1, @Dato)) = 1
            SET @IsLastDayInMonth = 1
        ELSE
            SET @IsLastDayInMonth = 0

		-- Skudaar	
        IF ((@Aar % 4 = 0) AND (@Aar % 100 != 0 OR @Aar % 400 = 0))
			SET @Skudaar = 1
		ELSE 
			SET @Skudaar = 0

		-- Insert værdier i tabel.
		INSERT [silver].[Dim_Date] (		

			 [SK_Date]
			,[Dato]
			,[Aar]
			,[Kvartal]
			,[Maaned]
			,[Uge]
			,[Dag_Nr_I_Uge] 
			,[Dag_Nr_I_Aar] 
			,[Dag_Nr_I_Maaned]
			,[Maaned_Navn] 
			,[Dag_Navn]   
			,[Skudaar]
			,[Uge_Nr_I_Maaned]
			,[Maaned_Nr_I_Kvartal]
			,[Aar_Maaned_Nr]
		
		 )
		VALUES 
		(
			 @Dato_Kode
			,@Dato
			,@Aar
			,@Kvartal
			,@Maaned
			,@Uge
			,@Dag_Nr_I_Uge
			,@Dag_Nr_I_Aar 
			,@Dag_Nr_I_Maaned
			,@Maaned_Navn 
			,@Dag_Navn   
			,@Skudaar
			,@Uge_Nr_I_Maaned
			,@Maaned_Nr_I_Kvartal
			,@Aar_Maaned_Nr
		)


		-- Increment dato med et.
		SELECT @Dato = DATEADD(DAY, 1, @Dato)

	END

	SET @RecordsSelected = (
		SELECT COUNT(*) FROM [silver].[Dim_Date]
			)


	UPDATE LZDB.Audit.ExtractLog
	SET  [RecordsSelected]=@RecordsSelected
		,[RecordsInserted] = @RecordsSelected
		,[RecordsFailed] = @RecordsFailed
		,[RecordsDiscarded] = @RecordsDiscarded
		,[Status] = @Status
        ,[EndTime] = GETDATE()
	WHERE [Id] = @ExtractId	

END
GO