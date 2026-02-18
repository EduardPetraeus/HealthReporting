
CREATE   PROCEDURE [Utility].[PrintLargeString]
    @String NVARCHAR(MAX)
AS
BEGIN
    DECLARE @LastBreak BIGINT = 1
    DECLARE @NextBreak BIGINT = 1
    DECLARE @LastOffset TINYINT = 1
    DECLARE @NextOffset TINYINT = 1

    SET @String = REPLACE(REPLACE(@String, CHAR(13) + CHAR(10), CHAR(10)), CHAR(13), CHAR(10))

    WHILE LEN(@String) > 1
    BEGIN
        SET @NextBreak = CHARINDEX(CHAR(10), @String, @LastBreak + 2) - 1
        SET @NextOffset = 2
        IF @NextBreak NOT BETWEEN 1 AND 4000
        BEGIN
            SET @NextBreak = CHARINDEX(',', @String, @LastBreak + 1)
            SET @NextOffset = 1
        END

        IF @LastBreak = 1 AND @NextBreak NOT BETWEEN 1 AND 4000
        BEGIN
            SET @LastBreak = 4000
            SET @LastOffset = 1
        END

        --PRINT CONCAT(@LastBreak, ':', @NextBreak)
        
        IF @LastBreak >= 4000 OR @NextBreak NOT BETWEEN 1 AND 4000
        BEGIN
            PRINT SUBSTRING(@String, 1, @LastBreak)
            SET @String = SUBSTRING(@String, @LastBreak + @LastOffset, LEN(@String))
            SET @LastBreak = 1
        END
        ELSE
        BEGIN
            SET @LastBreak = @NextBreak
            SET @LastOffset = @NextOffset
        END
    END

END