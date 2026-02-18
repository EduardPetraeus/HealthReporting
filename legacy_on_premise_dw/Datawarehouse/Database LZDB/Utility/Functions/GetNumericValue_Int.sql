CREATE FUNCTION [Utility].[GetNumericValue_Int]
(
  @AlphaNumericString NVARCHAR(500)
)
RETURNS Int
AS
BEGIN
	DECLARE @IntReturn Int;
  
	IF (PATINDEX('%[^0-9]%', @AlphaNumericString) = 0 )	
		SET @IntReturn = TRY_CAST(@AlphaNumericString as Int);	
	ELSE 
		SET @IntReturn = TRY_CAST(LEFT(@AlphaNumericString, PATINDEX('%[0-9][^0-9]%', @AlphaNumericString)) as Int);

	SET @IntReturn = NULLIF(@IntReturn,0);
  RETURN @IntReturn
END;
GO