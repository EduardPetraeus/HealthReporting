CREATE FUNCTION [Utility].[GetTextValue]
(
  @AlphaNumericString NVARCHAR(500)
)
RETURNS NVARCHAR(500)
AS
BEGIN
	DECLARE @ReturnString NVARCHAR(500);
	
    While PatIndex('%[^a-z]%', @AlphaNumericString) > 0
        Set @AlphaNumericString = Stuff(@AlphaNumericString, PatIndex('%[^a-z]%', @AlphaNumericString), 1, '');

	SET @ReturnString = NULLIF(@AlphaNumericString,'');

  RETURN @ReturnString
END;
GO