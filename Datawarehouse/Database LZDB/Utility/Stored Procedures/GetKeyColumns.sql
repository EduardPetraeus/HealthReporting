CREATE   PROCEDURE Utility.[GetKeyColumns] 
(
	@Database sysname, 
	@Schema sysname,
    @Table sysname
)
AS
SET NOCOUNT ON

DECLARE @cmd nvarchar(max) = N'
SELECT
    ''['' + b.COLUMN_NAME + '']''
FROM [{db}].INFORMATION_SCHEMA.TABLE_CONSTRAINTS a
INNER JOIN [{db}].INFORMATION_SCHEMA.KEY_COLUMN_USAGE b
    ON a.CONSTRAINT_CATALOG = b.CONSTRAINT_CATALOG
    AND a.CONSTRAINT_SCHEMA = b.CONSTRAINT_SCHEMA
    AND a.CONSTRAINT_NAME = b.CONSTRAINT_NAME
WHERE a.CONSTRAINT_TYPE = ''PRIMARY KEY''
    AND a.TABLE_SCHEMA = @Schema
    AND a.TABLE_NAME = @Table
    AND b.COLUMN_NAME NOT LIKE ''Meta[_]%''
ORDER BY b.ORDINAL_POSITION'

SET @cmd = REPLACE(@cmd, '{db}', @Database)

EXEC sp_executesql @cmd, N'@Schema SYSNAME, @Table SYSNAME', @Schema, @Table