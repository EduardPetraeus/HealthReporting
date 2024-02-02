CREATE   PROCEDURE Utility.[GetNonKeyColumns] 
(
	@Database sysname, 
	@Schema sysname,
    @Table sysname
)
AS
SET NOCOUNT ON
DECLARE @result TABLE(
    TABLE_QUALIFIER sysname,
    TABLE_OWNER sysname,
    TABLE_NAME  sysname,
    COLUMN_NAME sysname,
    KEY_SEQ smallint,
    PK_NAME sysname)

DECLARE @cmd nvarchar(max) = N'
SELECT
    ''['' + COLUMN_NAME + '']''
FROM [{db}].INFORMATION_SCHEMA.COLUMNS
WHERE DATA_TYPE NOT IN (''ntext'')
    AND TABLE_SCHEMA = @Schema
    AND TABLE_NAME = @Table
EXCEPT
SELECT ''['' + a.COLUMN_NAME + '']''
FROM [{db}].INFORMATION_SCHEMA.COLUMNS a
INNER JOIN [{db}].INFORMATION_SCHEMA.TABLE_CONSTRAINTS b
    ON a.TABLE_SCHEMA = b.TABLE_SCHEMA
    AND a.TABLE_NAME = b.TABLE_NAME
INNER JOIN [{db}].INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
    ON b.CONSTRAINT_SCHEMA = c.CONSTRAINT_SCHEMA
    AND b.CONSTRAINT_NAME = c.CONSTRAINT_NAME
    AND a.COLUMN_NAME = c.COLUMN_NAME
WHERE b.CONSTRAINT_TYPE = ''PRIMARY KEY''
    AND a.TABLE_SCHEMA = @Schema
    AND a.TABLE_NAME = @Table'

SET @cmd = REPLACE(@cmd, '{db}', @Database)
EXEC sp_executesql @cmd, N'@Schema SYSNAME, @Table SYSNAME', @Schema, @Table