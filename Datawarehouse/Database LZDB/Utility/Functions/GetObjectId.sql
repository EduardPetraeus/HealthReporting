
CREATE   FUNCTION Utility.GetObjectId(
    @db SYSNAME,
    @schema sysname,
    @table sysname)
RETURNS int 
AS
BEGIN
    RETURN (SELECT object_id(concat('[', @db, '].[', @schema, '].[', @table, ']')))
END