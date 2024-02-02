CREATE PROCEDURE Audit.ExtractEnd
    @ExtractId BIGINT,
    @Status NVARCHAR (20) = 'Succeeded',
    @RecordsSelected INT = 0,
    @RecordsInserted INT = 0,
    @RecordsFailed INT = 0,
    @RecordsDiscarded INT = 0
AS
BEGIN
    SET NOCOUNT ON

    BEGIN TRY

        UPDATE Audit.ExtractLog
        SET [Status] = @Status,
            [EndTime] = GETDATE(),
            [RecordsSelected] = @RecordsSelected,
            [RecordsInserted] = @RecordsInserted,
            [RecordsFailed] = @RecordsFailed,
            [RecordsDiscarded] = @RecordsDiscarded
        WHERE [Id] = @ExtractId
	
	END TRY

	BEGIN CATCH

        -- Ignore any errors
		
	END CATCH

END