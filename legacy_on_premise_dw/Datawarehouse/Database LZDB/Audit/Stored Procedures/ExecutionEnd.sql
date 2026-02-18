CREATE PROCEDURE Audit.ExecutionEnd
    @Id BIGINT,
    @Status NVARCHAR(10) = 'Succeeded'
AS
BEGIN
    SET NOCOUNT ON

    BEGIN TRY

        -----------------------------------------------
        -- Update the row in the LogTask
        -----------------------------------------------
        
        UPDATE Audit.ExecutionLog
        SET [Status] = @Status, [EndTime] = GETDATE()
        WHERE [Id] = @Id
	
	END TRY

	BEGIN CATCH

        -- Ignore any errors
		
	END CATCH

END