CREATE PROCEDURE Audit.ExecutionError
    @ExecutionID BIGINT = 0,
	@SourceName NVARCHAR(1000) NULL,
	@SourceDescription NVARCHAR(1000) NULL,
    @ErrorCode INT = -1,
    @ErrorDescription NVARCHAR(1000) NULL
AS
BEGIN
    SET NOCOUNT ON

    BEGIN TRY

        -------------------------------------------------------
        -- Insert data
        -------------------------------------------------------
        INSERT INTO Audit.ExecutionErrorLog ([ErrorTime], [ExecutionID], [SourceName], [SourceDescription], [ErrorCode], [ErrorDescription])
        VALUES (GETDATE(), @ExecutionID, @SourceName, @SourceDescription, @ErrorCode, @ErrorDescription);
	
	END TRY

	BEGIN CATCH

		THROW

	END CATCH

END
