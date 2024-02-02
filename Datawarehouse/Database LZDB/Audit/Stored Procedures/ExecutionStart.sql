CREATE PROCEDURE Audit.ExecutionStart
    @PackageName NVARCHAR (260),
    @PackageId NVARCHAR (50),
    @ServerExecutionID BIGINT = 0,
    @Id BIGINT = 0 OUTPUT,
	@ParentId BIGINT = -1,
    @Status NVARCHAR(10) = 'Started', 
	@Database NVARCHAR(255) = NULL
AS
BEGIN
    SET NOCOUNT ON

	DECLARE @ExecutionCurrentId table (Inserted_Id BIGINT);
        -------------------------------------------------------
        -- Insert data
        -------------------------------------------------------
        INSERT INTO Audit.ExecutionLog ([PackageID], [PackageName], [ServerExecutionID], [StartTime], [EndTime], [Status], [UserName], [HostName], [Database])
           
         OUTPUT INSERTED.Id INTO @ExecutionCurrentId

		VALUES (@PackageId, @PackageName, @ServerExecutionID, GETDATE(), GETDATE(), @Status, SYSTEM_USER, HOST_NAME(), @Database);

		SELECT @Id = Inserted_Id from @ExecutionCurrentId 


END
