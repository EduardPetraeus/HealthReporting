CREATE TABLE Audit.ExecutionLog
(
	[Id] BIGINT IDENTITY (1, 1) NOT NULL,
	[PackageID] NVARCHAR (50) NULL,
	[PackageName] NVARCHAR (260) NULL,
    [ServerExecutionID] BIGINT NULL,
	[StartTime] DATETIME2 NULL,
	[EndTime] DATETIME2 NULL,
	[Status] NVARCHAR (20) NULL,
	[UserName] NVARCHAR (255) NULL,
    [HostName] NVARCHAR (255) NULL,
    [Database]               NVARCHAR (255) NULL,

	/* Constraints */
    CONSTRAINT [CHK_ExecutionLog_Status] CHECK ([Status] = 'Started' OR [Status] = 'Succeeded' OR [Status] = 'Failed'),
	CONSTRAINT [PK_ExecutionLog] PRIMARY KEY CLUSTERED ([Id])
)
GO