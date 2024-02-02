CREATE TABLE Audit.ExecutionErrorLog
(
	[Id] BIGINT IDENTITY (1, 1) NOT NULL,
	[ErrorTime] DATETIME NULL,
    [ExecutionID] BIGINT NULL,
	[SourceName] NVARCHAR(1000) NULL,
	[SourceDescription] NVARCHAR(1000) NULL,
	[ErrorCode] INT NULL,
	[ErrorDescription] NVARCHAR(1000) NULL

	/* Constraints */
	CONSTRAINT [PK_ExecutionErrorLog] PRIMARY KEY CLUSTERED ([Id])
)
GO
