CREATE TABLE [meta].[Audit] (
    [AuditID]       INT              IDENTITY (1, 1) NOT NULL,
    [EntityName]    NVARCHAR (50)    NULL,
    [SubEntityName] NVARCHAR (100)   NULL,
    [TableName]     NVARCHAR (100)   NULL,
    [LoadStatus]    NVARCHAR (50)    NULL,
    [StartDateTime] DATETIME         NULL,
    [EndDateTime]   DATETIME         NULL,
    [RowsRead]      INT              NULL,
    [RowsInserted]  INT              NULL,
    [RowsUpdated]   INT              NULL,
    [RowsDeleted]   INT              NULL,
    [ExecutionID]   UNIQUEIDENTIFIER NULL,
    [ErrorCode]     NVARCHAR (50)    NULL,
    [ErrorMessage]  NVARCHAR (4000)  NULL,
    [ExecutedBy]    NVARCHAR (100)   NULL,
CONSTRAINT [PK_Audit] PRIMARY KEY CLUSTERED ([AuditID] ASC),
CONSTRAINT [CHK_Audit_LoadStatus] CHECK ([LoadStatus]='InProgress' OR [LoadStatus]='Completed' OR [LoadStatus]='Failed')
)





