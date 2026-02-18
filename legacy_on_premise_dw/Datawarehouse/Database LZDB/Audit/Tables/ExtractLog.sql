CREATE TABLE [Audit].[ExtractLog] (
    [Id]               BIGINT         IDENTITY (1, 1) NOT NULL,
    [Database]         NVARCHAR (255) NULL,
    [TableName]        NVARCHAR (100) NOT NULL,
	[Schema]		   NVARCHAR (255) NULL,
    [ExecutionId]      BIGINT         NULL,
    [Status]           NVARCHAR (20)  NULL,
    [StartTime]        DATETIME2 (7)  NULL,
    [EndTime]          DATETIME2 (7)  NULL,
    [IsFullLoad]       BIT            NULL,
    [RecordsSelected]  INT            NULL,
    [RecordsInserted]  INT            NULL,
    [RecordsFailed]    INT            NULL,
    [RecordsDiscarded] INT            NULL,
    [ArchiveId]        BIGINT         NULL,
    [ArchiveTime]      DATETIME2 (7)  NULL,
    CONSTRAINT [PK_ExtractLog] PRIMARY KEY CLUSTERED ([Id] ASC),
    CONSTRAINT [CHK_ExtractLog_Status] CHECK ([Status]='Started' OR [Status]='Succeeded' OR [Status]='Failed')
);

