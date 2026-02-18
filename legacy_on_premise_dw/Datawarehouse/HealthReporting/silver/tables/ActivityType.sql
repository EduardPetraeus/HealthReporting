CREATE TABLE [silver].[ActivityType] (
	[SK_ActivityType]           INT IDENTITY (1,1)  NOT NULL,
    [Activity Name]			    NVARCHAR(200)       NULL,
    [Activity Type]			    NVARCHAR(200)       NULL,
    [UniqueActivityTypeKey]     BIGINT				NOT NULL,  
	[DW_Datetime_Load]          DATETIME            NOT NULL,
	[DW_Pipeline_run_ID]        UNIQUEIDENTIFIER	NULL

 CONSTRAINT PK_SK_ActivityType PRIMARY KEY CLUSTERED ([SK_ActivityType])
);
GO
