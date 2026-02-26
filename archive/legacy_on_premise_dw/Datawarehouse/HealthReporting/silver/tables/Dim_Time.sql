
CREATE TABLE [silver].[Dim_Time](
	[SK_Time]  BIGINT IDENTITY (1,1) NOT NULL,
	[Ekey_Time] [varchar](20) NOT NULL,
	[TimeCode] [varchar](20) NULL,
	[Hour12Code] [varchar](20) NULL,
	[Hour12Key] [varchar](20) NULL,
	[MinuteCode] [varchar](20) NULL,
	[MinuteKey] [int] NULL,
	[AMPMCode] [varchar](20) NULL,
	[Hour24Code] [varchar](20) NULL,
	[Hour24Key] [int] NULL,
	[Minute15Code] [varchar](20) NULL,
	[Minute15Key] [int] NULL,
 CONSTRAINT [PK_Time] PRIMARY KEY CLUSTERED 
(
	[SK_Time] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO


