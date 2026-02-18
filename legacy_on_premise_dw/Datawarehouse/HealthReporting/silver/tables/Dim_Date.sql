CREATE TABLE [silver].[Dim_Date]
(
	 [SK_Date] INT NOT NULL
	,[Dato] DATETIME NOT NULL
	,[Aar] INT NOT NULL
	,[Kvartal] INT NOT NULL
	,[Maaned] INT NOT NULL
	,[Uge] INT NOT NULL
	,[Dag_Nr_I_Uge] INT NOT NULL
	,[Dag_Nr_I_Aar] INT NOT NULL
	,[Dag_Nr_I_Maaned] INT NOT NULL
	,[Maaned_Navn] NVARCHAR(20) NOT NULL
	,[Dag_Navn] NVARCHAR(20) NOT NULL
	,[Skudaar] BIT NOT NULL
	,[Uge_Nr_I_Maaned] INT NOT NULL
	,[Maaned_Nr_I_Kvartal] INT NOT NULL
	,[Aar_Maaned_Nr] INT NOT NULL
	,[Meta_Id] INT IDENTITY(1, 1) NOT NULL
	,[Meta_CreateTime] DATETIME DEFAULT GETDATE() NOT NULL
	CONSTRAINT PK_Dim_Dato_Meta_Id PRIMARY KEY CLUSTERED ([Meta_Id])
)
