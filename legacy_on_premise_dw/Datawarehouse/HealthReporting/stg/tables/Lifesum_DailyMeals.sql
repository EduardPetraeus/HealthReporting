CREATE TABLE [stg].[Lifesum_DailyMeals]
(
     [Date]			    VARCHAR(50) NULL,
	 [Meal type]	    VARCHAR(50) NULL,
	 [Title]		    NVARCHAR(200) NULL,
	 [Amount]			VARCHAR(50) NULL,
	 [Serving]		    VARCHAR(50) NULL,
	 [Amount in grams]  VARCHAR(50) NULL,
	 [Calories]		    VARCHAR(50) NULL,
	 [Carbs]			VARCHAR(50) NULL,
	 [Carbs fiber]	    VARCHAR(50) NULL,
	 [Carbs sugar]	    VARCHAR(50) NULL,
	 [Fat]			    VARCHAR(50) NULL,
	 [Fat saturated]	VARCHAR(50) NULL,
	 [Fat unsaturated]  VARCHAR(50) NULL,
	 [Cholesterol]	    VARCHAR(50) NULL,
	 [Protein]		    VARCHAR(50) NULL,
	 [Potassium]		VARCHAR(50) NULL,
	 [Sodium]           VARCHAR(50) NULL,

	/* Metadata */
	[Meta_Id]           BIGINT IDENTITY (1,1)        NOT NULL,
	[Meta_CreateTime]   DATETIME DEFAULT GETDATE()   NOT NULL,
	[Meta_CreateJob]	BIGINT		                 NOT NULL

	CONSTRAINT PK_Lifesum_DailyMeals_Id PRIMARY KEY CLUSTERED ([Meta_Id])
)
GO