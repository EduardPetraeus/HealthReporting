CREATE TABLE [silver].[BodyMeasurement] (
	[DateTime]							DATETIME NULL,
	[SK_Date]                           INT      NOT NULL,
	[SK_Time]                           BIGINT   NOT NULL,
    [Atrial fibrillation result]        SMALLINT NULL,
    [Basal Metabolic Rate (BMR)]        SMALLINT NULL,
    [BMI]							    SMALLINT NULL,
    [VO2 Max]						    SMALLINT NULL,
    [Nerve Health Score Feet]		    SMALLINT NULL,
    [Nerve Health Score left foot]	    SMALLINT NULL,
    [Nerve Health Score right foot]	    SMALLINT NULL,
    [Water %]						    SMALLINT NULL,
    [Bone %]							SMALLINT NULL,
    [Muscle %]						    SMALLINT NULL,
    [Fat Free body mass %]			    SMALLINT NULL,
    [Fat %]							    SMALLINT NULL,
    [Fat Free body mass (kg)]		    SMALLINT NULL,
    [Visceral Fat (kg)]				    SMALLINT NULL,
    [Vascular age]					    SMALLINT NULL,
    [Metabolic Age]					    SMALLINT NULL,
    [Extracellular Water]			    SMALLINT NULL,
    [Intracellular Water]			    SMALLINT NULL,
    [Fat Free Mass for segments]		SMALLINT NULL,
    [Fat Mass for segments in mass unit]SMALLINT NULL,
    [Muscle Mass for segments]			SMALLINT NULL,
    [Valvular heart disease result]		SMALLINT NULL,
    [DW_DateTime_Load_Insert]   DATETIME2 (7)    NULL,
    [DW_Pipeline_run_ID_Insert] UNIQUEIDENTIFIER NULL
);
GO

  CREATE NONCLUSTERED INDEX IX_silver_BodyMeasurement
  ON [silver].[BodyMeasurement]([SK_Date] ASC,[SK_Time] ASC)
  GO