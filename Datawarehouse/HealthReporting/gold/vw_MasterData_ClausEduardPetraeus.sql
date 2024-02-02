CREATE VIEW [gold].[vw_MasterData_ClausEduardPetraeus]
	AS

SELECT 

 N'Claus Eduard' AS [First name]
,N'Petræus'      AS [Last name]
,N'Male'         AS [Gender]
,191			 AS [Height in CM]
,'1985-08-24'    AS [Birthday]
,DATEDIFF(YEAR,'1985-08-24',GETDATE()) AS [Age]
