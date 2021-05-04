/* Retrieve all data and return the result */
CREATE PROCEDURE PROC_GET_ALL_DATA
AS
BEGIN
	SET NOCOUNT ON;

	SELECT s.sensorName
		,r.[messageDate]
		,r.[rawData]
		,r.[dataValue]
		,dt.[dataType]
		,r.[plotValue]
		,pl.[plotLabel]
	FROM [salfordMove].[dbo].[READINGS] AS r
	JOIN [salfordMOVE].[dbo].SENSORS AS s
		ON (r.sensorID = s.sensorID)
	JOIN [salfordMOVE].[dbo].PLOT_LABELS as pl
		ON (r.plotLabelID = pl.plotLabelID)
	JOIN [salfordMOVE].[dbo].DATA_TYPES as dt
		ON (r.dataTypeID = dt.dataTypeID)

	ORDER BY messageDate DESC
END
GO