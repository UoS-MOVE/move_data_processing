CREATE PROCEDURE PROC_INSERT_DATA(@sensorID AS INT, @applicationID AS INT, @networkID AS INT, @sensorName AS NVARCHAR(MAX), @dataMessageGUID AS UNIQUEIDENTIFIER, @dTypeID AS UNIQUEIDENTIFIER, @reading AS NVARCHAR(5), @messageType AS NVARCHAR(5), @signalStrength AS FLOAT, @batteryLevel AS INT, @pendingChange AS BIT, @voltage AS FLOAT)
AS
BEGIN
	SELECT sensorID
	FROM salfordMove.dbo.SENSORS
	WHERE sensorID = @sensorID
END;



(SENSORS)
sensorID
applicationID
networkID
sensorName

(READINGS)
dataMessageGUID
sensorID
dTypeID
reading / rawData
messageType

(SIGNAL_STATUS)
sensorID
dataMessageGUID
signalStrength

(BATTERY_STATUS)
sensorID
dataMessageGUID
batteryLevel


(PENDING_CHANGES)
sensorID
dataMessageGUID
pendingChange

(SENSOR_VOLTAGE)
sensorID
dataMessageGUID
voltage