CREATE PROCEDURE PROC_SELECT_DATA_TYPE_ID (@dType AS NVARCHAR(20))
AS
BEGIN
	SELECT dataTypeID
	FROM salfordMove.dbo.DATA_TYPES
	WHERE dType LIKE dataType
END;
GO

CREATE PROCEDURE PROC_INSERT_DATA(@sensorID AS INT, @applicationID AS INT, @networkID AS INT, @sensorName AS NVARCHAR(MAX), @dataMessageGUID AS UNIQUEIDENTIFIER, @dTypeID AS UNIQUEIDENTIFIER, @reading AS NVARCHAR(5), @messageType AS NVARCHAR(5), @signalStrength AS FLOAT, @batteryLevel AS INT, @pendingChange AS BIT, @voltage AS FLOAT)
AS
BEGIN
	SELECT sensorID
	FROM salfordMove.dbo.SENSORS
	WHERE sensorID = @sensorID
END;
GO

/*
	How can this be done?
	
	- Message is recieved
		- JSON read and converted to Pandas DF
		- Data needs to be split to remove concatenated strings in sensor values


		-Relevant IDs are looked up in the DB
			Looked up ID's are 

	Some values will need to be looked up in the table before they can be inserted.
	Data type ID will always need to be looked up before insertion because it's reliant on a value in an external table and uses the ID to look up the data type value.
		Data type table will need to be manually populated, can possobly be included in the procedures but may be difficult to implement.

	sensorID needs to be looked up, and if it doesn't exist it needs to create a new sensor entry in the SENSORS table, but if it does exist if only needs to enter the sensorID into the other tables.

	Application ID, Network ID, and Account ID don't need to be looked up as they're always the same value and their respective tables exist for the sake of futureproofing 
	the system in case data relating to applicaiton, network, and account becomes available through the API at some point.


	Sequence:
	- Select dataTypeID from DATA_TYPES
	- Insert sensorID, applicationID, networkID, and sensorName into SENSORS if entry doesn't already exist
	-- If exists just insert sensorID into other tables

	- Insert sensorID, dataMessageID, and signalStrength into SIGNAL_STATUS
	- Insert sensorID, dataMessageID, and betteryLevel into BATTERY_STATUS
	- Insert sensorID, dataMessageID, and pendingChange into PENDING_CHANGES

	- Insert sensorID, dataMessageGUID, dTypeID, rawData, messageDate into READINGS



	A sensor message will always have 

	sensorID
	applicationID
	networkID
	sensorName

	dataMessageGUID
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

*/