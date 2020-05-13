CREATE PROCEDURE PROC_GET_OR_CREATE_SENSOR (@applicationID as INT, @networkID as INT, @sensorName as NVARCHAR(20), @sensorID UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	IF EXISTS
	(
		SELECT sensorID
		FROM salfordMove.dbo.SENSORS
		WHERE sensorName LIKE @sensorName
	)
		SELECT @sensorID = sensorID
		FROM salfordMove.dbo.SENSORS
		WHERE sensorName LIKE @sensorName
	ELSE
		SET @sensorID = NEWID()
		INSERT INTO salfordMove.dbo.SENSORS (sensorID, applicationID, networkID, sensorName) VALUES (@sensorID, @applicationID, @networkID, @sensorName)
END;
GO

/*
## For calling the procedure:
DECLARE @applicationID = applicationID
DECLARE @networkID = networkID
DECLARE @sensorName = sensorName
DECLARE @sensorID UNIQUEIDENTIFIER
EXECUTE PROC_SELECT_SENSOR_ID @applicationID, @networkID, sensorID OUTPUT
## sensorID = EXECUTE PROC_SELECT_SENSOR_ID (@applicationID, @networkID, @sensorName)
PRINT @sensorID
*/


CREATE PROCEDURE PROC_GET_OR_CREATE_DATA_TYPE (@dataType AS NVARCHAR(20), @dataTypeID UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	IF EXISTS
	(
		SELECT dTypeID
		FROM salfordMove.dbo.DATA_TYPES
		WHERE dataType LIKE @dataType
	)
		SELECT @dataTypeID = dTypeID
		FROM salfordMove.dbo.DATA_TYPES
		WHERE dataType LIKE @dataType
	ELSE
		SET @dataTypeID = NEWID()

		INSERT INTO salfordMove.dbo.DATA_TYPES (dTypeID, dataType) VALUES (@dataTypeID, @dataType)
END;
GO

/*
## For calling the procedure:
DECLARE @dataType = dataType
DECLARE @dataTypeID UNIQUEIDENTIFIER
EXECUTE PROC_GET_OR_CREATE_DATA_TYPE @dataType, @dataTypeID OUTPUT
## dataTypeID = EXECUTE PROC_SELECT_SENSOR_ID (@dataType)
PRINT @sensorCount
*/


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