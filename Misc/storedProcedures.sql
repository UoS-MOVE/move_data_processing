/* Check if NETWORK exists, if not create it */
CREATE PROCEDURE PROC_GET_OR_CREATE_NETWORK(@networkID INT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT networkID
		FROM salfordMove.dbo.NETWORKS
		WHERE networkID = @networkID
	)
	PRINT('NETWORK ID EXISTS IN TABLE, SKIPPING...')

	ELSE
		INSERT INTO salfordMove.dbo.NETWORKS (networkID) VALUES (@networkID)
END;
GO


/* Check if APPLICATION exists, if not create it */
CREATE PROCEDURE PROC_GET_OR_CREATE_APPLICATION(@applicationID INT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT applicationID
		FROM salfordMove.dbo.APPLICATIONS
		WHERE applicationID = @applicationID
	)
	PRINT('APPLICATION ID EXISTS IN TABLE, SKIPPING...')

	ELSE
		INSERT INTO salfordMove.dbo.APPLICATIONS (applicationID) VALUES (@applicationID)
END;
GO


/* Check if SENSOR exists, 
if not create it and return the generated sensorID, 
if exists SELECT sensorID from table and return it */
CREATE PROCEDURE PROC_GET_OR_CREATE_SENSOR (@applicationID as INT, @networkID as INT, @sensorName as NVARCHAR(20), @sensorID UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
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


/* Check if DATA TYPE exists, 
if not create it and return the generated dataTypeID, 
if exists SELECT dataTypeID from table and return it */
CREATE PROCEDURE PROC_GET_OR_CREATE_DATA_TYPE (@dataType AS NVARCHAR(20), @dataTypeID UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
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


/* Check if PLOT LABEL exists,
if not create it and return the generated plotLabelID, 
if exists SELECT plotLabelID from table and return it */
CREATE PROCEDURE PROC_GET_OR_CREATE_PLOT_LABELS (@plotLabel AS NVARCHAR(20), @pLabelID UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT pLabelID
		FROM salfordMove.dbo.PLOT_LABELS
		WHERE plotLabel LIKE @plotLabel
	)
		SELECT @pLabelID = pLabelID
		FROM salfordMove.dbo.PLOT_LABELS
		WHERE plotLabel LIKE @plotLabel
	ELSE
		SET @pLabelID = NEWID()

		INSERT INTO salfordMove.dbo.PLOT_LABELS (pLabelID, dataType) VALUES (@plotLabel, @plotLabel)
END;
GO


/* Create a new reading in the database, 
and return the genreated readingID */
CREATE PROCEDURE PROC_CREATE_READING (@readingID UNIQUEIDENTIFIER OUTPUT, @dataMessageGUID UNIQUEIDENTIFIER, @sensorID UNIQUEIDENTIFIER, @rawData NVARCHAR(10), @dTypeID UNIQUEIDENTIFIER, @dataValue NVARCHAR(10), @pLabelID UNIQUEIDENTIFIER, @plotValue NVARCHAR(10), @messageDate DATETIME, @messageType NVARCHAR(10))
AS
BEGIN
	SET NOCOUNT ON;
	SET @readingID = NEWID()

	INSERT INTO salfordMove.dbo.READINGS (readingID, dataMessageGUID, sensorID, rawData, dTypeID, dataValue, pLabelID, plotValue, messageDate, messageType) VALUES (@readingID, @dataMessageGUID, @sensorID, @rawData, @dTypeID, @dataValue, @pLabelID, @plotValue, @messageDate, @messageType)
END;
GO


/* Create a new Signal Status entry */
CREATE PROCEDURE PROC_CREATE_SIGNAL_STATUS (@readingID UNIQUEIDENTIFIER, @dataMessageGUID UNIQUEIDENTIFIER, @signalStrength FLOAT)
AS
BEGIN
	SET NOCOUNT ON;
	INSERT INTO salfordMove.dbo.SIGNAL_STATUS (readingID, dataMessageGUID, signalStrength) VALUES (@readingID, @dataMessageGUID, @signalStrength)
END;
GO


/* Create a new Battery Status entry */
CREATE PROCEDURE PROC_CREATE_BATTERY_STATUS (@readingID UNIQUEIDENTIFIER, @dataMessageGUID UNIQUEIDENTIFIER, @batteryLevel INT)
AS
BEGIN
	SET NOCOUNT ON;
	INSERT INTO salfordMove.dbo.BATTERY_STATUS (readingID, dataMessageGUID, batteryLevel) VALUES (@readingID, @dataMessageGUID, @batteryLevel)
END;
GO


/* Create a new Pending Change entry */
CREATE PROCEDURE PROC_CREATE_PENDING_CHANGES (@readingID UNIQUEIDENTIFIER, @dataMessageGUID UNIQUEIDENTIFIER, @pendingChange BIT)
AS
BEGIN
	SET NOCOUNT ON;
	INSERT INTO salfordMove.dbo.PENDING_CHANGES (readingID, dataMessageGUID, pendingChange) VALUES (@readingID, @dataMessageGUID, @pendingChange)
END;
GO


/* Create a new Sensor Voltage entry */
CREATE PROCEDURE PROC_CREATE_SENSOR_VOLTAGE (@readingID UNIQUEIDENTIFIER, @dataMessageGUID UNIQUEIDENTIFIER, @voltage FLOAT)
AS
BEGIN
	SET NOCOUNT ON;
	INSERT INTO salfordMove.dbo.SENSOR_VOLTAGE (readingID, dataMessageGUID, voltage) VALUES (@readingID, @dataMessageGUID, @voltage)
END;
GO