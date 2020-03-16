/* Create tables for the processed data */
/* This file creates tables using corrected normalisation tables */

CREATE TABLE salfordMove.dbo.APPLICATIONS(
	applicationID INT NOT NULL,
	applicationName NVARCHAR(10)
);

CREATE TABLE salfordMove.dbo.NETWORKS(
	networkID INT NOT NULL,
	networkName NVARCHAR(10)
);

CREATE TABLE salfordMove.dbo.SENSORS(
	sensorID INT NOT NULL,
	applicationID INT NOT NULL,
	networkID INT NOT NULL,
	sensorName NVARCHAR(MAX)
);

CREATE TABLE salfordMove.dbo.DATA_TYPES(
	dTypeID INT NOT NULL,
	dataType NVARCHAR(20) NOT NULL
);

CREATE TABLE salfordMove.dbo.READINGS(
	readingID INT NOT NULL,
	dataMessageGUID GUID NOT NULL,
	sensorID INT NOT NULL,
	dTypeID INT NOT NULL,
	reading NVARCHAR(5) NOT NULL,
	messageType NVARCHAR(5)
);

CREATE TABLE salfordMove.dbo.SIGNAL_STATUS(
	signalID INT NOT NULL,
	dataMessageGUID GUID NOT NULL,
	signalStrength FLOAT
)

CREATE TABLE salfordMove.dbo.BATTERY_STATUS(
	batteryID INT NOT NULL,
	dataMessageGUID GUID NOT NULL,
	batteryLevel INT
)

CREATE TABLE salfordMove.dbo.PENDING_CHANGES(
	changeID INT NOT NULL,
	dataMessageGUID GUID NOT NULL,
	pendingChange BIT 
)

CREATE TABLE salfordMove.dbo.SENSOR_VOLTAGE(
	voltageID INT NOT NULL,
	dataMessageGUID GUID NOT NULL,
	voltage FLOAT
)
