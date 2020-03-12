/* Create tables for the processed data */

CREATE TABLE salfordMOVE.dbo.UNIT_DATA(
	unitID INT NOT NULL UNIQUE,
	unitName NVARCHAR(10) NOT NULL,
	CONSTRAINT unit_pk PRIMARY KEY (unitID)
);

CREATE TABLE salfordMOVE.dbo.SENSORS(
	sensorID INT NOT NULL,
	applicationID INT NOT NULL,
	networkID INT NOT NULL,
	sensorName NVARCHAR(MAX) NOT NULL,
	sensorState INT NOT NULL,
	batteryLevel INT NOT NULL,
	signalStrength INT NOT NULL,
	pendingChange BIT NOT NULL,
	voltage FLOAT NOT NULL,
	dataMessageGUID NVARCHAR(MAX) NOT NULL,
	CONSTRAINT PK_sensors PRIMARY KEY (sensorID)
);

CREATE TABLE salfordMOVE.dbo.TEMPERATURE_DATA(
	temperatureID INT NOT NULL,
	sensorID INT NOT NULL,
	temperature FLOAT NOT NULL,
	unitID INT NOT NULL,
	messageDate DATETIME NOT NULL,
	CONSTRAINT PK_temperature PRIMARY KEY (temperatureID),
	CONSTRAINT FK_temperature_sensorID FOREIGN KEY (sensorID)
	REFERENCES dbo.SENSORS (sensorID)
	ON DELETE CASCADE
	ON UPDATE CASCADE,
	CONSTRAINT FK_temperature_unitID FOREIGN KEY (unitID)
	REFERENCES dbo.UNIT_DATA (unitID)
	ON DELETE CASCADE
	ON UPDATE CASCADE
);

CREATE TABLE salfordMOVE.dbo.HUMIDITY_DATA(
	humidityID INT NOT NULL UNIQUE,
	sensorID INT NOT NULL,
	messageDate DATETIME NOT NULL,
	humidity FLOAT NOT NULL,
	temperature FLOAT NOT NULL,
	dewpoint FLOAT NOT NULL,
	gramsPerKilogram FLOAT NOT NULL,
	humidityUnitID INT NOT NULL,
	temperatureUnitID INT NOT NULL,
	dewpointUnitID INT NOT NULL,
	gramsPerKilogramUnitID INT NOT NULL,
	CONSTRAINT PK_humidity PRIMARY KEY (humidityID),
	CONSTRAINT FK_humidity_sensorID FOREIGN KEY (sensorID)
	REFERENCES dbo.SENSORS (sensorID)
	ON DELETE CASCADE
	ON UPDATE CASCADE,
	CONSTRAINT FK_humidity_humidityUnitID FOREIGN KEY (humidityUnitID)
	REFERENCES dbo.UNIT_DATA (unitID)
	ON DELETE CASCADE
	ON UPDATE CASCADE
);

CREATE TABLE salfordMOVE.dbo.LIGHT_DATA(
	lightID INT NOT NULL UNIQUE,
	sensorID INT NOT NULL,
	messageDate DATETIME NOT NULL,
	luxData INT NOT NULL,
	lightDetect BIT NOT NULL,
	unitID INT NOT NULL,
	CONSTRAINT PK_light PRIMARY KEY (lightID),
	CONSTRAINT FK_light_sensorID FOREIGN KEY (sensorID)
	REFERENCES dbo.SENSORS (sensorID)
	ON DELETE CASCADE
	ON UPDATE CASCADE,
	CONSTRAINT FK_light_unitID FOREIGN KEY (unitID)
	REFERENCES dbo.UNIT_DATA (unitID)
	ON DELETE CASCADE
	ON UPDATE CASCADE
);

CREATE TABLE salfordMOVE.dbo.MOTION_DATA(
	motionID INT NOT NULL UNIQUE,
	sensorID INT NOT NULL,
	messageDate DATETIME NOT NULL,
	motionData BIT NOT NULL,
	CONSTRAINT PK_motion PRIMARY KEY (motionID),
	CONSTRAINT FK_motion_sensorID FOREIGN KEY (sensorID)
	REFERENCES dbo.SENSORS (sensorID)
	ON DELETE CASCADE
	ON UPDATE CASCADE
);

CREATE TABLE salfordMOVE.dbo.AIR_QUALITY_DATA(
	aqID INT NOT NULL UNIQUE,
	sensorID INT NOT NULL,
	messageDate DATETIME NOT NULL,
	gPerM3 FLOAT NOT NULL,
	PM1 FLOAT NOT NULL,
	PM2_5 FLOAT NOT NULL,
	PM10 FLOAT NOT NULL,
	unitID INT NOT NULL,
	CONSTRAINT PK_aq PRIMARY KEY (aqID),
	CONSTRAINT FK_aq_sensorID FOREIGN KEY (sensorID)
	REFERENCES dbo.SENSORS (sensorID)
	ON DELETE CASCADE
	ON UPDATE CASCADE,
	CONSTRAINT FK_aq_unitID FOREIGN KEY (unitID)
	REFERENCES dbo.UNIT_DATA (unitID)
	ON DELETE CASCADE
	ON UPDATE CASCADE
);

CREATE TABLE salfordMOVE.dbo.CO2_DATA(
	co2ID INT NOT NULL UNIQUE,
	sensorID INT NOT NULL,
	messageDate DATETIME NOT NULL,
	instantaneous FLOAT NOT NULL,
	twa FLOAT NOT NULL,
	untiID INT NOT NULL,
	CONSTRAINT PK_co2 PRIMARY KEY (co2ID),
	CONSTRAINT FK_co2_sensorID FOREIGN KEY (sensorID)
	REFERENCES dbo.SENSORS (sensorID)
	ON DELETE CASCADE
	ON UPDATE CASCADE,
	CONSTRAINT FK_co2_unitID FOREIGN KEY (unitID)
	REFERENCES dbo.UNIT_DATA (unitID)
	ON DELETE CASCADE
	ON UPDATE CASCADE
);

CREATE TABLE salfordMOVE.dbo.AIR_VELOCITY_DATA(
	avID INT NOT NULL UNIQUE,
	sensorID INT NOT NULL,
	messageDate DATETIME NOT NULL,
	airVelocity FLOAT NOT NULL,
	temperature FLOAT NOT NULL,
	airVelocityUnitID INT NOT NULL,
	temperatureUnitID INT NOT NULL,
	CONSTRAINT PK_av PRIMARY KEY (avID),
	CONSTRAINT FK_av_sensorID FOREIGN KEY (sensorID)
	REFERENCES dbo.SENSORS (sensorID)
	ON DELETE CASCADE
	ON UPDATE CASCADE,
	CONSTRAINT FK_av_airVelocityUnitID FOREIGN KEY (airVelocityUnitID)
	REFERENCES dbo.UNIT_DATA (unitID)
	ON DELETE CASCADE
	ON UPDATE CASCADE,
	CONSTRAINT FK_av_temperatureUnitID FOREIGN KEY (temperatureUnitID)
	REFERENCES dbo.UNIT_DATA (unitID)
	ON DELETE CASCADE
	ON UPDATE CASCADE
);

GO;