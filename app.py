"""
Receives the webhook from the Monnit servers 
and stores the data into an SQL Server database,
as well as processing the data to separate 
concatonated data from the received data.
"""


__author__ = "Ethan Bellmer"
__version__ = "3"


# Import libraries
from flask import Flask, request, abort, g
from flask_basicauth import BasicAuth
from werkzeug.serving import WSGIRequestHandler
import json
import pandas as pd
import os
import datetime
from pandas import json_normalize
import pyodbc

from decouple import config, Csv

from move_functions.functions import rmTrailingValues, aqProcessing, filterNetwork, split_dataframe_rows, strToUUID, csvDump
from move_functions.db import execProcedureNoReturn, execProcedure, getDB, commitDB, closeDB

# Variable declarations
JSON_NAME = 'monnit_' + str(datetime.datetime.now()) + '.json'
CSV_DIR = os.getcwd() + '/data/csv/'
JSON_DIR = os.getcwd() + '/data/json/'


#POST credentials info
post_uname = config('MONNIT_WEBHOOK_UNAME')
post_pwd = config('MONNIT_WEBHOOK_PWD')


# Open file containing the sensor types to look for
sensor_types = config('MONNIT_SENSOR_TYPES', cast=Csv())


#SQL Server connection info
db_driver = config('DB_DRIVER')
db_server = config('AZURE_DB_SERVER')
db_database = config('AZURE_DB_DATABASE')
db_usr = config('AZURE_DB_USR')
db_pwd = config('AZURE_DB_PWD')


# Formatted connection string for the SQL DB.
#SQL_CONN_STR = 'DSN=' + db_server + ';Database=' + db_database + ';Trusted_Connection=no;UID=' + db_usr + ';PWD=' + db_pwd + ';'
SQL_CONN_STR = "DRIVER={0};SERVER={1};Database={2};UID={3};PWD={4};".format(db_driver, db_server, db_database, db_usr, db_pwd)

# Flask web server
app = Flask(__name__)
app.config['BASIC_AUTH_USERNAME'] = post_uname
app.config['BASIC_AUTH_PASSWORD'] = post_pwd
app.config['BASIC_AUTH_FORCE'] = True
basic_auth = BasicAuth(app)


# Main body
@app.route('/', methods=['POST'])
@basic_auth.required
# Primary (main) function
def webhook():
	print('Request Authenticated & JSON Recieved')

	# Store the recieved JSON file from the request 
	jsonLoad = request.json
	
	# Dump JSON to file system
	## To be disabled for production use ##
	#jsonDump(jsonLoad) # Disabled for testing as it doesn't work on Windows

	# Load gateway and sensor message data form JSON into separate variables
	gatewayMessages = jsonLoad['gatewayMessage']
	sensorMessages = jsonLoad['sensorMessages']
	# Convert the JSONs into pandas dataframes
	gatewayMessages = json_normalize(gatewayMessages)
	sensorMessages = json_normalize(sensorMessages)

	# Remove the trailing values present in the rawData field of some sensors
	sensorMessages = rmTrailingValues(sensorMessages, sensor_types)
	# Process any sensor messages for Air Quality
	sensorMessages = aqProcessing(sensorMessages)
	# Filter out messages from networks not related to MOVE
	sensorMessages = filterNetwork(sensorMessages, str(58947))

	# Delimeters used in the recieved sensor JSON
	delimeters = "%2c","|","%7c"
	# The columns that need to be split to remove concatonated values
	sensorColumns = ["rawData", "dataValue", "dataType", "plotValues", "plotLabels"]
	# Split the dataframe to move concatonated values to new rows
	splitDf = split_dataframe_rows(sensorMessages, sensorColumns, delimeters)


	# Use the Pandas 'loc' function to find and replace pending changes in the dataset
	splitDf.loc[(splitDf.pendingChange == 'False'), 'pendingChange'] = 0
	splitDf.loc[(splitDf.pendingChange == 'True'), 'pendingChange'] = 1


	# CONNECT TO DB HERE
	conn = getDB()


	# ADDITIONAL PROCESSING HERE
	for i, sensorData in splitDf.iterrows():
		print("Processing sensor message " + str(i) + ".")

		##############

		## CREATE NETWORK ##
		# Prepare SQL statement to call stored procedure to create a network entry using 
		# the networkID from the JSON
		sql = "{CALL [dbo].[PROC_GET_OR_CREATE_NETWORK] (?)}"
		# Bind the parameters that are required for the procedure to function
		params = (sensorData['networkID'])

		# Execute the stored procedure to create a network if it doesn't exist, 
		# and ignore input if exists
		print('Step 1/10: Creating network entry')
		execProcedureNoReturn(conn, sql, params)
		print('Network entry created')


		## CREATE APPLICATION ##
		# Prepare SQL statement to call stored procedure to create an application entry using 
		# the applicationID from the JSON
		sql = "{CALL [dbo].[PROC_GET_OR_CREATE_APPLICATION] (?)}"
		# Bind the applicationID used to check if app exists in DB
		params = (sensorData['applicationID'])

		# Execute the stored procedure to create an application if it doesn't exist, 
		# and ignore input if exists
		print('Step 2/10: Creating application entry')
		execProcedureNoReturn(conn, sql, params)
		print('Network application created')


		## GET OR CREATE SENSOR ##
		# pyodbc doesn't support the ".callproc" function from ODBC, 
		# so the following SQL statement is used to execute the stored procedure.
		#
		# Stored prodecure will submit the applicationID, networkID, and sensorName 
		# to the procedure, and will always recieve a sensorID in return.
		sql = """\
			DECLARE @out UNIQUEIDENTIFIER;
			EXEC [dbo].[PROC_GET_OR_CREATE_SENSOR] @applicationID = ?, @networkID = ?, @sensorName = ?, @sensorID = @out OUTPUT;
			SELECT @out AS the_output;
			"""
		# Bind the parameters that are required for the procedure to function
		params = (sensorData['applicationID'], sensorData['networkID'], sensorData['sensorName'])
		
		# Execute the procedure using the prepared SQL & parameters to 
		# create a new sensor in the DB, or get an existing one.
		print('Step 3/10: Creating or getting sensor')
		# Execute the procedure and return sensorID and convert trimmed string into a GUID (UUID)
		sensorData['sensorID'] = strToUUID(execProcedure(conn, sql, params))
		print(sensorData['sensorID'])
		

		## GET OR CREATE DATA TYPE ##
		# Prepare SQL statement to call stored procedure to a data type entry using 
		# the data type from the JSON and return a generated dataTypeID.
		sql = """\
			DECLARE @out UNIQUEIDENTIFIER;
			EXEC [dbo].[PROC_GET_OR_CREATE_DATA_TYPE] @dataType = ?, @dataTypeID = @out OUTPUT;
			SELECT @out AS the_output;
			"""
		# Bind the parameters that are required for the procedure to function
		params = sensorData['dataType']
		
		# Execute the procedure using the prepared SQL & parameters to 
		# create a new sensor in the DB, or get an existing one.
		print('Step 4/10: Creating or getting data type ID')
		sensorData['dataTypeID'] = strToUUID(execProcedure(conn, sql, params))
		print(sensorData['dataTypeID'])


		## GET OR CREATE PLOT LABELS ##
		# Prepare SQL statement to call stored procedure to a plot label entry using 
		# the data type from the JSON and return a generated plotLabelID.
		sql = """\
			DECLARE @out UNIQUEIDENTIFIER;
			EXEC [dbo].[PROC_GET_OR_CREATE_PLOT_LABELS] @plotLabel = ?, @plotLabelID = @out OUTPUT;
			SELECT @out AS the_output;
			"""
		# Bind the parameters that are required for the procedure to function
		params = sensorData['plotLabels']
		# Execute the procedure using the prepared SQL & parameters to 
		# create a new plot label in the DB, or get an existing one.
		print('Step 5/10: Creating or getting plot label ID')
		sensorData['plotLabelID'] = strToUUID(execProcedure(conn, sql, params)) ## Problem here?
		

		## GET OR CREATE READING ##
		# Prepare SQL statement to call stored procedure to create a new reading using 
		# the values aggregated from the JSON and generated variables from the DB.
		# A generated readingID will be returned. 
		sql = """\
			DECLARE @out UNIQUEIDENTIFIER;
			EXEC [dbo].[PROC_CREATE_READING] @dataMessageGUID = ?, @sensorID = ?, @rawData = ?, @dataTypeID = ?, @dataValue = ?, @plotLabelID = ?, @plotValue = ?, @messageDate = ?, @readingID = @out OUTPUT;
			SELECT @out AS the_output;
			"""
		# Bind the parameters that are required for the procedure to function
		params = (sensorData['dataMessageGUID'], sensorData['sensorID'], sensorData['rawData'], sensorData['dataTypeID'], sensorData['dataValue'], sensorData['plotLabelID'], sensorData['plotValues'], sensorData['messageDate'])
		
		# Execute the procedure using the prepared SQL & parameters to 
		# create a new reading in the DB, and return the genreated ID.
		print('Step 6/10: Creating reading, and getting ID')
		sensorData['readingID'] = strToUUID(execProcedure(conn, sql, params))
		

		## GET OR CREATE SIGNAL STATUS ##
		# Prepare SQL statement to call stored procedure to create a new signal 
		# status entry using the readingID from the DB and the dataMessgaeGUID, 
		# and signalStrength, from the JSON.
		sql = "{CALL [dbo].[PROC_CREATE_SIGNAL_STATUS] (?, ?, ?)}"
		# Bind the parameters that are required for the procedure to function
		params = (sensorData['readingID'], sensorData['dataMessageGUID'], sensorData['signalStrength'])
		
		# Execute the procedure using the prepared SQL & parameters to 
		# create a new signal status in the DB.
		print('Step 7/10: Creating signal atatus')
		execProcedureNoReturn(conn, sql, params)
		

		## GET OR CREATE BATTERY STATUS ##
		# Prepare SQL statement to call stored procedure to create a new battery status 
		# entry using the readingID from the DB and dataMessgaeGUID, and batteryLevel, from the JSON.
		sql = "{CALL [dbo].[PROC_CREATE_BATTERY_STATUS] (?, ?, ?)}"
		# Bind the parameters that are required for the procedure to function
		params = (sensorData['readingID'], sensorData['dataMessageGUID'], sensorData['batteryLevel'])
		
		# Execute the procedure using the prepared SQL & parameters to 
		# create a new battery status in the DB.
		print('Step 8/10: Creating battery status')
		execProcedureNoReturn(conn, sql, params)
		

		## GET OR CREATE PENDING CHANGES ##
		# Prepare SQL statement to call stored procedure to create a pending change 
		# entry using the readingID from the DB and dataMessgaeGUID, 
		# and pendingChange, from the JSON.
		sql = "{CALL [dbo].[PROC_CREATE_PENDING_CHANGES] (?, ?, ?)}"
		# Bind the parameters that are required for the procedure to function
		params = (sensorData['readingID'], sensorData['dataMessageGUID'], sensorData['pendingChange'])
		
		# Execute the procedure using the prepared SQL & parameters to 
		# create a new pending change in the DB.
		print('Step 9/10: Creating pending change')
		execProcedureNoReturn(conn, sql, params)
		

		## GET OR CREATE SENSOR VOLTAGE ##
		# Prepare SQL statement to call stored procedure to create a sensor voltage 
		# entry using the readingID from the DB and dataMessgaeGUID, 
		# and voltage, from the JSON.
		sql = "{CALL [dbo].[PROC_CREATE_SENSOR_VOLTAGE] (?, ?, ?)}"
		# Bind the parameters that are required for the procedure to function
		params = (sensorData['readingID'], sensorData['dataMessageGUID'], sensorData['voltage'])
		
		# Execute the procedure using the prepared SQL & parameters to 
		# create a new voltage entry in the DB.
		print('Step 10/10: Creating voltage reading')
		execProcedureNoReturn(conn, sql, params)

	# Commit data and close open database connection
	commitDB()
	closeDB()

	# Dump the data to CSV files using the prepared functions
	## To be disabled for production use ##
	csvDump('sensorData', splitDf)
	#csvDump('gatewayData', gatewayMessages)

	# Return status 200 (success) to the remote client
	return '', 200

if __name__ == '__main__':
	WSGIRequestHandler.protocol_version = "HTTP/1.1"
	app.run(host= '0.0.0.0', port= 80)