# Title: Monnit Webhook Processor
# Description: Receives the webhook from the Monnit servers 
#              and stores the data into an SQL Server database,
#			   as well as processing the data to separate 
# 			   concatonated data from the received data.
# Author: Ethan Bellmer
# Date: 16/01/2020
# Version: 2.0

# Venv activation is blocked by default because the process isn't singed, so run this first:
# Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process

# Import libraries
import sys
from flask import Flask, request, abort, current_app, g
from flask.cli import with_appcontext
from flask_basicauth import BasicAuth
from werkzeug.serving import WSGIRequestHandler
import json
import pandas as pd
import os
import traceback
import datetime

from pandas import json_normalize

import pyodbc

from uuid import UUID


# Variable declarations
JSON_NAME = 'monnit_' + str(datetime.datetime.now()) + '.json'
CSV_DIR = os.getcwd() + '/data/csv/'
JSON_DIR = os.getcwd() + '/data/json/'

#POST credentials info
with open("./config/.postCreds.json") as f:
	postCreds = json.load(f)

#SQL Server connection info
with open("./config/.dbCreds.json") as f:
	dbCreds = json.load(f)

# Formatted connection string for the SQL DB.
SQL_CONN_STR = 'DSN=' + dbCreds['SERVER'] + ';Database=' + dbCreds['DATABASE'] + ';Trusted_Connection=no;UID=' + dbCreds['UNAME'] + ';PWD=' + dbCreds['PWD'] + ';'

# Open file containing the sensor types to look for
with open('./config/sensorTypes.txt') as f:
    sensorTypes = f.read().splitlines()

# Flask web server
app = Flask(__name__)

app.config['BASIC_AUTH_USERNAME'] = postCreds['UNAME']
app.config['BASIC_AUTH_PASSWORD'] = postCreds['PWD']

app.config['BASIC_AUTH_FORCE'] = True

basic_auth = BasicAuth(app)


# Function to save the recieved JSON file to disk
def jsonDump(struct):
	print('JSON dump')
	# Open a file for writing, filename will always be unique so append functions uneccessary
	with open(JSON_DIR + JSON_NAME, 'w') as f:
		# Save the JSON to a JSON file on disk
		json.dump(struct, f)

# Function to save the processed data to a CSV
def csvDump(fileName, struct, index_set = False, index_label_usr = False):
	print('CSV Dump')
	if os.path.exists(CSV_DIR + fileName + '.csv'):
		with open(CSV_DIR + fileName + '.csv', 'a', encoding="utf-8", newline="") as fd:
			struct.to_csv(fd, header=False, index=index_set)
	else:
		struct.to_csv(CSV_DIR + fileName + '.csv', index=index_set, index_label = index_label_usr)

# Convert returned strings from the DB into GUID
def strToUUID(struct):
	# Remove the leading and trailing characters from the ID
	struct = struct.replace("[('", "")
	struct = struct.replace("', )]", "")
	# Convert trimmed string into a GUID (UUID)
	g.strUUID =  UUID(struct)

	# Return to calling function
	return g.strUUID

# Function for establishing a connection to the database
def dbConnect():
	try:
		print('Connecting to database...')
		# Create a new connection to the SQL Server using the prepared connection string
		cnxn = pyodbc.connect(SQL_CONN_STR)
	except pyodbc.Error as e:
		# Print error is one should occur
		sqlstate = e.args[1]
		print("An error occurred connecting to the database: " + sqlstate)
		abort(500)
	else:
		print("Connected to database. Proceeding")
		return cnxn

# Helper function to get DB and return it to the 
def getDB():
	if 'db' not in g:
		g.db = dbConnect()
	return g.db

def commitDB(e=None):
	if 'db' not in g:
		print('DB Connection doesn\'t exist')
	else:
		g.db.commit() # Commit the staged data to the DB

def closeDB(e=None):
	db = g.pop('db', None)

	if db is not None:
		db.close() # Close the DB connection


# Executes a Stored Procedure in the database to get or create data
def execProcedure(conn, sql, params):
	# Create new cursor from existing connection 
	cursor = conn.cursor()

	# Attempt to execute the stored procedure
	try:
		# Execute the SQL statement with the parameters prepared
		cursor.execute(sql, params)
		# Fetch all results for the executed statement
		rows = cursor.fetchall()
		while rows:
			print(rows)
			return str(rows)
			#if cursor.nextset(): # Disabled during testing, unsure if required if result will always return one result
			#	rows = cursor.fetchall()
			#else:
			#	rows = None
		# Close open database cursor
		cursor.close()

	except pyodbc.Error as e:
		# Extract the error argument
		sqlstate = e.args[1]

		# Close cursor
		cursor.close()

		# Print error is one should occur and raise an exception
		print("An error occurred executing stored procedure: " + sqlstate)
		abort(500)


# Executes a Stored Procedure in the database to create data without returning any values 
def execProcedureNoReturn(conn, sql, params):
	# Create new cursor from existing connection 
	cursor = conn.cursor()

	try:
		# Execute the SQL statement with the parameters prepared
		cursor.execute(sql, params)
		# Close open database cursor
		cursor.close()

	except pyodbc.Error as e:
		# Extract the error argument
		sqlstate = e.args[1]

		# Close cursor
		cursor.close()

		# Print error is one should occur and raise an exception
		print("An error occurred executing stored procedure (noReturn): " + sqlstate)
		print(e) # Testing
		abort(500)


# Function for splitting dataframes with concatonated values into multiple rows
# Solution provided by user: zouweilin
# Solution link: https://gist.github.com/jlln/338b4b0b55bd6984f883
# Modified to use a delimeter regex pattern, so rows can be split using different delimeters
# TODO: #2 Fix 'pop from empty stack' error while parsing through sensors without a split 
import re
def split_dataframe_rows(df,column_selectors, delimiters):
	# we need to keep track of the ordering of the columns
	print('Splitting rows...')
	regexPattern = "|".join(map(re.escape,delimiters))
	def _split_list_to_rows(row,row_accumulator,column_selector,regexPattern):
		split_rows = {}
		max_split = 0
		for column_selector in column_selectors:
			split_row = re.split(regexPattern,row[column_selector])
			split_rows[column_selector] = split_row
			if len(split_row) > max_split:
				max_split = len(split_row)
			
		for i in range(max_split):
			new_row = row.to_dict()
			for column_selector in column_selectors:
				try:
					new_row[column_selector] = split_rows[column_selector].pop(0)
				except IndexError:
					new_row[column_selector] = ''
			row_accumulator.append(new_row)

	new_rows = []
	df.apply(_split_list_to_rows,axis=1,args = (new_rows,column_selectors,regexPattern))
	new_df = pd.DataFrame(new_rows, columns=df.columns)
	return new_df


# Monnit data contains trailing values on sensors with more than one measurand, 
# 	and needs to be processed out before properly splitting.
# This function should check the name of the sensors for a list of known sensor 
# 	types and remove the trailing values.
def rmTrailingValues(df, sensors):
	print ('Removing trailing values from sensors')

	# Pre-compile the regex statement for the used sensors using the list of sensors provided via paraeter
	p = re.compile('|'.join(map(re.escape, sensors)), flags=re.IGNORECASE)
	# Locate any entries that begin with the sensor names provided in the list 
	# 	using the prepared regex and remove 4 characters from the raw data variable
	#df.loc[[bool(p.match(x)) for x in df['sensorName']], ['rawData']] = df['rawData'].str[:-4]
	df.loc[[bool(p.match(x)) for x in df['sensorName']], ['rawData']] = df.loc[[bool(p.match(x)) for x in df['sensorName']], 'rawData'].astype('str').str[:-4]

	return df


# Multiple networks can be configured on the Monnit system. 
# This function will filter out unwanted networks by keeping 
# 	networks with the IDs that are passed to the function.
def filterNetwork(df, networkID):
	print('Filtering out unwanted network')
	df = df[df.networkID == networkID]
	return df


def aqProcessing(df):
	print("Processing AQ sensor data")
	# Add an additional '0' to dataValue and rawData columns to preserve varible ordering when the variable is split
	df.loc[(df.plotLabels == '?g/m^3|PM1|PM2.5|PM10'), 'dataValue'] = "0|" + df.loc[(df.plotLabels == '?g/m^3|PM1|PM2.5|PM10'), 'dataValue']
	df.loc[(df.plotLabels == '?g/m^3|PM1|PM2.5|PM10'), 'rawData'] = "0%7c" + df.loc[(df.plotLabels == '?g/m^3|PM1|PM2.5|PM10'), 'rawData']
	# Add another occurance of 'Micrograms' to the dataType column to prevent Null entries upon splitting the dataframe. 
	df.loc[(df.dataType == 'Micrograms|Micrograms|Micrograms'), 'dataType'] = 'Micrograms|Micrograms|Micrograms|Micrograms'

	# Create a dataframe from Air Quality entries
	includedColumns = df.loc[df['plotLabels']=='?g/m^3|PM1|PM2.5|PM10']
	for i, x in includedColumns.iterrows():
		# Split the data so it can be re-rodered
		rawDataList = x.rawData.split('%7c')
		# Re-order the processed data into the proper order (PM1, 2.5, 10) and insert the original split delimiter
		includedColumns.loc[i, 'rawData'] = str(rawDataList[0]) + '%7c' + str(rawDataList[3]) + '%7c' + str(rawDataList[1]) + '%7c' + str(rawDataList[2])
			
	# Overrite the air quality data with the modified data that re-orders the variables 
	df = includedColumns.combine_first(df)

	return df


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
	sensorMessages = rmTrailingValues(sensorMessages, sensorTypes)
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
	app.run(host= '0.0.0.0', port = '80')