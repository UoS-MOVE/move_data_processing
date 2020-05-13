# Title: Monnit Webhook Processor
# Description: Receives the webhook from the Monnit servers 
#              and stores the data into an SQL Server database,
#			   as well as processing the data to separate 
# 			   concatonated data from the received data.
# Author: Ethan Bellmer
# Date: 16/01/2020
# Version: 1.0

# Venv activation is blocked by default because the process isn't singed, so run this first:
# Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process

# Import libraries
import sys
from flask import Flask, request, abort
import json
import pandas as pd
import os
import traceback
import datetime

from pandas.io.json import json_normalize

import pyodbc


# Variable declarations
JSON_NAME = 'monnit_' + str(datetime.datetime.now()) + '.json'
CSV_DIR = os.getcwd() + '/data/csv/'
JSON_DIR = os.getcwd() + '/data/json/'

#SQL Server connection info
with open(".dbCreds") as f:
	dbCreds = json.load(f)


DATABASE = dbCreds['DATABASE']
UNAME = dbCreds['UNAME']
PWD = dbCreds['PWD']

# Formatted connection string for the SQL DB.
SQL_CONN_STR = 'DSN=Salford-SQL-Server;Database='+DATABASE+';Trusted_Connection=no;UID='+UNAME+';PWD='+PWD+';'

# Flask web server
app = Flask(__name__)


# Main body
@app.route('/', methods=['POST'])


def dbConnect():
	print('dbConnect')
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
		return cnxn

def jsonDump(struct):
	print('JSON dump')
	with open(JSON_DIR + JSON_NAME, 'w') as f:
		json.dump(struct, f)

def csvDump(fileName, struct):
	print('CSV Dump')
	if os.path.exists(CSV_DIR + fileName + '.csv'):
		with open(CSV_DIR + fileName + '.csv', 'a') as fd:
			struct.to_csv(fd, header=False, index=False)
	else:
		struct.to_csv(CSV_DIR + fileName + '.csv', index=False)

def pushGatewayData(conn, struct):
	print('Push gateway data')

	# Create a new cursor from the connection object
	cursor = conn.cursor()

	# Push gateway data to the database
	for i, x in struct.iterrows():
		print("Pushing gateway message " + str(i) + " to the database.")
		dbTable = "dbo.gatewayData"
		columns = "(gatewayID, gatewayName, accountID, networkID, messageType, gatewayPower, batteryLevel, gatewayDate, gatewayCount, signalStrength, pendingChange)"

		gatewayID = x['gatewayID']
		gatewayName = x['gatewayName']
		accountID = x['accountID']
		networkID = x['networkID']
		messageType = x['messageType']
		gatewayPower = x['power']
		batteryLevel = x['batteryLevel']
		gatewayDate = x['date']
		gatewayCount = x['count']
		signalStrength = x['signalStrength']

		if x['pendingChange'] == 'False':
			pendingChange = 0
		else:
			pendingChange = 1

		try:
			# Execute query on database
			cursor.execute("INSERT INTO " + dbTable + columns + " VALUES (" + str(gatewayID) + ",'" + str(gatewayName) + "'," + str(accountID) + "," + str(networkID) + "," + str(messageType) + "," + str(gatewayPower) + "," + str(batteryLevel) + ",'" + str(gatewayDate) + "'," + str(gatewayCount) + "," + str(signalStrength) + "," + str(pendingChange) + ")")
			#cursor.execute("INSERT INTO " + dbTable + columns + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [gatewayID, "'"gatewayName"'", accountID, messageType, gatewayPower, batteryLevel, datetime.date(gatewayDate), gatewayCount, signalStrength, pendingChange])
			conn.commit()

		except pyodbc.Error as e:
			sqlstate = e.args[1]

			# Close cursor
			cursor.close()

			# Print error is one should occur and raise an exception
			print("An error occurred inserting gateway data to database: " + sqlstate)
			abort(500)

def pushSensorData(conn, struct):
	print('Push sensor data')

	# Create a new cursor from the connection object
	cursor = conn.cursor()

	# Push sensor data to the database
	for i, x in struct.iterrows():
		print("Pushing sensor message " + str(i) + " to the database.")
		dbTable = "dbo.sensorData"
		columns = "(sensorID, sensorName, applicationID, networkID, dataMessageGUID, sensorState, messageDate, rawData, dataType, dataValue, plotValues, plotLabels, batteryLevel, signalStrength, pendingChange, voltage)"

		sensorID = x['sensorID']
		sensorName = x['sensorName']
		applicationID = x['applicationID']
		networkID = x['networkID']
		dataMessageGUID = x['dataMessageGUID']
		sensorState = x['state']
		messageDate = x['messageDate']
		rawData = x['rawData']
		dataType = x['dataType']
		dataValue = x['dataValue']
		plotValues = x['plotValues']
		plotLabels = x['plotLabels']
		batteryLevel = x['batteryLevel']
		signalStrength = x['signalStrength']

		if x['pendingChange'] == 'False':
			pendingChange = 0
		else:
			pendingChange = 1
		sensorVoltage = x['voltage']

		try:
			# Execute query on database
			cursor.execute("INSERT INTO " + dbTable + columns + " VALUES (" + str(sensorID) + ",'" + str(sensorName) + "'," + str(applicationID) + "," + str(networkID) + ",'" + str(dataMessageGUID) + "'," + str(sensorState) + ",'" + str(messageDate) + "','" + str(rawData) + "','" + str(dataType) + "','" + str(dataValue) + "','" + str(plotValues) + "','" + str(plotLabels) + "'," + str(batteryLevel) + "," + str(signalStrength) + ',' + str(pendingChange) + ',' + str(sensorVoltage) + ")")
			#cursor.execute("INSERT INTO " + dbTable + columns + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [sensorID, "'"sensorName"'", applicationID, networkID, "'"dataMessageGUID"'", sensorState, datetime.date(messageDate), "'"rawData"'", "'"dataType"'", "'"dataValue"'", "'"plotValues"'", "'"plotLabels"'", batteryLevel, signalStrength, pendingChange])
			conn.commit()

		except pyodbc.Error as e:
			sqlstate = e.args[1]

			# Close cursor and database connection

			cursor.close()

			# Print error is one should occur and raise an exception
			print("An error occurred inserting sensor data to database: " + sqlstate)
			abort(500)

	# Close cursor and database connection
	cursor.close()



# Function for splitting dataframes with concatonated values into multiple rows
# Solution provided by user: zouweilin
# Solution link: https://gist.github.com/jlln/338b4b0b55bd6984f883
# Modified to use a delimeter regex pattern, so rows can be split using different delimeters
import re
def split_dataframe_rows(df,column_selectors, delimiters):
	# we need to keep track of the ordering of the columns
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


# Primary (main) function
def webhook():
	print("webhook"); sys.stdout.flush()
	if request.method == 'POST' and request.headers['uname'] == 'salford' and request.headers['pwd'] == 'MOVE-2019':
		print('Request Authenticated & JSON Recieved')

		#Store the recieved JSON file from the request 
		jsonLoad = request.json
		
		# Dump JSON to file system
		## To be disabled for production use ##
		jsonDump(jsonLoad)

		# Load gateway and sensor message data form JSON into separate variables
		gatewayMessages = jsonLoad['gatewayMessage']
		sensorMessages = jsonLoad['sensorMessages']
		# Convert the JSONs into pandas dataframes
		gatewayMessages = json_normalize(gatewayMessages)
		sensorMessages = json_normalize(sensorMessages)

		# Dump the data to CSV files using the prepared functions
		## To be disabled for production use ##
		csvDump('sensorData', sensorMessages)
		csvDump('gatewayData', gatewayMessages)


		# CONNECT TO DB HERE
		conn = dbConnect()

		# GATEWAY AND SENSOR TO DB HERE - Legacy Database
		pushGatewayData(conn, gatewayMessages)
		pushSensorData(conn, sensorMessages)

		# Delimeters used in the recieved sensor JSON
		delimeters = "%2c","|"
		# The columns that need to be split to remove concatonated values
		sensorColumns = ["rawData", "dataValue", "dataType", "plotValues", "plotLabels"]
		# Split the dataframe to move concatonated values to new row
		splitDf = split_dataframe_rows(sensorMessages, sensorColumns, delimeters)



		
		# ADDITIONAL PROCESSING HERE
		for i, x in splitDf.iterrows():
			print("Processing sensor message " + str(i) + ".")
			
			# Define tables for normalised sensor data.
			dbTable_applications = "dbo.APPLICATIONS"
			dbTable_networks = "dbo.NETWORKS"
			dbTable_sensors = "dbo.SENSORS"
			dbTable_dataTypes = "dbo.DATA_TYPES"
			dbTable_readings = "dbo.READINGS"
			dbTable_signalStatus = "dbo.SIGNAL_STATUS"
			dbTable_batteryStatus = "dbo.BATTERY_STATUS"
			dbTabel_pendingChanges = "dbo.PENDING_CHANGES"
			dbTable_sensorVoltage = "dbo.SENSOR_VOLTAGE"


			# Define database columns per table
			applicationsColumns = "(applicationID)"
			networksColumns = "(networkID)"
			sensorsColumns = "(sensorID, applicationID, networkID, sensorName)"
			dataTypesColumns = "(dTypeID, dataType)" # Special table. dTypeID NEEDS to be selected for other tables and is not available in the JSON
			readingsColumns = "(readingID, dataMessageGUID, sensorID, dTypeID, reading, messageDate, messageType)" # dTypeID value to be inserted after SELECTION from DB
			signalStatusColumns = "(readingID, dataMessageGUID, signalStrength)"
			batteryStatusColumns = "(readingID, dataMessageGUID, batteryLevel)"
			pendingChangesColumns = "(readingID, dataMessageGUID, pendingChange)"
			sensorVoltageColumns = "(readingID, dataMessageGUID, voltage)"




			# Separate current dataframe row into independant variables. 
			sensorID = x['sensorID'] # Created value, obtain from own DB
			sensorName = x['sensorName']
			applicationID = x['applicationID']
			networkID = x['networkID']
			dataMessageGUID = x['dataMessageGUID']
			sensorState = x['state']
			messageDate = x['messageDate']
			rawData = x['rawData']
			dTypeID = x['dTypeID']
			dataType = x['dataType'] # Not used, replaced by dTypeID
			dataValue = x['dataValue']
			plotValues = x['plotValues']
			plotLabels = x['plotLabels']
			batteryLevel = x['batteryLevel']
			signalStrength = x['signalStrength']

			if x['pendingChange'] == 'False':
				pendingChange = 0
			else:
				pendingChange = 1
			sensorVoltage = x['voltage']


			# Check if sensorID already exists, if it does then do not inser into SENSORS table
			 




		##############

		# PUSH PROCESSED DATA TO DB
		# Conditional selection depending on sensor data
		

		# Prepare everything using Stored Procedures and push data to them?
		# Still need to organise the logic for iterating through the data structure 


		#applications - Not necessary, data is static after initial creation
		#networks - Not necessary, data is static after initial creation
		#sensors
		#data types
		#readings
		#signal status
		#battery status
		#pending changes
		#sensor voltage

		

		# Will always run
		pushMiscSensorData()

		# CLOSE DB CONNECTIONS HERE
		conn.close()

		# Return status 200 (success) to the remote client
		return '', 200


	elif request.method == 'POST' and request.headers['uname'] != 'salford' and request.headers['pwd'] != 'pwd':
		print('Authentication Failed')
		abort(400)
	else:
		print('Invalid Response')
		abort(400)

if __name__ == '__main__':
	app.run(host= '0.0.0.0', port = '80')