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




def webhook():
	print("webhook"); sys.stdout.flush()
	if request.method == 'POST' and request.headers['uname'] == 'salford' and request.headers['pwd'] == 'MOVE-2019':
		print('Request Authenticated & JSON Recieved')

		#Store the recieved JSON file from the request 
		jsonLoad = request.json
		
		# Dump JSON to file system
		jsonDump(jsonLoad)
		
		# Load gateway and sensor message data form JSON into separate variables
		gatewayMessages = jsonLoad['gatewayMessage']
		sensorMessages = jsonLoad['sensorMessages']
		# Convert the JSONs into pandas dataframes
		gatewayMessages = json_normalize(gatewayMessages)
		sensorMessages = json_normalize(sensorMessages)


		# CONNECT TO DB HERE
		conn = dbConnect()

		# GATEWAY AND SENSOR TO DB HERE
		pushGatewayData(conn, gatewayMessages)
		pushSensorData(conn, sensorMessages)

		# ADDITIONAL PROCESSING HERE
		for i, x in sensorMessages.iterrows():
			#print("Pushing sensor message " + str(i) + " to the database.")
			dbTable = "dbo.sensorData"
			columns = "(sensorID, sensorName, applicationID, networkID, dataMessageGUID, sensorState, messageDate, rawData, dataType, dataValue, plotValues, plotLabels, batteryLevel, signalStrength, pendingChange, voltage)"

			sensorID = x['sensorID']
			sensorName = x['sensorName']
			applicationID = x['applicationID']
			networkID = x['networkID']
			dataMessageGUID = x['dataMessageGUID']
			sensorState = x['state']
			batteryLevel = x['batteryLevel']
			signalStrength = x['signalStrength']

			if x['pendingChange'] == 'False':
				pendingChange = 0
			else:
				pendingChange = 1
			sensorVoltage = x['voltage']

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

		# CALL CSV DUMP HERE
		csvDump('sensorData', sensorMessages)
		csvDump('gatewayData',gatewayMessages)

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