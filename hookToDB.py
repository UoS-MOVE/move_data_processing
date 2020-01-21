# Title: Monnit Webhook Parser
# Description: Recieves the webhook from the Monnit servers 
#              and stores the data into an SQL Server database
# Author: Ethan Bellmer
# Date: 16/01/2020
# Version: 0.3

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

#SQL Server connection info
with open(".dbCreds") as f:
	dbCreds = json.load(f)


SERVER = dbCreds['SERVER']
DATABASE = dbCreds['DATABASE']
UNAME = dbCreds['UNAME']
PWD = dbCreds['PWD']

# Formatted connection string for the SQL DB.
SQL_CONN_STR = 'Driver={ODBC Driver 17 for SQL Server};Server='+SERVER+';Database='+DATABASE+';Trusted_Connection=no;UID='+UNAME+';PWD='+PWD+';'

# Flask web server
app = Flask(__name__)


# Main body
@app.route('/', methods=['POST'])

def webhook():
	print("webhook"); sys.stdout.flush()
	if request.method == 'POST' and request.headers['uname'] == 'salford' and request.headers['pwd'] == 'MOVE-2019':
		print('Request Authenticated & JSON Recieved')

		# Try to connect to the database
		try:
			print('Connecting to database...')
			# Create a new connection to the SQL Server using the prepared connection string
			conn = pyodbc.connect(SQL_CONN_STR)
			cursor = conn.cursor()
		except pyodbc.Error as e:
			# Print error is one should occur
			print("Error: " + str(e))
		else:
			print('Successfully connected to database')

			#Store the recieved JSON file from the request 
			jsonLoad = request.json

			with open(JSON_NAME, 'w') as f:
				json.dump(jsonLoad, f)

			# Load gateway and sensor message data form JSON into separate variables
			gatewayMessages = jsonLoad['gatewayMessage']
			sensorMessages = jsonLoad['sensorMessages']
			# Convert the JSONs into pandas dataframes
			gatewayMessages = json_normalize(gatewayMessages)
			sensorMessages = json_normalize(sensorMessages)


			# Push gateway data to the database
			for i, x in gatewayMessages.iterrows():
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
					#print(sqlstate)

					# Close cursor and database connection
					cursor.close()
					conn.close()
					# Print error is one should occur and raise an exception
					#print("Error: " + str(e) + ". An error ocurred inserting gateway data to database.")
					raise("An error occurred inserting gateway data to database: " + sqlstate)

			# Push sensor data to the database
			for i, x in sensorMessages.iterrows():
				print("Pushing sensor message " + str(i) + " to the database.")
				dbTable = "dbo.sensorData"
				columns = "(sensorID, sensorName, applicationID, networkID, dataMessageGUID, sensorState, messageDate, rawData, dataType, dataValue, plotValues, plotLabels, batteryLevel, signalStrength, pendingChange, sensorVoltage)"

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
					#print(sqlstate)

					# Close cursor and database connection
					cursor.close()
					conn.close()
					# Print error is one should occur and raise an exception
					#print("Error: " + str(e) + ". An error ocurred inserting gateway data to database.")
					raise("An error occurred inserting sensor data to database: " + sqlstate)

			# Close cursor and database connection
			cursor.close()
			conn.close()


			# Store sensor data into a CSV file
			if os.path.exists('sensorCSV.csv'):
				with open('sensorCSV.csv', 'a') as fd:
					sensorMessages.to_csv(fd, header=False, index=False)
			else:
				sensorMessages.to_csv('sensorCSV.csv', index=False)

			# Store gateway data into a CSV file
			if os.path.exists('gatewayCSV.csv'):
				with open('gatewayCSV.csv', 'a') as fd:
					gatewayMessages.to_csv(fd, header=False, index=False)
			else:
				gatewayMessages.to_csv('gatewayCSV.csv', index=False)

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