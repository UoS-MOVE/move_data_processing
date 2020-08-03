# Title: Collected Data Normaliser
# Description: Fetches the data that has already been 
# 	received from the Monnit servers from the SQL DB 
# 	and processes it into it's normalised form using 
# 	the updated model. 
# Author: Ethan Bellmer
# Date: 09/07/2020
# Version: 1.0


# Import libraries
import sys
import json
import pandas as pd
import os
import traceback
import datetime

import pyodbc

from uuid import UUID

# Import the main webhook file so its functions can be used. 
import app
from app import execProcedureNoReturn, execProcedure, dbConnect, split_dataframe_rows, strToUUID

#SQL Server connection info
with open("./../config/.dbCreds.json") as f:
	dbCreds = json.load(f)


SERVER = dbCreds['SERVER']
DATABASE = dbCreds['DATABASE']
UNAME = dbCreds['UNAME']
PWD = dbCreds['PWD']

# Formatted connection string for the SQL DB.
SQL_CONN_STR = 'DSN='+SERVER+';Database='+DATABASE+';Trusted_Connection=no;UID='+UNAME+';PWD='+PWD+';'

# Create a new DB object
conn = dbConnect()
# Create a new cursor from established DB connection
cursor = conn.cursor()

# Select all the data stored in the old DB form
#SQL = "SELECT * FROM salfordMove.dbo.sensorData"

# pandas.read_sql_table(table_name, con, schema=None, index_col=None, coerce_float=True, parse_dates=None, columns=None, chunksize=None)
# 
oldData = pd.read_sql_table('sensorData', conn, schema="dbo", index_col=None, coerce_float=True, parse_dates=['messageDate'], columns=['sensorID', 'sensorName', 'applicationID', 'networkID', 'dataMessgaeGUID', 'sensorState', 'messageDate', 'rawData', 'dataType', 'dataValue', 'plotValues', 'plotLabels', 'batteryLevel', 'signalStrength', 'pendingChange', 'voltage'], chunksize=None)

print(oldData)

# Delimeters used in the recieved data
delimeters = "%2c","|"
# The columns that need to be split to remove concatonated values
sensorColumns = ["rawData", "dataValue", "dataType", "plotValues", "plotLabels"]
# Split the dataframe to move concatonated values to new rows
splitDf = split_dataframe_rows(oldData, sensorColumns, delimeters)



# Iterate through the fetched data, and convert it to it's normalised form
for i, sensorData in splitDf.iterrows():
	print("Processing sensor message " + str(i) + ".")

	# Convert pendingChange True/False value into a boolean 1/0
	if sensorData['pendingChange'] == 'False':
		sensorData['pendingChange'] = 0
	else:
		sensorData['pendingChange'] = 1


	##############

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