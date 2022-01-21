# Title: Collected Data Normaliser
# Description: Fetches the data that has already been 
# 	received from the Monnit servers from the SQL DB 
# 	and processes it into it's normalised form using 
# 	the updated model.
#	Processed data is saved to CSV and XLSX.
# Author: Ethan Bellmer
# Date: 05/08/2020
# Version: 2.0


# Import libraries
import json
import pandas as pd
import numpy as np
import os
from decouple import config, Csv
import re
from uuid import UUID

from move_functions.functions import rmTrailingValues, aqProcessing, filterNetwork, split_dataframe_rows, sortSensors, pivotTable, toXLSX
from move_functions.db import dbConnect, execProcedureNoReturn


# Open file containing the sensor types to look for
#sensor_types = config('MONNIT_SENSOR_TYPES', cast=Csv())
SENSOR_TYPES = config('MONNIT_SENSOR_TYPES', cast=lambda v: [s.strip() for s in v.split(',')])
SENSOR_COLUMNS = config('MONNIT_SENSOR_COLUMNS', cast=lambda v: [s.strip() for s in v.split(',')])
DELIMETERS = config('MONNIT_DELIMETERS', cast=lambda v: [s.strip() for s in v.split('*')])


#SQL Server connection info
db_driver = config('DB_DRIVER')
db_server = config('AZURE_DB_SERVER')
db_database = config('AZURE_DB_DATABASE')
db_usr = config('AZURE_DB_USR')
db_pwd = config('AZURE_DB_PWD')

METHOD = config('GET_METHOD', default=None)


# Formatted connection string for the SQL DB.
SQL_CONN_STR = "DRIVER={0};SERVER={1};Database={2};UID={3};PWD={4};".format(db_driver, db_server, db_database, db_usr, db_pwd)

# Directory to save CSV and XLSX files to.
CSV_DIR = os.getcwd() + '/data/csv/'
XLSX_DIR = os.getcwd() + '/data/xlsx/'

XLSX_NAME = XLSX_DIR + 'sensorDataSplitPivot_Full.xlsx'


pivotValues = ['rawData', 'dataValue', 'plotValues']
pivotIndex = ['messageDate']
pivotColumns = ['dataType', 'plotLabels', 'sensorName']


# Main Body
if (METHOD == "CSV"):
	print('Getting data from CSV')
	oldData = pd.read_csv(os.getcwd() + "/data/csv/iMonnit_Complete.csv")

	# JOIN SENSOR NAMES HERE
	#sNames = pd.read_csv(os.getcwd() + "/data/get/sensorNames.csv")

	#oldData = oldData.join(sNames.set_index('sensorID'), on = "sensorID")

else:
	# Create a new DB object
	conn = dbConnect()
	# Create a new cursor from established DB connection
	cursor = conn.cursor()
	# Select all the data stored in the old DB form
	SQL = "SELECT * FROM salfordMove.dbo.sensorData"

	print('Getting data from DB')
	oldData = pd.read_sql(SQL,conn)


oldData.dropna(subset=['sensorName', 'rawData'], inplace=True)

print('Pre-processing AQ Sensor Data')
oldData = aqProcessing(oldData)
print('Removing trailing integers')
oldData = rmTrailingValues(oldData, SENSOR_TYPES)

# Split the dataframe to move concatonated values to new rows
print('Splitting DataFrame')
splitDf = split_dataframe_rows(oldData, SENSOR_COLUMNS, DELIMETERS)

if 'pendingChanges' in splitDf.columns:
	# Use the Pandas 'loc' function to find and replace pending changes in the dataset
	print('Converting Pending Changes')
	splitDf.loc[(splitDf.pendingChange == 'False'), 'pendingChange'] = 0
	splitDf.loc[(splitDf.pendingChange == 'True'), 'pendingChange'] = 1
else:
	splitDf['pendingChanges'] = 0

if not 'networkID' in splitDf.columns:
	splitDf['networkID'] = 58947

# Pass the loaded dataframe into a function that will filter out any networks that don't involve MOVE
filteredDF = filterNetwork(splitDf, 58947)
# Sort the dataframe by sensorID and remove index names
filteredDF = sortSensors(filteredDF, 'sensorID')

# Split the dataframe into separate dataframes by sensorID
for i, x in filteredDF.groupby('sensorID'):
	# Export the un-pivoted data
	with pd.ExcelWriter(XLSX_DIR + 'sensorData_full.xlsx', engine="openpyxl") as writer: # pylint: disable=abstract-class-instantiated
		# Export the data to a single XLSX file with a worksheet per sensor 
		x.to_excel(writer, sheet_name = str(i))
	
	# Convert the dataframe to a pivot table 
	processedDF = pivotTable(x, pivotValues, pivotIndex, pivotColumns, np.sum)

	# Export the processed data
	toXLSX(processedDF, i, XLSX_NAME)
	#toCSV(processedDF, i)

if 'conn' in globals():
	# Close DB connection
	conn.close()
