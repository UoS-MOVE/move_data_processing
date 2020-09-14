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
import sys
import json
import pandas as pd
import numpy as np
import os
import traceback
import datetime

import pyodbc

from uuid import UUID

# Import the main webhook file so its functions can be used. 
import app
from app import dbConnect, split_dataframe_rows, rmTrailingValues, filterNetwork, aqProcessing


# Variables
# Open file containing the sensor types to look for
with open('./config/sensorTypes.txt') as f:
    sensorTypes = f.read().splitlines()

# SQL Server connection info
with open("./config/.dbCreds.json") as f:
	dbCreds = json.load(f)

# Formatted connection string for the SQL DB.
SQL_CONN_STR = 'DSN='+dbCreds['SERVER']+';Database='+dbCreds['DATABASE']+';Trusted_Connection=no;UID='+dbCreds['UNAME']+';PWD='+dbCreds['PWD']+';'

# Directory to save CSV and XLSX files to.
CSV_DIR = os.getcwd() + '/data/csv/'
XLSX_DIR = os.getcwd() + '/data/xlsx/'

XLSX_NAME = XLSX_DIR + 'sensorDataSplitPivot.xlsx'

#pivotValues = ['rawData', 'dataValue', 'plotValues']
pivotValues = ['rawData', 'dataValue', 'plotValues', 'plotLabels']
#pivotIndex = ['sensorID']
pivotIndex = ['messageDate']
#pivotColumns = ['sensorID', 'sensorName', 'applicationID', 'networkID', 'sensorState', 'messageDate', 'dataType', 'plotLabels', 'batteryLevel', 'signalStrength', 'pendingChange', 'voltage']
#pivotColumns = ['dataType', 'plotLabels']
pivotColumns = ['dataType']


# Functions

# Split the data by sensor ID and export the data to separate CSV 
# 	files and an XLSX file with separate worksheets per sensor
def sortSensors(df):
	print('Sorting and cleaning sensors')
	# Sort the values in the dataframe by their sensor ID
	df.sort_values(by = ['sensorID'], inplace = True)
	# Set the DF index to the sensor IDs
	df.set_index(keys = ['sensorID'], drop = False, inplace = True)
	# Remove existing index names
	df.index.name = None
	return df

# Pivot passed in DF to make analysis easier. 
# 'values', 'index', and 'columns', are all lists of variables
def pivotTable(df, values, index, columns, aggFunc):
	print('Pivoting data...')
	#df = pd.pivot_table(df, values=['rawData', 'dataValue', 'plotValues'], index=['sensorID'], columns=['dataType', 'plotLabels'], aggfunc=np.sum)
	df = pd.pivot_table(df, values=values, index=index, columns=columns, aggfunc=aggFunc)
	return df 

# Export passed in DF to XLSX files
def toXLSX(df, sensorID, fileName):
	print('Exporting Sensor data to XLSX...')
	# Check if file already exists, if it does then append otherwise create it
	if os.path.exists(XLSX_NAME):
		print('XLSX file exists, appending...')
		with pd.ExcelWriter(fileName, engine="openpyxl", mode='a') as writer: # pylint: disable=abstract-class-instantiated
			# Export the data to a single XLSX file with a worksheet per sensor
			df.to_excel(writer, sheet_name = str(sensorID))
	else:
		print('File does not exist, creating...')
		with pd.ExcelWriter(fileName, engine="openpyxl") as writer: # pylint: disable=abstract-class-instantiated
			# Export the data to a single XLSX file with a worksheet per sensor 
			df.to_excel(writer, sheet_name = str(sensorID))

# Export the data to separate CSV files
def toCSV(df, sensorID):
	print('Exporting Sensor data to CSV')
	p = os.path.join(CSV_DIR, "sensor_{}.csv".format(sensorID))
	df.to_csv(p, index=False)


# Main Body
# Create a new DB object
conn = dbConnect()
# Create a new cursor from established DB connection
cursor = conn.cursor()

# Select all the data stored in the old DB form
SQL = "SELECT TOP(200) * FROM salfordMove.dbo.sensorData"

print('Getting data from DB')
oldData = pd.read_sql(SQL,conn)

oldData = aqProcessing(oldData)
oldData = rmTrailingValues(oldData, sensorTypes)

# Delimeters used in the recieved data
delimeters = "%2c","|","%7c"
# The columns that need to be split to remove concatonated values
sensorColumns = ["rawData", "dataValue", "dataType", "plotValues", "plotLabels"]
# Split the dataframe to move concatonated values to new rows
print('Splitting DataFrame')
splitDf = split_dataframe_rows(oldData, sensorColumns, delimeters)


# Use the Pandas 'loc' function to find and replace pending changes in the dataset
print('Converting Pending Changes')
splitDf.loc[(splitDf.pendingChange == 'False'), 'pendingChange'] = 0
splitDf.loc[(splitDf.pendingChange == 'True'), 'pendingChange'] = 1

#print(splitDf)

# Pass the loaded dataframe into a function that will filter out any networks that don't involve MOVE
filteredDF = filterNetwork(splitDf)
# Sort the dataframe by sensorID and remove index names
filteredDF = sortSensors(filteredDF)

#print(filteredDF)

# Split the dataframe into separate dataframes by sensorID
for i, x in filteredDF.groupby('sensorID'):
	# Export the un-pivoted data
	with pd.ExcelWriter(XLSX_DIR + 'sensorData.xlsx', engine="openpyxl") as writer: # pylint: disable=abstract-class-instantiated
		# Export the data to a single XLSX file with a worksheet per sensor 
		x.to_excel(writer, sheet_name = str(i))
	
	# Convert the dataframe to a pivot table 
	processedDF = pivotTable(x, pivotValues, pivotIndex, pivotColumns, np.sum)

	# Export the processed data
	toXLSX(processedDF, i, XLSX_NAME)
	#toCSV(processedDF, i)

# Close DB connection
conn.close()
