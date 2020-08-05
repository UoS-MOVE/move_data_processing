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
import os
import traceback
import datetime

import pyodbc

from uuid import UUID

# Import the main webhook file so its functions can be used. 
import app
from app import dbConnect, split_dataframe_rows


## Variables ##

#SQL Server connection info
with open("./config/.dbCreds.json") as f:
	dbCreds = json.load(f)

CSV_DIR = os.getcwd() + '/data/csv/'
XLSX_DIR = os.getcwd() + '/data/xlsx/'

# Formatted connection string for the SQL DB.
SQL_CONN_STR = 'DSN='+dbCreds['SERVER']+';Database='+dbCreds['DATABASE']+';Trusted_Connection=no;UID='+dbCreds['UNAME']+';PWD='+dbCreds['PWD']+';'


## Functions ##
def filterNetwork(pdDF):
	print('Filtering out unused network data')
	pdDF = pdDF[splitDf.networkID == 58947]
	#pdDF = pdDF.drop(columns = ['Unnamed: 0'])

	return pdDF

def splitSensors(pdDF):
	#writer = pd.ExcelWriter("sensorData.xlsx", engine = 'xlsxwriter')

	print('Splitting data by sensor ID')

	print('Sorting Values')
	pdDF.sort_values(by = ['sensorID'], inplace = True)
	
	print('Setting Index')
	pdDF.set_index(keys = ['sensorID'], drop = False, inplace = True)

	print('Stripping Index Name')
	pdDF.index.name = None

	print('Exporting Sensor data to CSV & XLSX')
	with pd.ExcelWriter(XLSX_DIR + 'sensorData.xlsx') as writer: # pylint: disable=abstract-class-instantiated
		for i, x in pdDF.groupby('sensorID'):
			print('Processing Sensor ' + str(i))

			# Export the data to separate CSV files
			p = os.path.join(CSV_DIR, "sensor_{}.csv".format(i))
			x.to_csv(p, index=False)
			
			# Export the data to a single XLSX file with a worksheet per sensor 
			x.to_excel(writer, sheet_name = str(i))
		#writer.save


## Main Body ##

# Create a new DB object
conn = dbConnect()
# Create a new cursor from established DB connection
cursor = conn.cursor()

# Select all the data stored in the old DB form
SQL = "SELECT * FROM salfordMove.dbo.sensorData"

print('Getting data from DB')
oldData = pd.read_sql(SQL,conn)
#oldData = oldData.head(20) # Overrites the dataframe wiht the top 20 entires for quick testing

# Delimeters used in the recieved data
delimeters = "%2c","|"
# The columns that need to be split to remove concatonated values
sensorColumns = ["rawData", "dataValue", "dataType", "plotValues", "plotLabels"]
# Split the dataframe to move concatonated values to new rows
print('Splitting DataFrame')
splitDf = split_dataframe_rows(oldData, sensorColumns, delimeters)


# Iterate through the fetched data, and convert it to it's normalised form
for i, row in splitDf.iterrows():
	print("Processing sensor message " + str(i) + ".")

	# Convert pendingChange True/False value into a boolean 1/0
	if row['pendingChange'] == 'False':
		splitDf.at[i, 'pendingChange'] = 0
	else:
		splitDf.at[i, 'pendingChange'] = 1


# Pass the loaded dataframe into a function that will filter out any networks that don't involve MOVE
networkFiltered = filterNetwork(splitDf)

# Take the network filtered data and pass it into a functiuon that will split the data into separate sheets per sensor.
splitSensors(networkFiltered)

# Close DB connection
conn.close()
