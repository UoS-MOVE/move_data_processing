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
from app import dbConnect, split_dataframe_rows


## Variables ##

#SQL Server connection info
with open("./config/.dbCreds.json") as f:
	dbCreds = json.load(f)

CSV_DIR = os.getcwd() + '/data/csv/'

# Formatted connection string for the SQL DB.
SQL_CONN_STR = 'DSN='+dbCreds['SERVER']+';Database='+dbCreds['DATABASE']+';Trusted_Connection=no;UID='+dbCreds['UNAME']+';PWD='+dbCreds['PWD']+';'


## Functions ##

# Iterates through list of returned strings from the DB into and converts them to GUID
def strToUUID(struct):
	for element in struct:
		# Remove the leading and trailing characters from the ID
		element = struct.replace("[('", "")
		element = struct.replace("', )]", "")
		# Convert trimmed string into a GUID (UUID)
		element = UUID(element)
		
	# Return the list of UUIDs
	return struct


## Main Body ##

# Create a new DB object
conn = dbConnect()
# Create a new cursor from established DB connection
cursor = conn.cursor()

# Select all the data stored in the old DB form
SQL = "SELECT * FROM salfordMove.dbo.sensorData"

print('Getting data from DB')
oldData = pd.read_sql(SQL,conn)
#oldData = oldData.head()
#print(oldData)

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


# Check if CSV exists, create if it doesn't
if os.path.exists(CSV_DIR + 'sensorDataNormalised.csv'):
	splitDf.to_csv('sensorDataNormalised.csv', mode = 'a', header = False)
else:
	splitDf.to_csv('sensorDataNormalised.csv', mode = 'w', header = True)

# Commit data to DB and close connection
conn.commit()
conn.close()
