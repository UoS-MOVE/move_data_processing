# Title: Sensor Reading Output 
# Description: Pulls sensor readings data from the databases 
#				and outputs to a CSV file for analysis
# Author: Ethan Bellmer
# Date: 30/03/2021


# Import libraries
#import sys
import pandas as pd
import numpy as np
import os
#import traceback
import datetime
import pyodbc
#import re

from app import dbConnect



#sql = "{CALL [dbo].[PROC_GET_ALL_DATA]}"

sql = "CALL [dbo].[PROC_GET_ALL_DATA], @OUT = @out OUTPUT;"

# CONNECT TO DB HERE
conn = dbConnect()
cursor = conn.cursor()

try:
	# Execute the SQL statement with the parameters prepared
	cursor.execute(sql)
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

dataDF = pd.DataFrame()

rows = cursor.fetchall()
while rows:
	print(rows) # import to DF here
	if (dataDF.empty):
		dataDF = rows
	else:
		dataDF = dataDF.append(rows)

	if cursor.nextset():
		rows = cursor.fetchall()
	else:
		rows = None

conn.close()