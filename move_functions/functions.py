"""

"""


__author__ = ""
__version__ = ""


import os
import json
from uuid import UUID
from flask import g
import pandas as pd


# Function to save the recieved JSON file to disk
def jsonDump(name, struct, dir = os.getcwd() + "\\"):
	print('JSON dump')
	# Open a file for writing, filename will always be unique so append functions uneccessary
	with open(dir + name, 'w') as f:
		# Save the JSON to a JSON file on disk
		json.dump(struct, f)


# Function to save the processed data to a CSV
def csvDump(fileName, struct, index_set = False, index_label_usr = False, dir = os.getcwd() + "\\"):
	if os.path.exists(dir + fileName + '.csv'):
		print('CSV Append')
		with open(dir + fileName + '.csv', 'a', encoding="utf-8", newline="") as fd:
			struct.to_csv(fd, header=False, index=index_set)
	else:
		print('CSV Create')
		struct.to_csv(dir + fileName + '.csv', header=True, index=index_set, index_label = index_label_usr)


# Convert returned strings from the DB into GUID
def strToUUID(struct):
	# Remove the leading and trailing characters from the ID
	struct = struct.replace("[('", "")
	struct = struct.replace("', )]", "")
	# Convert trimmed string into a GUID (UUID)
	g.strUUID =  UUID(struct)

	# Return to calling function
	return g.strUUID


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


# Split the data by sensor ID and export the data to separate CSV 
# files and an XLSX file with separate worksheets per sensor
def sortSensors(df, col):
	print('Sorting and cleaning sensors')
	# Sort the values in the dataframe by their sensor ID
	df.sort_values(by = [col], inplace = True)
	# Set the DF index to the sensor IDs
	df.set_index(keys = [col], drop = False, inplace = True)
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
def toXLSX(df, pName, fileName):
	print('Exporting Sensor data to XLSX...')

	# Strip any invalid characters from the sheet name
	''.join(e for e in str(pName) if e.isalnum())
	
	# Check if file already exists, if it does then append otherwise create it
	if os.path.exists(fileName):
		print('XLSX file exists, appending...')
		with pd.ExcelWriter(fileName, engine="openpyxl", mode='a') as writer: # pylint: disable=abstract-class-instantiated
			# Export the data to a single XLSX file with a worksheet per sensor
			df.to_excel(writer, sheet_name = str(pName))
	else:
		print('File does not exist, creating...')
		with pd.ExcelWriter(fileName, engine="openpyxl") as writer: # pylint: disable=abstract-class-instantiated
			# Export the data to a single XLSX file with a worksheet per sensor 
			df.to_excel(writer, sheet_name = str(pName))


# Export the data to separate CSV files
def toCSV(df, sensorID, dir = os.getcwd() + "\\"):
	print('Exporting Sensor data to CSV')
	p = os.path.join(dir, "sensor_{}.csv".format(sensorID))
	df.to_csv(p, index=False)