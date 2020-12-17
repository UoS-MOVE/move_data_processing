# Title: Weather Data Aggregator
# Description: Aggregates data from the weather station on Cockcroft from the OnCall API.
# Author: Ethan Bellmer
# Date: 17/12/2020
# Version: 1.0


# Import libraries
import pandas as pd
from pandas import json_normalize
import json
import requests
from datetime import datetime, timedelta
from app import csvDump
import os

# Variable Declarations
URL = "http://146.87.171.58/Panasense.OnCall.Finestra/api"
ENDPOINT = "/dailypollarchive"
DEVICE = "0FF00FFA2DBB4A029D2902CD33A43364"	# Cockcroft Weather Station GUID
ACTION_IDENT_TEMP = "AD7396F9F28D4DA798F0370934C368A9" # Air Tempertaure in C endpoint GUID
ACTION_IDENT_HUM = "8C5DAA6DB83E4E5C8310A27F6E549527" # Relative Humidity endpoint GUID

START = "2019-09-01T00:00:00"
END = "2020-12-13T23:59:59"

#POST credentials info
with open("./config/.onCallAPI.json") as f:
	accessToken = json.load(f)
API_KEY = accessToken['TOKEN']

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"	# Date format for parsing datetime returned by OnCall API


# Main body
start = datetime.strptime(START, DATE_FORMAT).date()
end = datetime.strptime(END, DATE_FORMAT).date()
step = timedelta(days=5)	# Sets the step gap for getting data in specific increments

while start < end:
	stepStart = start.strftime(DATE_FORMAT)
	start += step
	stepEnd = start.strftime(DATE_FORMAT)

	print('Step Start ' + str(stepStart) + ' | Step End ' + str(stepEnd))

	# Fetch Temperature data
	REQUEST_URL = URL + ENDPOINT + "/" + DEVICE + "/" + ACTION_IDENT_TEMP + "?start=" + stepStart + "&end=" + stepEnd + "&api_key=" + API_KEY	# API URL for temperature data
	response = requests.get(REQUEST_URL)
	print("Temperature Endpoint Status " + response.status_code)
	if (response.status_code != 200): break	# Break the loop is the returned status code is not HTTP 200

	jsonLoad = response.json()	# Load the recieved JSON file from the request 
	sensorData = json_normalize(jsonLoad)	# Convert the JSONs into pandas dataframes
	sensorData.rename(columns={"PollTimeStamp": "Datetime", "VarValue": "Temperature in C"}, inplace=True)	# Rename used columns to more appropriate names

	# Fetch Humidity data
	REQUEST_URL = URL + ENDPOINT + "/" + DEVICE + "/" + ACTION_IDENT_HUM + "?start=" + stepStart + "&end=" + stepEnd + "&api_key=" + API_KEY	# API URl for humidity data
	response = requests.get(REQUEST_URL)
	print("Humidity Endpoint Status " + response.status_code)
	if (response.status_code != 200): break	# Break the loop is the returned status code is not HTTP 200
	 
	jsonLoad = response.json()	# Load the recieved JSON file from the request
	sensorDataHum = json_normalize(jsonLoad)	# Convert the JSONs into pandas dataframes
	sensorDataHum.rename(columns={"PollTimeStamp": "Datetime", "VarValue": "Humidity in %"}, inplace=True)	# Rename used columns to more appropriate names

	# Join fetched data & additional processing
	sensorData = sensorData.set_index('Datetime').join(sensorDataHum.set_index('Datetime'))
	#sensorData.join(sensorDataHum, on='Datetime')	# Join the Temperature & Humidity dataframes

	#sensorData.set_index('Datetime', inplace=True)	# Set the dataframe index to the timestamp
	sensorData.drop(['RECID','Limit','DeviceGUID','ActionGUID','PollType','RV'], axis=1, inplace=True)	# Drop irrelevant variables
	#sensorData.index.names = ['Datetime']
	sensorData = sensorData.resample('15min').mean()	# Resample data to a 15 minute average
	sensorData = sensorData.reindex(pd.date_range(sensorData.index.min(), sensorData.index.max(), freq="15min"))


	csvDump("weatherDataRange_" + str(START.replace(":", "-")) + '_' + str(END.replace(":", "-")), sensorData)
