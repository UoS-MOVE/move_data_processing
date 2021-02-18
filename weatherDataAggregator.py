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
ACTION_IDENT_PRE = "E589656878094D03A1554197DC90B5B5" # 
ACTION_IDENT_RF_MM = "90828B8769E74A5B9F74761335CB1676" #
ACTION_IDENT_RF_MM_1HR = "627EB89627FA4B98880411B3F1CB47BC" #
ACTION_IDENT_WS_MS = "B04BE963E74F467A875C534B90BE05A0" #
ACTION_IDENT_WD_D = "752FC7FCFE584FBF980E2FFCAD991D87" #
ACTION_IDENT_SOL_KWM2 = "4EF9B920C87444939DE8069D37ECA200" #


START = "2019-09-01T00:00:00"
END = "2021-02-16T23:59:59"

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
	print("Temperature Endpoint Status " + str(response.status_code))
	if (response.status_code != 200): break	# Break the loop is the returned status code is not HTTP 200

	jsonLoad = response.json()	# Load the recieved JSON file from the request 
	sensorData = json_normalize(jsonLoad)	# Convert the JSONs into pandas dataframes

	if (sensorData.empty):
		print('Valid content returned')
		sensorData.rename(columns={"PollTimeStamp": "Datetime", "VarValue": "Temperature in C"}, inplace=True)	# Rename used columns to more appropriate names


		# Fetch Humidity data
		REQUEST_URL = URL + ENDPOINT + "/" + DEVICE + "/" + ACTION_IDENT_HUM + "?start=" + stepStart + "&end=" + stepEnd + "&api_key=" + API_KEY	# API URl for humidity data
		response = requests.get(REQUEST_URL)
		print("Humidity Endpoint Status " + str(response.status_code))
		if (response.status_code != 200): break	# Break the loop is the returned status code is not HTTP 200
		
		jsonLoad = response.json()	# Load the recieved JSON file from the request
		sensorDataHum = json_normalize(jsonLoad)	# Convert the JSONs into pandas dataframes
		sensorDataHum.rename(columns={"PollTimeStamp": "Datetime", "VarValue": "Humidity in %"}, inplace=True)	# Rename used columns to more appropriate names


		# Fetch Pressure data
		REQUEST_URL = URL + ENDPOINT + "/" + DEVICE + "/" + ACTION_IDENT_PRE + "?start=" + stepStart + "&end=" + stepEnd + "&api_key=" + API_KEY	# API URl for humidity data
		response = requests.get(REQUEST_URL)
		print("Pressure Endpoint Status " + str(response.status_code))
		if (response.status_code != 200): break	# Break the loop is the returned status code is not HTTP 200
		
		jsonLoad = response.json()	# Load the recieved JSON file from the request
		sensorDataPre = json_normalize(jsonLoad)	# Convert the JSONs into pandas dataframes
		sensorDataPre.rename(columns={"PollTimeStamp": "Datetime", "VarValue": "Pressure in mBar"}, inplace=True)	# Rename used columns to more appropriate names


		# Fetch Rainfall mm data
		REQUEST_URL = URL + ENDPOINT + "/" + DEVICE + "/" + ACTION_IDENT_RF_MM + "?start=" + stepStart + "&end=" + stepEnd + "&api_key=" + API_KEY	# API URl for humidity data
		response = requests.get(REQUEST_URL)
		print("Rainfall Endpoint Status " + str(response.status_code))
		if (response.status_code != 200): break	# Break the loop is the retur                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           bned status code is not HTTP 200
		
		jsonLoad = response.json()	# Load the recieved JSON file from the request
		sensorDataRF = json_normalize(jsonLoad)	# Convert the JSONs into pandas dataframes
		sensorDataRF.rename(columns={"PollTimeStamp": "Datetime", "VarValue": "Rainfall in mm"}, inplace=True)	# Rename used columns to more appropriate names


		# Fetch Raindall mm /1hr data
		#REQUEST_URL = URL + ENDPOINT + "/" + DEVICE + "/" + ACTION_IDENT_RF_MM_1HR + "?start=" + stepStart + "&end=" + stepEnd + "&api_key=" + API_KEY	# API URl for humidity data
		#response = requests.get(REQUEST_URL)
		#print("Rainfall Endpoint Status " + str(response.status_code))
		#if (response.status_code != 200): break	# Break the loop is the returned status code is not HTTP 200
		
		#jsonLoad = response.json()	# Load the recieved JSON file from the request
		#sensorDataRF1HR = json_normalize(jsonLoad)	# Convert the JSONs into pandas dataframes
		#sensorDataRF1HR.rename(columns={"PollTimeStamp": "Datetime", "VarValue": "Rainfall in mm /1hr"}, inplace=True)	# Rename used columns to more appropriate names


		# Fetch Windspeed m/s data
		REQUEST_URL = URL + ENDPOINT + "/" + DEVICE + "/" + ACTION_IDENT_WS_MS + "?start=" + stepStart + "&end=" + stepEnd + "&api_key=" + API_KEY	# API URl for humidity data
		response = requests.get(REQUEST_URL)
		print("Windspeed Endpoint Status " + str(response.status_code))
		if (response.status_code != 200): break	# Break the loop is the returned status code is not HTTP 200
		jsonLoad = response.json()	# Load the recieved JSON file from the request
		sensorDataWS = json_normalize(jsonLoad)	# Convert the JSONs into pandas dataframes
		sensorDataWS.rename(columns={"PollTimeStamp": "Datetime", "VarValue": "Windspeed in ms"}, inplace=True)	# Rename used columns to more appropriate names


		# Fetch Windspeed direction in degrees data
		REQUEST_URL = URL + ENDPOINT + "/" + DEVICE + "/" + ACTION_IDENT_WD_D + "?start=" + stepStart + "&end=" + stepEnd + "&api_key=" + API_KEY	# API URl for humidity data
		response = requests.get(REQUEST_URL)
		print("Wind direction Endpoint Status " + str(response.status_code))
		if (response.status_code != 200): break	# Break the loop is the returned status code is not HTTP 200
		
		jsonLoad = response.json()	# Load the recieved JSON file from the request
		sensorDataWSD = json_normalize(jsonLoad)	# Convert the JSONs into pandas dataframes
		sensorDataWSD.rename(columns={"PollTimeStamp": "Datetime", "VarValue": "Wind direction in deg"}, inplace=True)	# Rename used columns to more appropriate names


		# Fetch Solar output data
		REQUEST_URL = URL + ENDPOINT + "/" + DEVICE + "/" + ACTION_IDENT_SOL_KWM2 + "?start=" + stepStart + "&end=" + stepEnd + "&api_key=" + API_KEY	# API URl for humidity data
		response = requests.get(REQUEST_URL)
		print("Solar output Endpoint Status " + str(response.status_code))
		if (response.status_code != 200): break	# Break the loop is the returned status code is not HTTP 200
		
		jsonLoad = response.json()	# Load the recieved JSON file from the request
		sensorDataSOL = json_normalize(jsonLoad)	# Convert the JSONs into pandas dataframes
		sensorDataSOL.rename(columns={"PollTimeStamp": "Datetime", "VarValue": "Solar output in kW/m2"}, inplace=True)	# Rename used columns to more appropriate names

		
		sensorData.drop(['RECID','Limit','DeviceGUID','ActionGUID','PollType','RV'], axis=1, inplace=True)	# Drop irrelevant variables
		sensorDataHum.drop(['RECID','Limit','DeviceGUID','ActionGUID','PollType','RV'], axis=1, inplace=True)	# Drop irrelevant variables
		sensorDataPre.drop(['RECID','Limit','DeviceGUID','ActionGUID','PollType','RV'], axis=1, inplace=True)	# Drop irrelevant variables
		sensorDataRF.drop(['RECID','Limit','DeviceGUID','ActionGUID','PollType','RV'], axis=1, inplace=True)	# Drop irrelevant variables
		#sensorDataRF1HR.drop(['RECID','Limit','DeviceGUID','ActionGUID','PollType','RV'], axis=1, inplace=True)	# Drop irrelevant variables
		sensorDataWS.drop(['RECID','Limit','DeviceGUID','ActionGUID','PollType','RV'], axis=1, inplace=True)	# Drop irrelevant variables
		sensorDataWSD.drop(['RECID','Limit','DeviceGUID','ActionGUID','PollType','RV'], axis=1, inplace=True)	# Drop irrelevant variables
		sensorDataSOL.drop(['RECID','Limit','DeviceGUID','ActionGUID','PollType','RV'], axis=1, inplace=True)	# Drop irrelevant variables

		sensorData['Datetime'] = pd.to_datetime(sensorData['Datetime'])
		sensorDataHum['Datetime'] = pd.to_datetime(sensorDataHum['Datetime'])
		sensorDataPre['Datetime'] = pd.to_datetime(sensorDataPre['Datetime'])
		sensorDataRF['Datetime'] = pd.to_datetime(sensorDataRF['Datetime'])
		#sensorDataRF1HR['Datetime'] = pd.to_datetime(sensorDataRF1HR['Datetime'])
		sensorDataWS['Datetime'] = pd.to_datetime(sensorDataWS['Datetime'])
		sensorDataWSD['Datetime'] = pd.to_datetime(sensorDataWSD['Datetime'])
		sensorDataSOL['Datetime'] = pd.to_datetime(sensorDataSOL['Datetime'])


		# Join fetched data & additional processing
		sensorData = sensorData.set_index('Datetime').join(sensorDataHum.set_index('Datetime'), on = 'Datetime')
		sensorData = sensorData.join(sensorDataPre.set_index('Datetime'), on = 'Datetime')
		sensorData = sensorData.join(sensorDataRF.set_index('Datetime'), on = 'Datetime')
		#sensorData = sensorData.join(sensorDataRF1HR.set_index('Datetime'), on = 'Datetime')
		sensorData = sensorData.join(sensorDataWS.set_index('Datetime'), on = 'Datetime')
		sensorData = sensorData.join(sensorDataWSD.set_index('Datetime'), on = 'Datetime')
		sensorData = sensorData.join(sensorDataSOL.set_index('Datetime'), on = 'Datetime')
		

		#sensorData.index.names = ['Datetime']
		sensorData = sensorData.resample('60min').mean()	# Resample data to a 15 minute average
		sensorData = sensorData.reindex(pd.date_range(sensorData.index.min(), sensorData.index.max(), freq="60min"))


		csvDump("weatherDataRange_" + str(START.replace(":", "-")) + '_' + str(END.replace(":", "-")), sensorData)
