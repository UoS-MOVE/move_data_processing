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
ACTION_IDENT_PRE = "E589656878094D03A1554197DC90B5B5" # Pressure endpoint GUID
ACTION_IDENT_RF_MM = "90828B8769E74A5B9F74761335CB1676" # Rainfall in mm endpoint GUID
ACTION_IDENT_WS_MS = "B04BE963E74F467A875C534B90BE05A0" # Windspeed in ms endpoint GUID
ACTION_IDENT_WD_D = "752FC7FCFE584FBF980E2FFCAD991D87" # Wind direction endpoint GUID
ACTION_IDENT_SOL_KWM2 = "4EF9B920C87444939DE8069D37ECA200" # Solar Radiation endpoint GUID


START = "2021-02-01T00:00:00"
END = "2021-04-19T23:59:59"

dropCols = ['RECID','Limit','DeviceGUID','ActionGUID','PollType','RV']

# POST credentials
with open("./config/.onCallAPI.json") as f:
	accessToken = json.load(f)
API_KEY = accessToken['TOKEN']

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"	# Date format for parsing datetime returned by OnCall API

sd = pd.DataFrame()
sdH = pd.DataFrame()
sdP = pd.DataFrame()
sdRF = pd.DataFrame()
sdWS = pd.DataFrame()
sdWSD = pd.DataFrame()
sdS = pd.DataFrame()

# Function declaration
def RESAMPLE_DATA(df, RESAMPLE_PERIOD = '60min'):
	#df.index.names = ['Datetime']
	df = df.resample(RESAMPLE_PERIOD).mean()	# Resample data to an average over a defined period
	df = df.reindex(pd.date_range(df.index.min(), df.index.max(), freq=RESAMPLE_PERIOD))
	return df

def RENAME_COLUMNS(df, valName, inplaceB = True):
	df.rename(columns={"PollTimeStamp": "Datetime", "VarValue": valName}, inplace = inplaceB)	# Rename used columns to more appropriate names
	return df


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

	if not (sensorData.empty):
		print('Valid content returned')
		sensorData = RENAME_COLUMNS(sensorData, "Temperature in C")	# Rename used columns to more appropriate names
		sensorData.drop(dropCols, axis=1, inplace=True)	# Drop irrelevant variables
		sensorData['Datetime'] = pd.to_datetime(sensorData['Datetime'])
		sensorData = sensorData.set_index('Datetime')
		csvDump("Temperature", RESAMPLE_DATA(sensorData), index_set = True, index_label_usr = "Datetime")
	else:
		print("Temperature Response Empty, skipping...")


	# Fetch Humidity data
	REQUEST_URL = URL + ENDPOINT + "/" + DEVICE + "/" + ACTION_IDENT_HUM + "?start=" + stepStart + "&end=" + stepEnd + "&api_key=" + API_KEY	# API URl for humidity data
	response = requests.get(REQUEST_URL)
	print("Humidity Endpoint Status " + str(response.status_code))
	if (response.status_code != 200): break	# Break the loop is the returned status code is not HTTP 200
	
	jsonLoad = response.json()	# Load the recieved JSON file from the request
	sensorDataHum = json_normalize(jsonLoad)	# Convert the JSONs into pandas dataframes

	if not (sensorDataHum.empty):
		sensorDataHum = RENAME_COLUMNS(sensorDataHum, "Humidity in %")	# Rename used columns to more appropriate names
		sensorDataHum.drop(dropCols, axis=1, inplace=True)	# Drop irrelevant variables
		sensorDataHum['Datetime'] = pd.to_datetime(sensorDataHum['Datetime'])
		sensorDataHum = sensorDataHum.set_index('Datetime')
		csvDump("Humidity", RESAMPLE_DATA(sensorDataHum), index_set = True, index_label_usr = "Datetime")
	else:
		print("Humidity Response Empty, skipping...")


	# Fetch Pressure data
	REQUEST_URL = URL + ENDPOINT + "/" + DEVICE + "/" + ACTION_IDENT_PRE + "?start=" + stepStart + "&end=" + stepEnd + "&api_key=" + API_KEY	# API URl for humidity data
	response = requests.get(REQUEST_URL)
	print("Pressure Endpoint Status " + str(response.status_code))
	if (response.status_code != 200): break	# Break the loop is the returned status code is not HTTP 200
	
	jsonLoad = response.json()	# Load the recieved JSON file from the request
	sensorDataPre = json_normalize(jsonLoad)	# Convert the JSONs into pandas dataframes

	if not (sensorDataPre.empty):
		sensorDataPre = RENAME_COLUMNS(sensorDataPre, "Pressure in mBar")	# Rename used columns to more appropriate names
		sensorDataPre.drop(dropCols, axis=1, inplace=True)	# Drop irrelevant variables
		sensorDataPre['Datetime'] = pd.to_datetime(sensorDataPre['Datetime'])
		sensorDataPre = sensorDataPre.set_index('Datetime')
		csvDump("Pressure", RESAMPLE_DATA(sensorDataPre), index_set = True, index_label_usr = "Datetime")
	else:
		print("Pressure Response Empty, skipping...")


	# Fetch Rainfall mm data
	REQUEST_URL = URL + ENDPOINT + "/" + DEVICE + "/" + ACTION_IDENT_RF_MM + "?start=" + stepStart + "&end=" + stepEnd + "&api_key=" + API_KEY	# API URl for humidity data
	response = requests.get(REQUEST_URL)
	print("Rainfall Endpoint Status " + str(response.status_code))
	if (response.status_code != 200): break	# Break the loop is the retur                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           bned status code is not HTTP 200
	
	jsonLoad = response.json()	# Load the recieved JSON file from the request
	sensorDataRF = json_normalize(jsonLoad)	# Convert the JSONs into pandas dataframes

	if not (sensorDataRF.empty):
		sensorDataRF = RENAME_COLUMNS(sensorDataRF, "Rainfall in mm")	# Rename used columns to more appropriate names
		sensorDataRF.drop(dropCols, axis=1, inplace=True)	# Drop irrelevant variables
		sensorDataRF['Datetime'] = pd.to_datetime(sensorDataRF['Datetime'])
		sensorDataRF = sensorDataRF.set_index('Datetime')
		csvDump("Rainfall", RESAMPLE_DATA(sensorDataRF), index_set = True, index_label_usr = "Datetime")
	else:
		print("Rainfall Response Empty, skipping...")


	# Fetch Windspeed m/s data
	REQUEST_URL = URL + ENDPOINT + "/" + DEVICE + "/" + ACTION_IDENT_WS_MS + "?start=" + stepStart + "&end=" + stepEnd + "&api_key=" + API_KEY	# API URl for humidity data
	response = requests.get(REQUEST_URL)
	print("Windspeed Endpoint Status " + str(response.status_code))
	if (response.status_code != 200): break	# Break the loop is the returned status code is not HTTP 200
	jsonLoad = response.json()	# Load the recieved JSON file from the request
	sensorDataWS = json_normalize(jsonLoad)	# Convert the JSONs into pandas dataframes

	if not (sensorDataWS.empty):
		sensorDataWS = RENAME_COLUMNS(sensorDataWS, "Windspeed in ms")	# Rename used columns to more appropriate names
		sensorDataWS.drop(dropCols, axis=1, inplace=True)	# Drop irrelevant variables
		sensorDataWS['Datetime'] = pd.to_datetime(sensorDataWS['Datetime'])
		sensorDataWS = sensorDataWS.set_index('Datetime')
		csvDump("Windspeed", RESAMPLE_DATA(sensorDataWS), index_set = True, index_label_usr = "Datetime")
	else:
		print("Windspeed Response Empty, skipping...")


	# Fetch Windspeed direction in degrees data
	REQUEST_URL = URL + ENDPOINT + "/" + DEVICE + "/" + ACTION_IDENT_WD_D + "?start=" + stepStart + "&end=" + stepEnd + "&api_key=" + API_KEY	# API URl for humidity data
	response = requests.get(REQUEST_URL)
	print("Wind direction Endpoint Status " + str(response.status_code))
	if (response.status_code != 200): break	# Break the loop is the returned status code is not HTTP 200
	
	jsonLoad = response.json()	# Load the recieved JSON file from the request
	sensorDataWSD = json_normalize(jsonLoad)	# Convert the JSONs into pandas dataframes

	if not (sensorDataWSD.empty):
		sensorDataWSD = RENAME_COLUMNS(sensorDataWSD, "Wind direction in deg")	# Rename used columns to more appropriate names
		sensorDataWSD.drop(dropCols, axis=1, inplace=True)	# Drop irrelevant variables
		sensorDataWSD['Datetime'] = pd.to_datetime(sensorDataWSD['Datetime'])
		sensorDataWSD = sensorDataWSD.set_index('Datetime')
		csvDump("WindDirection", RESAMPLE_DATA(sensorDataWSD), index_set = True, index_label_usr = "Datetime")
	else:
		print("Wind Direction Response Empty, skipping...")


	# Fetch Solar output data
	REQUEST_URL = URL + ENDPOINT + "/" + DEVICE + "/" + ACTION_IDENT_SOL_KWM2 + "?start=" + stepStart + "&end=" + stepEnd + "&api_key=" + API_KEY	# API URl for humidity data
	response = requests.get(REQUEST_URL)
	print("Solar output Endpoint Status " + str(response.status_code))
	if (response.status_code != 200): break	# Break the loop is the returned status code is not HTTP 200
	
	jsonLoad = response.json()	# Load the recieved JSON file from the request
	sensorDataSOL = json_normalize(jsonLoad)	# Convert the JSONs into pandas dataframes

	if not (sensorDataSOL.empty):
		sensorDataSOL = RENAME_COLUMNS(sensorDataSOL, "Solar output in kW/m2")	# Rename used columns to more appropriate names
		sensorDataSOL.drop(dropCols, axis=1, inplace=True)	# Drop irrelevant variables
		sensorDataSOL['Datetime'] = pd.to_datetime(sensorDataSOL['Datetime'])
		sensorDataSOL = sensorDataSOL.set_index('Datetime')
		csvDump("SolarOutput", RESAMPLE_DATA(sensorDataSOL), index_set = True, index_label_usr = "Datetime")
	else:
		print("Solar Response Empty, skipping...")

	#	Maintain separate dataframes for the aggregated weather station data	
	##	This is a workaround to prevent significant gaps from ocurring in the data 
	## 		when resampling and joining the dataframes differences in the recorded timestamps 
	## 		causes gaps to appear in the data even when resampling to 1 hour gaps.
	if (sd.empty):
		sd = sensorData
	else:
		sd = sd.append(sensorData)
	
	if (sdH.empty):
		sdH = sensorDataHum
	else:
		sdH = sdH.append(sensorDataHum)
	
	if (sdP.empty):
		sdP = sensorDataPre
	else:
		sdP = sdP.append(sensorDataPre)
	
	if (sdRF.empty):
		sdRF = sensorDataRF
	else:
		sdRF = sdRF.append(sensorDataRF)
	
	if (sdWS.empty):
		sdWS = sensorDataWS
	else:
		sdWS = sdWS.append(sensorDataWS)
	
	if (sdWSD.empty):
		sdWSD = sensorDataWSD
	else:
		sdWSD = sdWSD.append(sensorDataWSD)

	if (sdS.empty):
		sdS = sensorDataSOL
	else:
		sdS = sdS.append(sensorDataSOL)


#	Resample data to 60 minute intervals
##	Resampling data after all available data has been aggregated revents gaps
sd = RESAMPLE_DATA(sd)
sdH = RESAMPLE_DATA(sdH)
sdP = RESAMPLE_DATA(sdP)
sdRF = RESAMPLE_DATA(sdRF)
sdWS = RESAMPLE_DATA(sdWS)
sdWSD = RESAMPLE_DATA(sdWSD)
sdS = RESAMPLE_DATA(sdS)

# Join the dataframes
sensorDataFinal = sd.join(sdH, how = "outer")
sensorDataFinal = sensorDataFinal.join(sdP, how = "outer")
sensorDataFinal = sensorDataFinal.join(sdRF, how = "outer")
sensorDataFinal = sensorDataFinal.join(sdWS, how = "outer")
sensorDataFinal = sensorDataFinal.join(sdWSD, how = "outer")
sensorDataFinal = sensorDataFinal.join(sdS, how = "outer")

csvDump("weatherDataRange_" + str(START.replace(":", "-")) + '_' + str(END.replace(":", "-")), sensorDataFinal, index_set = True, index_label_usr = "Datetime")