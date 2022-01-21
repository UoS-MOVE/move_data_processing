from distutils.dep_util import newer
import os
import requests
import pandas as pd
from pandas import json_normalize
from decouple import config


CSV_DIR = os.getcwd() + '/data/csv/'


# Function to save the processed data to a CSV
def csvDump(fileName, struct, index_set = False, index_label_usr = False):
	if os.path.exists(CSV_DIR + fileName + '.csv'):
		print('CSV Append')
		with open(CSV_DIR + fileName + '.csv', 'a', encoding="utf-8", newline="") as fd:
			struct.to_csv(fd, header=False, index=index_set)
	else:
		print('CSV Create')
		struct.to_csv(CSV_DIR + fileName + '.csv', header=True, index=index_set, index_label = index_label_usr)



URL_BASE = config('MONNIT_API_URL')
URL_ENDPOINT = config('MONNIT_API_URL_ENDPOINT')
API_KEY = config('MONNIT_API_ID')
API_SECRET = config('MONNIT_API_SECRET')
NETWORK_ID = config('MONNIT_NETWORK_ID')


# Load lists from env variables
SENSOR_TYPES = config('MONNIT_SENSOR_TYPES', cast=lambda v: [s.strip() for s in v.split(',')])
SENSOR_COLUMNS = config('MONNIT_SENSOR_COLUMNS', cast=lambda v: [s.strip() for s in v.split(',')])
DELIMETERS = config('MONNIT_DELIMETERS', cast=lambda v: [s.strip() for s in v.split('*')])




older_data = pd.read_csv(CSV_DIR + "dataRange_2019-09-03_2020-11-19.csv")
newer_data = pd.read_csv(CSV_DIR + "dataRange_2019-09-03_2022-01-13.csv")

sensor_data = pd.concat([older_data, newer_data], ignore_index=True)
sensor_data.drop_duplicates(subset=['dataMessageGUID'], inplace=True)


FULL_URL = "{0}{1}?NetworkID={2}".format(URL_BASE, "SensorList", NETWORK_ID)
response = requests.get(FULL_URL, headers={"APIKeyID":API_KEY, "APISecretKey":API_SECRET})
if response.status_code == 200:
	print('Got Sensor Names')
	# Store the recieved JSON file from the request 
	json_load = response.json()
	sensor_names_json = json_load['Result']
	# Convert the JSONs into pandas dataframes
	sensor_names_raw = json_normalize(sensor_names_json)

	sensor_names_raw.rename(columns={"SensorID": "sensorID", "ApplicationID": "applicationID", "SensorName": "sensorName", "AccountID": "accountID"}, inplace = True)
	#cols = sensor_names_raw[["sensorID", "applicationID", "sensorName", "accountID"]]
	sensor_names = sensor_names_raw
	
	sensor_names.drop(["CSNetID", "LastCommunicationDate", "NextCommunicationDate", "LastDataMessageMessageGUID", 
						"PowerSourceID", "Status", "CanUpdate", "CurrentReading", "BatteryLevel", "SignalStrength", 
						"AlertsActive", "CheckDigit"], axis = 1, inplace = True)


# Get gateway details for network id
FULL_URL = "{0}{1}".format(URL_BASE, "GatewayList")
response = requests.get(FULL_URL, headers={"APIKeyID":API_KEY, "APISecretKey":API_SECRET})
if response.status_code == 200:
	print('Got Gateway Data')
	# Store the recieved JSON file from the request 
	json_load = response.json()
	gateways_json = json_load['Result']
	# Convert the JSONs into pandas dataframes
	gateways_raw = json_normalize(gateways_json)

	gateways_raw.rename(columns={"GatewayID": "gatewayID", "NetworkID": "networkID", "Name": "gatewayName", 
								"GatewayType": "gatewayType", "Heartbeat": "heartbeat", "IsDirty": "isDirty", 
								"LastCommunicationDate": "lastCommunicationDate", "LastInboundIPAddress": "lastInboundIPAddress", 
								"MacAddress": "macAddress", "IsUnlocked": "isUnlocked", "CheckDigit": "checkDigit", 
								"AccountID": "accountID", "SignalStrength": "signalStrength", "BatteryLevel": "batteryLevel"}, inplace = True)
	gateways = gateways_raw

	gateways.drop(["gatewayType", "heartbeat", "isDirty", "lastCommunicationDate", 
					"lastInboundIPAddress", "macAddress", "isUnlocked", "checkDigit", 
					"accountID", "signalStrength", "batteryLevel"], axis = 1, inplace = True)


if sensor_names.empty == False:
	sensor_data = sensor_data.merge(sensor_names, on = "sensorID", how = "left")
if gateways.empty == False:
	sensor_data = sensor_data.merge(gateways, on = "gatewayID", how = "left")

sensor_data.sort_values(by="messageDate", inplace=True)

if sensor_data.empty == False:
	csvDump("iMonnit_Complete", sensor_data)