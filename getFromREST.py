# Title: Get From REST API
# Description: Fetches data from the Monnit servers 
# 	using the RESTful API for a specified data range.
# Author: Ethan Bellmer
# Date: 18/11/2020
# Version: 0.1


# Import Libraries
import pandas as pd 
from pandas import json_normalize
from datetime import datetime, timedelta
import requests
import re
from app import csvDump
import json

# Variable Declarations
MONNIT_URL = "https://www.imonnit.com/json/"
MONNIT_ENDPOINT = "AccountDataMessages/"

#POST credentials info
with open("./config/.monnitAccessToken.json") as f:
	accessToken = json.load(f)
MONNIT_ACCESS_TOKEN = accessToken['TOKEN']
MONNIT_NETWORK_ID = "networkID=58947"
FROM_DATE = "2019/09/03"
TO_DATE = "2022/01/13"

DATE_FORMAT = "%Y/%m/%d"

# Main
start = datetime.strptime(FROM_DATE, DATE_FORMAT).date()
end = datetime.strptime(TO_DATE, DATE_FORMAT).date()


step = timedelta(days=1)

while start < end:
	stepStart = start.strftime(DATE_FORMAT)
	start += step
	stepEnd = start.strftime(DATE_FORMAT)

	print('Step Start ' + str(stepStart) + ' | Step End ' + str(stepEnd))

	REQUEST_URL = MONNIT_URL + MONNIT_ENDPOINT + MONNIT_ACCESS_TOKEN + '?' + MONNIT_NETWORK_ID + '&fromDate=' + stepStart + '&toDate=' + stepEnd

	response = requests.get(REQUEST_URL)
	print(response.status_code)
	
	# Store the recieved JSON file from the request 
	jsonLoad = response.json()
	
	sensorData = jsonLoad['Result']
	# Convert the JSONs into pandas dataframes
	sensorData = json_normalize(sensorData)
	
	sensorData.rename(columns={"MessageDate": "messageDate", "SensorID": "sensorID", "DataMessageGUID": "dataMessageGUID", "State": "state", 
		"SignalStrength": "signalStrength", "Voltage": "voltage", "Battery": "battery", "Data": "rawData", "DisplayData": "displayData", 
		"PlotValue": "plotValue", "MetNotificationRequirements": "metNotificationRequirements", "GatewayID": "gatewayID", "DataValues": "dataValue", 
		"DataTypes": "dataType", "PlotValues": "plotValues", "PlotLabels": "plotLabels"}, inplace=True)

	for i, data in sensorData.iterrows():
		TimestampUtc = re.split('\(|\)', data.messageDate)[1][:10]
		sensorData.messageDate.iat[i] = datetime.fromtimestamp(int(TimestampUtc))
	if sensorData.empty:
		print('Response Empty, Skipping...')
	else:
		csvDump("dataRange_" + str(FROM_DATE.replace("/", "-")) + '_' + str(TO_DATE.replace("/", "-")), sensorData)