# Title: Webhook Listener (MOVE)
# Description: A webhook listener for the data collected by the 
#              MOVE sensors in the Salford Museum and Gallery
# Author: Ethan Bellmer
# Date: 29/08/2019
# Version: 0.1

# Import libraries
import sys
from flask import Flask, request, abort
import json
import pandas as pd
import os
import traceback
from pandas.io.json import json_normalize
from flatten_json import flatten

testJson = "{'gatewayMessage': {'gatewayID': '929837', 'gatewayName': 'Joule House Test Gateway - 929837', 'accountID': '27825', 'networkID': '58894', 'messageType': '0', 'power': '0', 'batteryLevel': '101', 'date': '2019-09-08 09:37:49', 'count': '46', 'signalStrength': '0', 'pendingChange': 'False'}, 'sensorMessages': [{'sensorID': '483526', 'sensorName': 'Humidity - 483526', 'applicationID': '43', 'networkID': '58894', 'dataMessageGUID': '765070b5-7709-4ffb-8a25-0c60f46ad996', 'state': '0', 'messageDate': '2019-09-08 09:35:52', 'rawData': '62.22%2c15.85', 'dataType': 'Percentage|TemperatureData', 'dataValue': '62.22|15.85', 'plotValues': '62.22|15.85', 'plotLabels': 'Humidity|Celsius', 'batteryLevel': '100', 'signalStrength': '100', 'pendingChange': 'True', 'voltage': '2.84'}]}"
testJson = testJson.replace("\'", "\"")

json = json.loads(testJson)
#df = pd.read_json(json)
#df = json_normalize(json)
df = flatten(json)
df = json_normalize(df)
df.columns = df.columns.str.replace("_0", "")
#df = df.replace("_0","")

if os.path.exists('testCSV.csv'):
	with open('testCSV.csv', 'a') as fd:
		df.to_csv(fd, header=False)
	#df.to_csv('testCSV.csv')
else:
	df.to_csv('testCSV.csv')

