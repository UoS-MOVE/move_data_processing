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
#from flatten_json import flatten


app = Flask(__name__)


@app.route('/', methods=['POST'])

def webhook():
	print("webhook"); sys.stdout.flush()
	if request.method == 'POST':
		print('JSON Recieved')
		jsonLoad = request.json

        with open('sampleJSON.json', 'w') as f:
                json.dump(jsonLoad, f)

		gatewayMessages = jsonLoad['gatewayMessage']
		sensorMessages = jsonLoad['sensorMessages']

		gatewayMessages = json_normalize(gatewayMessages)
		sensorMessages = json_normalize(sensorMessages)

		if os.path.exists('sensorCSV.csv'):
			with open('sensorCSV.csv', 'a') as fd:
				sensorMessages.to_csv(fd, header=False, index=False)
		else:
			sensorMessages.to_csv('sensorCSV.csv', index=False)


		if os.path.exists('gatewayCSV.csv'):
			with open('gatewayCSV.csv', 'a') as fd:
				gatewayMessages.to_csv(fd, header=False, index=False)
		else:
			gatewayMessages.to_csv('gatewayCSV.csv', index=False)

		return '', 200
	else:
		abort(400)


if __name__ == '__main__':
	app.run(host= '0.0.0.0', port = '80')

