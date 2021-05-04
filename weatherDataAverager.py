# Testing script for experimenting with resampling and merging weather station data.

import os
import pandas as pd

CSV_DIR = os.getcwd() + "/data/csv/"


def resample(struct, interval = "60min"):
	struct = struct.resample(interval).mean()
	struct = struct.reindex(pd.date_range(struct.index.min(), struct.index.max(), freq=interval))
	return struct

Humidity = pd.read_csv(CSV_DIR + "Humidity.csv", index_col="Datetime", parse_dates=True, low_memory=False)
Pressure = pd.read_csv(CSV_DIR + "Pressure.csv", index_col="Datetime", parse_dates=True, low_memory=False)
Rainfall = pd.read_csv(CSV_DIR + "Rainfall.csv", index_col="Datetime", parse_dates=True, low_memory=False)
SolarOutput = pd.read_csv(CSV_DIR + "SolarOutput.csv", index_col="Datetime", parse_dates=True, low_memory=False)
Temperature = pd.read_csv(CSV_DIR + "Temperature.csv", index_col="Datetime", parse_dates=True, low_memory=False)
WindDirection = pd.read_csv(CSV_DIR + "WindDirection.csv", index_col="Datetime", parse_dates=True, low_memory=False)
Windspeed = pd.read_csv(CSV_DIR + "Windspeed.csv", index_col="Datetime", parse_dates=True, low_memory=False)

data = Humidity.join(Pressure, on = "Datetime", how = "outer")
data = data.join(Rainfall, on = "Datetime", how = "outer")
data = data.join(SolarOutput, on = "Datetime", how = "outer")
data = data.join(Temperature, on = "Datetime", how = "outer")
data = data.join(WindDirection, on = "Datetime", how = "outer")
data = data.join(Windspeed, on = "Datetime", how = "outer")

data.to_csv(CSV_DIR + "/Joined/" + "Data.csv", index=True, index_label="DateTime")