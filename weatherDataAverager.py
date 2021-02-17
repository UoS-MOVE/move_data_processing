import os
import pandas as pd

CSV_DIR = os.getcwd() + "/data/csv/"

df = pd.read_csv(CSV_DIR + "weatherDataRange_2019-09-01T00-00-00_2020-12-13T23-59-59.csv", index_col="PollTimeStamp", parse_dates=True, low_memory=False)
df.drop(['RECID','Limit','DeviceGUID','ActionGUID','PollType','RV'], axis=1, inplace=True)
df.rename(columns={"PollTimeStamp": "Datetime", "VarValue": "Temperature in C"}, inplace=True)
df.index.names = ['Datetime']
df = df.resample('10min').mean()
df = df.reindex(pd.date_range(df.index.min(), df.index.max(), freq="10min"))
df.to_csv(CSV_DIR + "weatherDataReduced.csv", index=True, index_label="DateTime")