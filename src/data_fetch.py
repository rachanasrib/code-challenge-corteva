from datetime import datetime
import numpy as np
import pandas as pd
import os
import glob


# Method to get weather station data and logs data from the input files respectively
def get_weather_station_data(filepath):
    # Column names list to be read from input files
    cols_wx = ['record_date', 'max_temp', 'min_temp', 'precipitation', 'weather_station']
    logs = []
    start_time = datetime.now()
    df_list = []
    # Iterate over each file and process the file data and create a dataframe for each file. Each dataframe is pushed into the df_list list
    for file in glob.glob(os.path.join(filepath, "*.txt")):
        file_name = os.path.splitext(os.path.basename(file))[0]
        data = pd.read_csv(file, header=None, names=cols_wx, delimiter='\t', index_col=False)
        data['weather_station'] = file_name
        # Replace -9999 with null values in the dataframe and convert the date from yyyymmdd string format to postgresql date format
        for i, row in data.iterrows():
            if row['max_temp'] == -9999:
                data.loc[i, 'max_temp'] = np.nan
            if row['min_temp'] == -9999:
                data.loc[i, 'min_temp'] = np.nan
            if row['precipitation'] == -9999:
                data.loc[i, 'precipitation'] = np.nan
            date_val = str(row['record_date'])
            date_obj = datetime(year=int(date_val[0:4]), month=int(date_val[4:6]), day=int(date_val[6:8]))
            data.at[i, 'record_date'] = date_obj

        df_list.append(data)
        # Append log data into logs list
        logs.append([start_time, datetime.now(), data.shape[0], file_name])
        start_time = datetime.now()
    # Create weather station data dataframe from list of dataframes
    weather_station_data = pd.concat(df_list)
    # Create weather station logs data dataframe
    weather_station_logs = pd.DataFrame(logs, columns=['start_time', 'end_time', 'records', 'weather_station'])
    return weather_station_data, weather_station_logs


# Method to get yield data from the input files respectively
def get_yield_data(filepath):
    # Column names list to be read from input files
    cols_yld = ['record_year', 'total_yield']
    df_list = []
    # Iterate over all the files and create a dataframe from each file content. Push each dataframe into df_list list
    for file in glob.glob(os.path.join(filepath, "*.txt")):
        data = pd.read_csv(file, header=None, names=cols_yld, delimiter='\t', index_col=False)
        df_list.append(data)
    # Create yield data dataframe from list of dataframes
    yield_data = pd.concat(df_list)
    return yield_data
