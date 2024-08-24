import openmeteo_requests
import requests_cache
from retry_requests import retry

import datetime
import pandas as pd
import numpy as np
import time


# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)


# Define the start and end years
start_year = 1950
end_year = 2023

# Define the months to include
start_month = 3  # March
end_month = 8    # August


# Function to generate date ranges
def generate_date_ranges(start_date, end_date, chunk_size=16):
    current_date = start_date
    while current_date < end_date:
        end_chunk = current_date + datetime.timedelta(days=chunk_size-1)
        if end_chunk > end_date:
            end_chunk = end_date
        yield (current_date.strftime('%Y-%m-%d'), end_chunk.strftime('%Y-%m-%d'))
        current_date = end_chunk + datetime.timedelta(days=1)

# Function to calculate averages of elements in arrays
def calculate_averages(arrays):
    np_arrays = np.array(arrays)
    averages = np.mean(np_arrays, axis=0)
    return averages

# Function to calculate averages of elements in arrays
def calculate_averages(arrays):
    np_arrays = np.array(arrays)
    averages = np.mean(np_arrays, axis=0)
    return averages

# Function to process the request data into a dictionary
def get_weather_data(start_date,end_date):

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
    "latitude": [52.52,53.55,48.13,50.11,51.45,48.78],
    "longitude": [13.41,9.99,11.57,8.68,7.01,9.17],
    "start_date": start_date,
    "end_date": end_date,
    "daily": ["temperature_2m_max", "temperature_2m_min", "sunshine_duration", "precipitation_sum", "et0_fao_evapotranspiration"]
    }

    responses = openmeteo.weather_api(url, params=params)
    
    # Initialize Array for each parameter 
    daily_apparent_temperature_max = []
    daily_apparent_temperature_min = []
    daily_sunshine_duration = []
    daily_precipitation_sum = []
    daily_et0_fao_evapotranspiration = []
    
    for i in range(6):
        response = responses[i]

        daily = response.Daily()
    
        daily_apparent_temperature_max.append(daily.Variables(0).ValuesAsNumpy())
        daily_apparent_temperature_min.append(daily.Variables(1).ValuesAsNumpy())
        daily_sunshine_duration.append(daily.Variables(2).ValuesAsNumpy())
        daily_precipitation_sum.append(daily.Variables(3).ValuesAsNumpy())
        daily_et0_fao_evapotranspiration.append(daily.Variables(4).ValuesAsNumpy())    

    daily_data = {"date": pd.date_range(
        start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
        end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = daily.Interval()),
        inclusive = "left"
    )}
    
    daily_data["temperature_2m_max"] = calculate_averages(daily_apparent_temperature_max)
    daily_data["temperature_2m_min"] = calculate_averages(daily_apparent_temperature_min)
    daily_data["sunshine_duration"] = calculate_averages(daily_sunshine_duration)
    daily_data["precipitation_sum"] = calculate_averages(daily_precipitation_sum)
    daily_data["et0_fao_evapotranspiration"] = calculate_averages(daily_et0_fao_evapotranspiration)

    return daily_data

daily_data = get_weather_data("1950-01-01","1950-01-02")

#Initialize Data_dict
Data_dict = daily_data


# Loop through each year and month
for year in range(start_year, end_year + 1):
    for month in range(start_month, end_month + 1):
        # Define the start and end date for the current month
        start_date = datetime.date(year, month, 1)
        end_date = (start_date + datetime.timedelta(days=32)).replace(day=1) - datetime.timedelta(days=1)
        
        # Generate 16-day chunks for the current month
        for start_chunk, end_chunk in generate_date_ranges(start_date, end_date):
            # Call the API and retrieve the data for the date range
            API_response = get_weather_data(start_chunk, end_chunk)
            
            for key in Data_dict:
                Data_dict[key]= np.concatenate((Data_dict[key],API_response[key]))
    print(Data_dict['date'][-1])
    if ((year>1950) and (year%7==0)):
        time.sleep(60)

# Convert data to Dataframe
daily_dataframe = pd.DataFrame(data = Data_dict)

# Export data to csv
daily_dataframe.to_csv('daily_datax.csv', encoding='utf-8')