#!/usr/bin/env python3

# Libraries for the taks (not all of them are require)

import numpy as py
import datetime
import os # Know where you are
import databricks.koalas as ks # Koalas
import pandas as pd # Require
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

sc = SparkContext(master = 'local[*]') #Start the spark functionality
sqlContext = SQLContext(sc) # Define the context on sql to describe the dataframe

# Read the text files using pandas library and separate the headers using delim_whitespace 
weather_1 = pd.read_csv("/home/antonio/Documents/Weather/weather01.txt",delim_whitespace = True)
weather_2 = pd.read_csv("/home/antonio/Documents/Weather/weather02.txt",delim_whitespace = True)
weather_3 = pd.read_csv("/home/antonio/Documents/Weather/weather03.txt",delim_whitespace = True)
weather_4 = pd.read_csv("/home/antonio/Documents/Weather/weather04.txt",delim_whitespace = True)
weather_5 = pd.read_csv("/home/antonio/Documents/Weather/weather05.txt",delim_whitespace = True)
weather_6 = pd.read_csv("/home/antonio/Documents/Weather/weather06.txt",delim_whitespace = True)

# Join all the weather_num files
weather = pd.concat([weather_1,weather_2,weather_3,weather_4,weather_5,weather_6], axis=1)
del(weather['X']) # Drop the column extra
# Melting some columns to create the a new column called day
weather = pd.melt(weather,
                 id_vars=['year', 'month', 'measure'],
                 var_name='day');
weather.dropna(inplace=True) # Remove missing values
weather = weather.reset_index(drop=True) # Reset the index to rename the columns
weather = weather.rename(columns={'year': 'Year', 'month': 'Month','measure': 'Type of Weather','day':'Day','value':'Value'}) #Rename the columns
# Create a function to add the context of date according to YYYY/MM/DD
def creating_date(row):
    return "%d-%02d-%02d" % (row['Year'], row['Month'], int(row['Day'][1:]))

weather['Date'] = weather.apply(creating_date,axis=1)
weather = weather [['Type of Weather', 'Value', 'Date']]

# Repeat the dropna command for removing the missing values on the dataframe
weather = weather.replace('<NA>',py.NaN)
weather = weather.dropna()
# Reorganized the data by using the index with pivot command
weather = weather.pivot(index='Date', columns= 'Type of Weather', values='Value')
weather = weather.reset_index() # Reboot the index
weather = weather.drop(365,axis=0) # It will no take into account the values that are more than the 365's day
weather['Events'] =weather['Events'].fillna(method='ffill') # It with fill the missing date with random numbers

# Then state the column Date as index
weather = weather.sort_values('Date',ascending= False)
weather = weather.set_index('Date',drop=True)
weather.index = pd.to_datetime(weather.index) # Add the format datetime

# Change the type of data from some columns
weather['Max.TemperatureF'] = weather['Max.TemperatureF'].astype(int)
weather['Min.TemperatureF'] = weather['Min.TemperatureF'].astype(int)
weather['Mean.TemperatureF'] = weather['Mean.TemperatureF'].astype(int)
weather['Max.Dew.PointF'] = weather['Max.Dew.PointF'].astype(int)
weather['MeanDew.PointF'] = weather['MeanDew.PointF'].astype(int)
weather['Min.DewpointF'] = weather['Min.DewpointF'].astype(int)

weather_ks = ks.from_pandas(weather) # Transfrom the pandas core into a koalas dataframe
type(weather_ks) #Check if it worked
weather_ks.columns # Verify the type of columns in the dataframe

weather.to_csv() # The command will turn into a csv file
sc.stop() # This will stop the SparkContext

