import pandas as pd
import numpy as np 
import os

current_directory = os.getcwd()
file_path = os.path.join(current_directory, 'flood_data', 'flood_warnings.csv')
df = pd.read_csv(file_path)


file_path = os.path.join(current_directory, 'flood_data', 'stations.csv')
df_stations = pd.read_csv(file_path)
