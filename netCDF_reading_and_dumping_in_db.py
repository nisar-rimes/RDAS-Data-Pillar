import netCDF4 as nc
import numpy as np
import psycopg2
from datetime import datetime, timedelta

# Open the NetCDF file
nc_file = "RF25_ind2023_rfp25.nc"
data = nc.Dataset(nc_file)

# Extract variables
longitude = data.variables['LONGITUDE'][:]
latitude = data.variables['LATITUDE'][:]
time_var = data.variables['TIME']
rainfall = data.variables['RAINFALL']

time_origin_str = data.variables["TIME"].time_origin

# Define function to convert NetCDF time to datetime
def convert_time(nc_time, time_origin_str):
    # Parse the time origin string to datetime object
    time_origin = datetime.strptime(time_origin_str, "%d-%b-%Y")
    # Calculate the timedelta from the time origin
    time_delta = timedelta(days=float(nc_time))
    # Return the sum of time origin and timedelta
    return time_origin + time_delta

# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname="RDAS",
    user="postgres",
    password="postgres",
    host="localhost",
    port=5432
    #port:5050
)

# Create a cursor object
cur = conn.cursor()
print("task started ")

# Loop through time, latitude, and longitude to extract data and insert into the database
for t_idx in range(len(time_var)):
    for lat_idx in range(len(latitude)):
        for lon_idx in range(len(longitude)):
            date = convert_time(time_var[t_idx], time_origin_str)
            rainfall_value = float(rainfall[t_idx, lat_idx, lon_idx].data)  # Convert masked array to regular NumPy array
            if rainfall_value != -999.0:  # Exclude missing values
                cur.execute("""INSERT INTO rainfall_data (date, longitude, latitude, rainfall) VALUES (%s, %s, %s, %s)""", 
                            (date, float(longitude[lon_idx]), float(latitude[lat_idx]), rainfall_value))
                conn.commit()

# Close the NetCDF file
data.close()

# Close cursor and connection
cur.close()
conn.close()
print("task completed ")
