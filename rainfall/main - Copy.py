from fastapi import FastAPI, HTTPException, Depends, status, Response, APIRouter
import netCDF4 as nc
from typing import List
#import psycopg2
import numpy as np  # Import NumPy for handling masked arrays
from datetime import datetime, timedelta 

from . import models,schemas
from dependencies import get_db 
router= APIRouter()


# Define function to convert NetCDF time to datetime
def convert_time(nc_time, time_origin_str):
    # Parse the time origin string to datetime object
    time_origin = datetime.strptime(time_origin_str, "%d-%b-%Y")
    # Calculate the timedelta from the time origin
    time_delta = timedelta(days=float(nc_time))
    # Return the sum of time origin and timedelta
    return time_origin + time_delta


db_dependency = Depends(get_db)

@router.get("/rainfall/import-data")
async def import_data(db= db_dependency):
    try:
        # Open the NetCDF file
        #nc_file = "RF25_ind2023_rfp25.nc"
        data = nc.Dataset("C:\\RDAS-Data-Pillar\\RF25_ind2023_rfp25.nc")

        # Extract variables
        longitude = data.variables['LONGITUDE'][:]
        latitude = data.variables['LATITUDE'][:]
        time_var = data.variables['TIME']
        rainfall = data.variables['RAINFALL']

        time_origin_str = data.variables["TIME"].time_origin

        # Create a cursor object
        #cur = conn.cursor()
        #print("task started ")

        # Loop through time, latitude, and longitude to extract data and insert into the database
        for t_idx in range(len(time_var)):
            for lat_idx in range(len(latitude)):
                for lon_idx in range(len(longitude)):
                    date = convert_time(time_var[t_idx], time_origin_str)
                    rainfall_value = float(rainfall[t_idx, lat_idx, lon_idx].data)  # Convert masked array to regular NumPy array
                    if rainfall_value != -999.0:  # Exclude missing values


                        # cur.execute("""INSERT INTO rainfall_data (date, longitude, latitude, rainfall) VALUES (%s, %s, %s, %s)""", 
                        #             (date, float(longitude[lon_idx]), float(latitude[lat_idx]), rainfall_value))
                        

                        db_rainfall_data = models.rainfall_data(
                            date=date,
                            longitude= float(longitude[lon_idx]),
                            latitude=  float(latitude[lat_idx]),
                            rainfall= rainfall_value )

                        db.add(db_rainfall_data)
                        db.commit()
                        #db.refresh(db_rainfall_data)


                     
       # Close the NetCDF file
        data.close()
        # Close cursor and connection
        # cur.close()
        # conn.close()
        count  =models.query(func.count(RainfallData.id)).scalar()
        print("task completed ")
        return {"message": "Data imported successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")









