from fastapi import FastAPI, HTTPException, Depends, status, Response, APIRouter, UploadFile, File
import netCDF4 as nc
from typing import List
import numpy as np
from datetime import datetime, timedelta 
import os
from fastapi.encoders import jsonable_encoder


from . import models, schemas
from dependencies import get_db 

router = APIRouter()

# Define function to convert NetCDF time to datetime
def convert_time(nc_time, time_origin_str):
    # Parse the time origin string to datetime object
    time_origin = datetime.strptime(time_origin_str, "%d-%b-%Y")
    # Calculate the timedelta from the time origin
    time_delta = timedelta(days=float(nc_time))
    # Return the sum of time origin and timedelta
    return time_origin + time_delta

db_dependency = Depends(get_db)

@router.post("/rainfall/import-data")
async def import_data(file: UploadFile = File(...), db= db_dependency):
    try:

        
        # Read the contents of the uploaded file
        contents = await file.read()

        # Define the directory to store the uploaded files
        current_directory = os.path.dirname(os.path.abspath(__file__))
        temp_folder_path = os.path.join(current_directory, 'temp')
    
        # Ensure that the directory exists
        os.makedirs(temp_folder_path, exist_ok=True)
        
        # Save the file to the upload directory
        file_path = os.path.join(temp_folder_path, file.filename)
        with open(file_path, "wb") as temp_file:
            temp_file.write(contents)

        # Open the NetCDF file
        data = nc.Dataset(file_path)

        # Extract variables
        longitude = data.variables['LONGITUDE'][:]
        latitude = data.variables['LATITUDE'][:]
        time_var = data.variables['TIME'][:]
        rainfall = data.variables['RAINFALL'][:]

        time_origin_str = data.variables["TIME"].time_origin

        batch_size = 1000  # Batch size for database insertion

        # Loop through time, latitude, and longitude to extract data and insert into the database
        count = 0
    
        for t_idx in range(len(time_var)):
            for lat_idx in range(len(latitude)):
                for lon_idx in range(len(longitude)):
                    date_nc = convert_time(time_var[t_idx], time_origin_str)
                    rainfall_value = rainfall[t_idx, lat_idx, lon_idx]
            
                    if rainfall_value != -999.0:
                        db_obj = models.Rainfalldata(date=date_nc, longitude=lon_idx, latitude=lat_idx, rainfall=rainfall_value)
                        db.add(db_obj)
                        db.commit()
                
                        count += 1
                
                        # if count == batch_size:
                        #     data.close()
                        #     count = 0  # Reset batch data counter
                        #     break  # Break out of the loop if batch size is reached
                            
                        # else:  # Innermost loop completed without breaking
                        #  continue  # Continue to the next iteration of the middle loop
           


        data.close()

        print("Task completed ")
        
        return {"message": "Data imported successfully:: " + str(count)}
    except Exception as e:
        # Rollback the transaction in case of an exception
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
