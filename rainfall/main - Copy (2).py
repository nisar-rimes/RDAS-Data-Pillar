from fastapi import FastAPI, HTTPException, Depends, status, Response, APIRouter, UploadFile, File, Query
import netCDF4 as nc
from typing import List
import numpy as np
from datetime import datetime, timedelta 
import os
from fastapi.encoders import jsonable_encoder


from . import models, schemas
from dependencies import get_db
from sqlalchemy import func 

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

import os
import netCDF4 as nc
from fastapi import HTTPException, File, UploadFile
from fastapi import APIRouter

router = APIRouter()

@router.post("/rainfall/import-data")
async def import_data(file: UploadFile = File(...), db= db_dependency):
    try:
        if not file.filename.endswith('.nc'):
            raise HTTPException(status_code=400, detail="Only files with .nc extension are allowed.")

        contents = await file.read()

        current_directory = os.path.dirname(os.path.abspath(__file__))
        temp_folder_path = os.path.join(current_directory, 'temp')
        os.makedirs(temp_folder_path, exist_ok=True)
        
        file_path = os.path.join(temp_folder_path, file.filename)
        with open(file_path, "wb") as temp_file:
            temp_file.write(contents)

        data = nc.Dataset(file_path)
        longitude = data.variables['LONGITUDE'][:]
        latitude = data.variables['LATITUDE'][:]
        time_var = data.variables['TIME'][:]
        rainfall = data.variables['RAINFALL'][:]
        time_origin_str = data.variables["TIME"].time_origin

        count = 0
        batch_size = 1000
        batch_data = []

        for t_idx in range(len(time_var)):
            date_nc = convert_time(time_var[t_idx], time_origin_str)
            for lat_idx in range(len(latitude)):
                for lon_idx in range(len(longitude)):
                    rainfall_value = rainfall[t_idx, lat_idx, lon_idx]
                    if rainfall_value != -999.0:
                        db_obj = models.Rainfalldata(date=date_nc, longitude=longitude[lon_idx], latitude=latitude[lat_idx], rainfall=rainfall_value)
                        batch_data.append(db_obj)
                        count += 1

                        if len(batch_data) >= batch_size:
                            db.add_all(batch_data)
                            db.commit()
                            batch_data = []

        if batch_data:
            db.add_all(batch_data)
            db.commit()

        data.close()
        print("Task completed ")
        
        return {"message": "Data imported successfully:: " + str(count)}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.get("/rainfall/get_data_by_lat_long_range")
async def get_rainfall_data(lat_range: tuple = Query(...), lon_range: tuple = Query(...),  db= db_dependency):
        #http://127.0.0.1:8000/rainfall/get_data_by_lat_long_range?lat_range=33.5&lat_range=34.5&lon_range=72.7&lon_range=73.3
        try:
            # Validate latitude and longitude ranges
            if len(lat_range) != 2 or len(lon_range) != 2:
                raise HTTPException(status_code=400, detail="Latitude and longitude ranges must be tuples of length 2.")
            
            # Retrieve data for the specified latitude and longitude ranges
            rainfall_data = db.query(models.Rainfalldata).filter(
                models.Rainfalldata.latitude >= lat_range[0],
                models.Rainfalldata.latitude <= lat_range[1],
                models.Rainfalldata.longitude >= lon_range[0],
                models.Rainfalldata.longitude <= lon_range[1]
            ).all()

            if not rainfall_data:
                raise HTTPException(status_code=404, detail="No data found for the specified latitude and longitude ranges.")

            # Format data to return
            formatted_data = []
            for data_entry in rainfall_data:
                formatted_data.append({
                    "date": data_entry.date,
                    "latitude": data_entry.latitude,
                    "longitude": data_entry.longitude,
                    "rainfall": data_entry.rainfall
                })

            return formatted_data
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    



@router.get("/rainfall/all-data")
async def get_all_rainfall_data( db= db_dependency):
    try:
        # Retrieve all data from the database
        rainfall_data = db.query(models.Rainfalldata).all()

        if not rainfall_data:
            raise HTTPException(status_code=404, detail="No data found in the database.")

        # Format data to return
        formatted_data = []
        for data_entry in rainfall_data:
            formatted_data.append({
                "date": data_entry.date,
                "latitude": data_entry.latitude,
                "longitude": data_entry.longitude,
                "rainfall": data_entry.rainfall
            })

        return formatted_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    

@router.get("/rainfall/average_by_date")
async def get_average_rainfall_by_date(db= db_dependency):
    try:
        # Query the database to calculate average rainfall grouped by date
        average_rainfall_data = db.query(
            models.Rainfalldata.date,
            func.avg(models.Rainfalldata.rainfall).label("average_rainfall")
        ).group_by(models.Rainfalldata.date).all()

        if not average_rainfall_data:
            raise HTTPException(status_code=404, detail="No data found in the database.")

        # Format data to return
        formatted_data = []
        for date, average_rainfall in average_rainfall_data:
            formatted_data.append({
                "date": date,
                "average_rainfall": average_rainfall
            })

        return formatted_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

