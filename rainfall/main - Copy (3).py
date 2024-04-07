from fastapi import FastAPI, HTTPException, Depends, status, Response, APIRouter, UploadFile, File, Query
import netCDF4 as nc
from typing import List, Tuple
import numpy as np
from datetime import datetime, timedelta 
import os
from fastapi.encoders import jsonable_encoder
import time
import requests

import os
import netCDF4 as nc
from fastapi import HTTPException, File, UploadFile , APIRouter


from . import models, schemas
from dependencies import get_db
from sqlalchemy import func 
from geopy.distance import geodesic
import math
from sqlalchemy import text

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



router = APIRouter()


async def process_batch(batch_data: List[models.Rainfalldata],db= db_dependency):
    db.add_all(batch_data)
    db.commit()
    db.close()

@router.post("/rainfall/import-data")
async def import_data(file: UploadFile = File(...), db= db_dependency):
    try:
        start_time = time.time()  # Start time of the script

        if not file.filename.endswith('.nc'):
            raise HTTPException(status_code=400, detail="Only files with .nc extension are allowed.")

        current_directory = os.path.dirname(os.path.abspath(__file__))
        temp_folder_path = os.path.join(current_directory, 'temp')
        os.makedirs(temp_folder_path, exist_ok=True)
        
        file_path = os.path.join(temp_folder_path, file.filename)
        with open(file_path, "wb") as temp_file:
            chunk_size = 1024  # You can adjust the chunk size as needed
            while chunk := await file.read(chunk_size):
                temp_file.write(chunk)

        data = nc.Dataset(file_path)
        longitude = data.variables['LONGITUDE'][:]
        latitude = data.variables['LATITUDE'][:]
        time_var = data.variables['TIME'][:]
        rainfall = data.variables['RAINFALL'][:]
        time_origin_str = data.variables["TIME"].time_origin

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

                        if len(batch_data) >= batch_size:
                            await process_batch(batch_data, db)
                            batch_data = []

        if batch_data:
            await process_batch(batch_data, db)

        data.close()
        print("Task completed ")
        
        end_time = time.time()  # End time of the script
        execution_time = end_time - start_time
        print(f"Script execution time: {execution_time} seconds")

        return {"message": f"Data imported successfully time took in seconds = {execution_time} "}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


# @router.get("/rainfall/get_data_by_lat_long_range")
# async def get_rainfall_data(lat_range: tuple = Query(...), lon_range: tuple = Query(...),  db= db_dependency):
#         #http://127.0.0.1:8000/rainfall/get_data_by_lat_long_range?lat_range=33.5&lat_range=34.5&lon_range=72.7&lon_range=73.3 
#         try:
#             # Validate latitude and longitude ranges
#             if len(lat_range) != 2 or len(lon_range) != 2:
#                 raise HTTPException(status_code=400, detail="Latitude and longitude ranges must be tuples of length 2.")
            
#             # Retrieve data for the specified latitude and longitude ranges
#             rainfall_data = db.query(models.Rainfalldata).filter(
#                 models.Rainfalldata.latitude >= lat_range[0],
#                 models.Rainfalldata.latitude <= lat_range[1],
#                 models.Rainfalldata.longitude >= lon_range[0],
#                 models.Rainfalldata.longitude <= lon_range[1]
#             ).all()

#             if not rainfall_data:
#                 raise HTTPException(status_code=404, detail="No data found for the specified latitude and longitude ranges.")

#             # Format data to return
#             formatted_data = []
#             for data_entry in rainfall_data:
#                 formatted_data.append({
#                     "date": data_entry.date,
#                     "latitude": data_entry.latitude,
#                     "longitude": data_entry.longitude,
#                     "rainfall": data_entry.rainfall
#                 })

#             return formatted_data
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
        
    



# @router.get("/rainfall/all-data")
# async def get_all_rainfall_data( db= db_dependency):
#     try:
#         # Retrieve all data from the database
#         rainfall_data = db.query(models.Rainfalldata).all()

#         if not rainfall_data:
#             raise HTTPException(status_code=404, detail="No data found in the database.")

#         # Format data to return
#         formatted_data = []
#         for data_entry in rainfall_data:
#             formatted_data.append({
#                 "date": data_entry.date,
#                 "latitude": data_entry.latitude,
#                 "longitude": data_entry.longitude,
#                 "rainfall": data_entry.rainfall
#             })

#         return formatted_data
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    

# @router.get("/rainfall/average_by_date")
# async def get_average_rainfall_by_date(db= db_dependency):
#     try:
#         # Query the database to calculate average rainfall grouped by date
#         average_rainfall_data = db.query(
#             models.Rainfalldata.date,
#             func.avg(models.Rainfalldata.rainfall).label("average_rainfall")
#         ).group_by(models.Rainfalldata.date).all()

#         if not average_rainfall_data:
#             raise HTTPException(status_code=404, detail="No data found in the database.")

#         # Format data to return
#         formatted_data = []
#         for date, average_rainfall in average_rainfall_data:
#             formatted_data.append({
#                 "date": date,
#                 "average_rainfall": average_rainfall
#             })

#         return formatted_data
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    

@router.get("/rainfall/monthly-average/{year}")
# /rainfall/monthly-average/2023
async def get_monthly_average_rainfall(year: int, db= db_dependency):
    try:
        # Query the database to calculate average monthly rainfall for the specified year
        monthly_average_rainfall_data = db.query(
            func.date_trunc('month', models.Rainfalldata.date).label("month"),
            func.avg(models.Rainfalldata.rainfall).label("average_rainfall")
        ).filter(
            func.extract('year', models.Rainfalldata.date) == year
        ).group_by(func.date_trunc('month', models.Rainfalldata.date)).order_by("month").all()

        if not monthly_average_rainfall_data:
            raise HTTPException(status_code=404, detail=f"No data found for the year {year}.")

        # Format data to return
        formatted_data = []
        for month, average_rainfall in monthly_average_rainfall_data:
            formatted_data.append({
                "month": month.strftime("%Y-%m"),
                "average_rainfall": average_rainfall
            })

        return formatted_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


 #Function to call OpenStreetMap Nominatim API and retrieve coordinates
def geocode_region(region_name: str):
    # Construct the URL for the Nominatim API endpoint
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": region_name,
        "format": "json",
        "limit": 1
    }

    # Send GET request to the API
    response = requests.get(url, params=params)

    # Check if request was successful
    if response.status_code == 200:
        # Parse JSON response
        data = response.json()
        if data:
            # Extract latitude and longitude from response
            latitude = float(data[0]['lat'])
            longitude = float(data[0]['lon'])
            return latitude, longitude
        else:
            raise HTTPException(status_code=404, detail="Region not found")
    else:
        raise HTTPException(status_code=response.status_code, detail="Failed to fetch coordinates")

# Mock function to query database based on coordinates
def get_rainfall_data(longitude: float, latitude: float):
    # Replace this with actual database query logic
    # For demonstration, returning mock data
    return {"longitude": longitude, "latitude": latitude, "rainfall": 10.5}  # Example data

# def find_closest_point(latitude, longitude, session):
#     closest_point = session.query(models.Rainfalldata).order_by(
#         func.pow(models.Rainfalldata.latitude - latitude, 2) + func.pow(models.Rainfalldata.longitude - longitude, 2)
#     ).first()
    
#     return closest_point

def fetch_rainfall_data(lat_range: Tuple[float, float], lon_range: Tuple[float, float], db):
    try:
        # Validate latitude and longitude ranges
        if len(lat_range) != 2 or len(lon_range) != 2:
            raise HTTPException(status_code=400, detail="Latitude and longitude ranges must be tuples of length 2.")
        
        # Retrieve data for the specified latitude and longitude ranges
        query = text('''
            SELECT DATE_TRUNC('month', date) AS month,
                   AVG(rainfall) AS average_rainfall
            FROM public.rainfalldata
            WHERE latitude >= :lat_min AND latitude <= :lat_max
              AND longitude >= :lon_min AND longitude <= :lon_max
            GROUP BY DATE_TRUNC('month', date)
            ORDER BY month ASC
        ''')

        result = db.execute(query, {"lat_min": lat_range[0], "lat_max": lat_range[1], "lon_min": lon_range[0], "lon_max": lon_range[1]})

        # Format the result
        formatted_data = []
        for row in result:
            formatted_data.append({
                "month": row[0],  # Accessing columns by integer index
                "average_rainfall": row[1]  # Accessing columns by integer index
            })

        if not formatted_data:
            raise HTTPException(status_code=404, detail="No data found for the specified latitude and longitude ranges.")

        return formatted_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

    


def bounding_box(latitude, longitude, distance_km):
    # Approximate radius of the Earth in km
    earth_radius = 6371.01

    # Calculate the maximum and minimum latitudes
    max_lat = latitude + (distance_km / earth_radius) * (180 / 3.141)
    min_lat = latitude - (distance_km / earth_radius) * (180 / 3.141)

    # Calculate the maximum and minimum longitudes
    max_lon = longitude + (distance_km / earth_radius) * (180 / 3.141) / \
              abs(math.cos(latitude * 3.141 / 180))
    min_lon = longitude - (distance_km / earth_radius) * (180 / 3.141) / \
              abs(math.cos(latitude * 3.141 / 180))

    return (min_lat, min_lon), (max_lat, max_lon)





# API endpoint to retrieve rainfall data based on region name
@router.get("/rainfall/")
async def get_rainfall_by_region(region_name: str, db= db_dependency):
        try:
        # Call geocoding function to get coordinates
            latitude, longitude = geocode_region(region_name)

            # Query database based on coordinates
            #rainfall_data = get_rainfall_data(longitude, latitude)

            #return rainfall_data
            #http://127.0.0.1:8000/rainfall/get_data_by_lat_long_range?lat_range=33.5&lat_range=34.5&lon_range=72.7&lon_range=73.3
            #closest_point = find_closest_point(latitude, longitude, db)

            # return fetch_rainfall_data(lat_range, lon_range, db)
        
            # lat_range = (33.5, 34.5)
            # lon_range = (72.7, 73.3)
            # lat_range = (lat, closest_point.latitude)
            # lon_range = (lon, closest_point.latitude)
            # result = fetch_rainfall_data(lat_range, lon_range, db)


        

            # Check if latitude and longitude are not None
            if latitude is not None and longitude is not None:
                # closest_point = find_closest_point(latitude, longitude, db)
                # lat_range = (round(latitude, 1), round(closest_point.latitude, 1))
                # lon_range = (round(longitude, 1), round(closest_point.longitude, 1))



                # Example coordinates (latitude, longitude) obtained from nominatim
                # latitude = 52.520008
                # longitude = 13.404954

                # Define the distance in km
                distance_km = 40

                # Calculate the bounding box
                (min_lat, min_lon), (max_lat, max_lon) = bounding_box(latitude, longitude, distance_km)

                # print("Minimum Latitude:", min_lat)
                # print("Maximum Latitude:", max_lat)
                # print("Minimum Longitude:", min_lon)
                # print("Maximum Longitude:", max_lon)

                #return f"min_lat :  { round(min_lat , 1)}  , max_lat :  {round(max_lat , 1)} - min_lon :  {round(min_lon , 1)} , max_lon : {round(max_lon , 1)} "
                #return fetch_rainfall_data(lat_range, lon_range, db)
                
                return fetch_rainfall_data((round(min_lat , 1), round(max_lat , 1)), (round(min_lon , 1) ,round(max_lon , 1)), db)
           


                #return closest_point
            #return f"latitude :  {latitude}  , latitude2 :  {closest_point.latitude} - longitude : {longitude} , longitude2 : {closest_point.longitude} "
        except HTTPException as e:
            raise e 
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) 
    




@router.get("/rainfall/get_data_by_lat_long_range")
async def get_rainfall_data(lat_range: Tuple[float, float] = Query(...), lon_range: Tuple[float, float] = Query(...), db= db_dependency):
    return fetch_rainfall_data(lat_range, lon_range, db)


