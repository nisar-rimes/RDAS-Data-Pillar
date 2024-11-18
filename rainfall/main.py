from fastapi import FastAPI, HTTPException, APIRouter
from pydantic import BaseModel, Field
import xarray as xr
import rioxarray
import geopandas as gpd
import os
import pandas as pd
import re
import requests
from typing import List
from datetime import  date, datetime, timedelta
import imdlib as imd
import numpy as np
from shapely.geometry import Point
import json
from fastapi import HTTPException

router = APIRouter()

# Construct the path relative to the location of main.py
base_path = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the main.py file
path = os.path.join(base_path, "netcdf_files")
shapefile = os.path.join(base_path, "netcdf_files", "India_districts_gadm.shp")

# def fetch_api_data():
#     url = "https://api.rdas.live/data/get/region"
#     payload = {"country": "IND", "level": 2}
#     response = requests.post(url, json=payload)
#     if response.status_code == 200:
#         return response.json()['data']
#     else:
#         raise HTTPException(status_code=response.status_code, detail="Error fetching data from API.")
    





def fetch_api_data(use_file=False):

    file_path = os.path.join(base_path, "api_data_districts_india.json")  # Construct the full path to the JSON file

    if use_file:
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail=f"Data file not found at {file_path}.")
        
        try:
            with open(file_path, "r") as file:
                data = json.load(file)
                return data['data']
        except json.JSONDecodeError:
            raise HTTPException(status_code=500, detail="Error decoding JSON data from file.")
    # else:
    #     # Fetch data from API
    #     import requests
    #     url = "https://api.rdas.live/data/get/region"
    #     payload = {"country": "IND", "level": 2}
    #     response = requests.post(url, json=payload)
    #     if response.status_code == 200:
    #         return response.json()['data']
    #     else:
    #         raise HTTPException(status_code=response.status_code, detail="Error fetching data from API.")


# Function to get district details by district code
def get_district_by_code(district_code):
    api_data = fetch_api_data(use_file=True)
    if api_data:
        for district in api_data:
            if district['code'] == district_code:
                return  district
        raise HTTPException(status_code=404, detail=f"District with code {district_code} not found.")
    raise HTTPException(status_code=500, detail="Error fetching API data.")

# Input Model for API
class RainfallRequest(BaseModel):
    source: str = Field(..., example="IMD")
    indic: str = Field(..., example="rainfall")
    period: str = Field(..., example="monthly")
    area: List[str] = Field(..., example=["IND.35.10_1", "IND.35.10_1"])
    start_date: date = Field(..., example="2021-01-01")
    end_date: date = Field(..., example="2023-12-31")

# API route for extracting rainfall data for India
@router.post("/rainfall")
async def extract_rainfall(request: RainfallRequest):
    district_codes = request.area
    start_year = request.start_date.year
    end_year = request.end_date.year

    # Validate input years
    if start_year > end_year:
        raise HTTPException(status_code=400, detail="Start year should be less than or equal to end year.")

    # Initialize list to store all district results
    all_district_data = []

    # Read the shapefile once
    try:
        gdf = gpd.read_file(shapefile)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading shapefile: {str(e)}")

    # Loop through each district code in the request
    for district_code in district_codes:
        district_details = get_district_by_code(district_code)
        district_name = district_details.get('name')

        # Initialize a DataFrame to store the results for this district
        district_data = []

        # Loop through all NetCDF files in the folder
        for file in os.listdir(path):
            if file.endswith(".nc") and file.startswith("rainfall_"):
                year_match = re.search(r'rainfall_(\d{4})\.nc', file)
                if year_match:
                    year = int(year_match.group(1))

                    if start_year <= year <= end_year:
                        try:
                            nc_file = os.path.join(path, file)
                            ds = xr.open_dataset(nc_file)

                            ds['TIME'] = pd.to_datetime(ds['TIME'].values)
                            ds.rio.write_crs("epsg:4326", inplace=True)

                            # Select the district by name
                            selected_district = gdf[gdf['NAME_2'] == district_name]

                            if selected_district.empty:
                                raise HTTPException(status_code=404, detail=f"District '{district_name}' not found in shapefile.")

                            # Loop through each month
                            for month in range(1, 13):
                                ds_selected = ds.sel(TIME=(ds['TIME'].dt.year == year) & (ds['TIME'].dt.month == month))

                                if ds_selected.sizes['TIME'] == 0:
                                    continue

                                # Clip the netCDF data using the shapefile for the selected district
                                clipped_district = ds_selected.rio.clip(selected_district.geometry, selected_district.crs)

                                clipped_mean = clipped_district.mean(dim=['LATITUDE', 'LONGITUDE'], skipna=True)
                                monthly_mean = clipped_mean['RAINFALL'].mean(dim='TIME').item()

                                if pd.notna(monthly_mean):
                                    district_data.append({
                                        "date": f"{year}-{month:02d}-15",  # Assume mid-month for monthly data
                                        "area": district_code,
                                        "source": request.source,
                                        "indicid": request.indic,
                                        "unitid": "mm",
                                        "value": round(monthly_mean, 2)
                                    })

                        except Exception as e:
                            raise HTTPException(status_code=500, detail=f"Error processing file {file}: {str(e)}")

        # Add the district's data to the overall list
        all_district_data.extend(district_data)

    # Final JSON response
    return {
        "metadata": {
            "source": {
                "name": "IMD New High Spatial Resolution",
                "url": "https://www.imdpune.gov.in/cmpg/Griddata/Rainfall_25_NetCDF.html"
            },
            "indic": "Rainfall",
            "period": "Monthly",
            "input": request.dict(),
            "status": "success",
            "cache": "false",
            "hash": ""
        },
        "data": all_district_data
    }


# Function to get district names
def get_district_names():
    try:
        gdf = gpd.read_file(shapefile)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading shapefile: {str(e)}")

    districts = gdf['NAME_2'].unique()
    return sorted(districts.tolist())

# API route for getting district names
@router.get("/districts/india", response_model=list)
async def districts():
    district_names = get_district_names()
    return district_names
















    



# Input model
class TemperatureRequest(BaseModel):
    source: str = Field(..., example="IMD")
    indic: str = Field(..., example="temperature")
    period: str = Field(..., example="monthly")
    area: List[str] = Field(..., example=["IND.1.2_1", "IND.2.3_1"])  # List of district IDs
    start_date: date = Field(..., example="2020-01-01")
    end_date: date = Field(..., example="2021-12-31")

@router.post("/temperature")
async def extract_temperature(request: TemperatureRequest):
    """
    Extract temperature data for the specified districts and date range.
    """
    district_codes = request.area
    start_year = request.start_date.year
    end_year = request.end_date.year

    # Validate input years
    if start_year > end_year:
        raise HTTPException(status_code=400, detail="Start year should be less than or equal to end year.")

    # Initialize list to store all district results
    all_district_data = []

    # Fetch district mapping from API
    try:
        district_mapping = fetch_api_data(use_file=True)
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)

    # Loop through each district code in the request
    for district_code in district_codes:
        district_details = get_district_by_code(district_code)
        district_name = district_details.get('name')

        # Initialize a list to store the results for this district
        district_data = []

        # Paths for data
        data_dir = os.path.join(base_path, "tmax")

        # Read the shapefile once
        try:
            gdf = gpd.read_file(shapefile)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error reading shapefile: {str(e)}")

        # Iterate through years
        for year in range(start_year, end_year + 1):
            # Open IMD data and extract the data array
            try:
                data = imd.open_data('tmax', year, year, 'yearwise', data_dir)
                np_array = data.data  # Data array from IMD
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error opening IMD data for year {year}: {str(e)}")

            # Set grid parameters
            grid_size = 1  # Example: 1 degree for tmax/tmin
            x_start = 67.5  # Starting longitude
            y_start = 7.5   # Starting latitude
            days = 366 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 365
            monthly_data = {month: [] for month in range(1, 13)}

            # Loop through the grid and extract data for points within the district
            for j in range(31):  # Adjust loop bounds based on grid size
                for i in range(31):  # Adjust loop bounds based on grid size
                    lon = (i * grid_size) + x_start
                    lat = (j * grid_size) + y_start

                    # Check if the point is within the district
                    point = Point(lon, lat)
                    selected_district = gdf[gdf['NAME_2'] == district_name]
                    if selected_district.empty:
                        raise HTTPException(status_code=404, detail=f"District '{district_name}' not found in shapefile.")

                    if selected_district.geometry.iloc[0].contains(point):
                        for day_idx in range(days):
                            date_obj = datetime(year, 1, 1) + timedelta(days=day_idx)
                            month = date_obj.month
                            val = np_array[day_idx, i, j]
                            if val not in [99.9000015258789, -999]:  # Exclude invalid data
                                monthly_data[month].append(val)

            # Calculate monthly means and append to district data
            for month, values in monthly_data.items():
                if values:
                    monthly_mean = sum(values) / len(values)
                    district_data.append({
                        "date": f"{year}-{month:02d}-15",  # Assume mid-month for monthly data
                        "area": district_code,
                        "source": request.source,
                        "indicid": request.indic,
                        "unitid": "Celsius",
                        "value": round(monthly_mean, 2)
                    })

        # Add the district's data to the overall list
        all_district_data.extend(district_data)

    # Final JSON response
    return {
        "metadata": {
            "source": {
                "name": request.source,
                "url": "https://www.imdpune.gov.in/"
            },
            "indic": request.indic.capitalize(),
            "period": request.period.capitalize(),
            "input": request.dict(),
            "status": "success",
            "cache": "false",
            "hash": ""
        },
        "data": all_district_data
    }

