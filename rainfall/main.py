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
from datetime import date

router = APIRouter()

# Construct the path relative to the location of main.py
base_path = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the main.py file
path = os.path.join(base_path, "netcdf_files")
shapefile = os.path.join(base_path, "netcdf_files", "India_districts_gadm.shp")

def fetch_api_data():
    url = "https://api.rdas.live/data/get/region"
    payload = {"country": "IND", "level": 2}
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        return response.json()['data']
    else:
        raise HTTPException(status_code=response.status_code, detail="Error fetching data from API.")

# Function to get district details by district code
def get_district_by_code(district_code):
    api_data = fetch_api_data()
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
