from fastapi import FastAPI, HTTPException, APIRouter
from pydantic import BaseModel
import xarray as xr
import rioxarray
import geopandas as gpd
import os
import pandas as pd
import re
import requests

router = APIRouter()

# Construct the path relative to the location of main.py
base_path = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the main.py file
path = os.path.join(base_path, "netcdf_files")
shapefile = os.path.join(base_path, "netcdf_files", "India_districts_gadm.shp")

def fetch_api_data():
    url = "https://api.rdas.live/data/get/region"
    payload = {
        "country": "IND",
        "level": 2
    }
    response = requests.post(url, json=payload)
    
    if response.status_code == 200:
        return response.json()['data']  # Return the API data as a list of dictionaries
    else:
        raise HTTPException(status_code=response.status_code, detail="Error fetching data from API.")

# Function to get district details by district code
def get_district_by_code(district_code):
    api_data = fetch_api_data()
    
    if api_data:
        # Search for the district with the matching code
        for district in api_data:
            if district['code'] == district_code:
                return district
        # If not found, raise an HTTPException
        raise HTTPException(status_code=404, detail=f"District with code {district_code} not found.")
    
    raise HTTPException(status_code=500, detail="Error fetching API data.")

# Input Model for API
class RainfallRequest(BaseModel):
    country: str
    district_code: str
    start_year: int
    end_year: int

# API route for extracting rainfall data for India
@router.post("/rainfall")
async def extract_rainfall(request: RainfallRequest):
    district_code = request.district_code
    start_year = request.start_year
    end_year = request.end_year

    district_details = get_district_by_code(district_code)
    district_name = district_details.get('name')

    # Validate input years
    if start_year > end_year:
        raise HTTPException(status_code=400, detail="Start year should be less than or equal to end year.")

    # Read the shapefile
    try:
        gdf = gpd.read_file(shapefile)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading shapefile: {str(e)}")

    # Initialize a DataFrame to store the results for all years and months
    all_years_stats = pd.DataFrame(columns=['Year', 'Month', 'Min', 'Max', 'Mean'])

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

                        # Loop through each month (from 1 to 12)
                        for month in range(1, 13):
                            ds_selected = ds.sel(TIME=(ds['TIME'].dt.year == year) & (ds['TIME'].dt.month == month))

                            if ds_selected.sizes['TIME'] == 0:
                                print(f"No data available for {year}-{month:02d}")
                                continue

                            # Clip the netCDF data using the shapefile for the selected district
                            clipped_district = ds_selected.rio.clip(selected_district.geometry, selected_district.crs)

                            clipped_mean = clipped_district.mean(dim=['LATITUDE', 'LONGITUDE'], skipna=True)
                            monthly_min = clipped_mean['RAINFALL'].min(dim='TIME').item()
                            monthly_max = clipped_mean['RAINFALL'].max(dim='TIME').item()
                            monthly_mean = clipped_mean['RAINFALL'].mean(dim='TIME').item()

                            if pd.notna(monthly_min) and pd.notna(monthly_max) and pd.notna(monthly_mean):
                                temp_df = pd.DataFrame({
                                    'Year': [year],
                                    'Month': [month],
                                    'Min': [monthly_min],
                                    'Max': [monthly_max],
                                    'Mean': [monthly_mean]
                                })
                                
                                all_years_stats = pd.concat([all_years_stats, temp_df], ignore_index=True)

                    except Exception as e:
                        raise HTTPException(status_code=500, detail=f"Error processing file {file}: {str(e)}")

    # Sort the DataFrame by Year and Month after accumulating all data
    all_years_stats = all_years_stats.sort_values(by=['Year', 'Month'], ascending=[True, True]).reset_index(drop=True)
    all_years_stats = all_years_stats.round(2)

    return {
        "metadata": {
            "source": {
                "name": "IMD New High Spatial Resolution",
                "url": "https://www.imdpune.gov.in/cmpg/Griddata/Rainfall_25_NetCDF.html"
            },
            "indic": "Rainfall",
        },
        "data": all_years_stats.to_dict(orient="records"),
    }


# Function to get district names
def get_district_names():
    try:
        # Read the shapefile
        gdf = gpd.read_file(shapefile)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading shapefile: {str(e)}")

    # Extract district names
    districts = gdf['NAME_2'].unique()
    return sorted(districts.tolist())

# API route for getting district names
@router.get("/districts/india", response_model=list)
async def districts():
    district_names = get_district_names()
    return district_names



