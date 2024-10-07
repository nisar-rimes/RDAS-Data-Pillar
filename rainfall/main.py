from fastapi import FastAPI, HTTPException, APIRouter
from pydantic import BaseModel
import xarray as xr
import rioxarray
import geopandas as gpd
import os
import pandas as pd
import re

router = APIRouter()

# Path to netCDF and shapefile (absolute path)
#path = os.path.abspath("netcdf_files")
#shapefile = os.path.join(path, "India_districts_gadm.shp")

# Construct the path relative to the location of main.py
base_path = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the main.py file
path= os.path.join(base_path, "netcdf_files")
shapefile = os.path.join(base_path, "netcdf_files", "India_districts_gadm.shp")


# Print the path for debugging purposes (you can remove this after verifying)
print(f"Shapefile path: {shapefile}")

# Input Model for API
class RainfallRequest(BaseModel):
    country: str
    district_name: str
    start_year: int
    end_year: int

# API route for extracting rainfall data for India
@router.post("/rainfall")
async def extract_rainfall(request: RainfallRequest):
    district_name = request.district_name
    start_year = request.start_year
    end_year = request.end_year

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
            # Extract the year from the filename (e.g., "rainfall_1930.nc" -> year = 1930)
            year_match = re.search(r'rainfall_(\d{4})\.nc', file)
            if year_match:
                year = int(year_match.group(1))

                # Process only if the year is within the user-specified range
                if start_year <= year <= end_year:
                    try:
                        # Read the NetCDF file
                        nc_file = os.path.join(path, file)
                        ds = xr.open_dataset(nc_file)

                        # Convert the TIME coordinate to datetime objects for filtering
                        ds['TIME'] = pd.to_datetime(ds['TIME'].values)

                        # Define CRS for netCDF if needed and reproject the shapefile
                        ds.rio.write_crs("epsg:4326", inplace=True)
                        gdf = gdf.to_crs(ds.rio.crs)

                        # Select the district by name
                        selected_district = gdf[gdf['NAME_2'] == district_name]

                        # Loop through each month (from 1 to 12)
                        for month in range(1, 13):
                            # Extract the data for the specific month and year
                            ds_selected = ds.sel(TIME=(ds['TIME'].dt.year == year) & (ds['TIME'].dt.month == month))

                            if ds_selected.sizes['TIME'] == 0:
                                # If no data for the selected month, skip to the next
                                print(f"No data available for {year}-{month:02d}")
                                continue

                            # Clip the netCDF data using the shapefile for the selected district
                            clipped_district = ds_selected.rio.clip(selected_district.geometry, selected_district.crs)

                            # Calculate the mean, min, and max over the clipped area for the selected district for each day
                            clipped_mean = clipped_district.mean(dim=['LATITUDE', 'LONGITUDE'], skipna=True)

                          # Calculate the overall min, max, and mean for the month (over time)
                            monthly_min = clipped_mean['RAINFALL'].min(dim='TIME').item()  # Convert to scalar
                            monthly_max = clipped_mean['RAINFALL'].max(dim='TIME').item()  # Convert to scalar
                            monthly_mean = clipped_mean['RAINFALL'].mean(dim='TIME').item()  # Convert to scalar

                            if pd.notna(monthly_min) and pd.notna(monthly_max) and pd.notna(monthly_mean):
                                # Create a temporary DataFrame for the current month and year
                                temp_df = pd.DataFrame({
                                    'Year': [year],
                                    'Month': [month],
                                    'Min': [monthly_min],
                                    'Max': [monthly_max],
                                    'Mean': [monthly_mean]
                                })
                                
                                # Concatenate the current month's data to the main DataFrame
                                all_years_stats = pd.concat([all_years_stats, temp_df], ignore_index=True)

                                print(f"Year {year} - Month {month:02d} - Min: {monthly_min}, Max: {monthly_max}, Mean: {monthly_mean}")
                            else:
                                print(f"Invalid data for {year}-{month:02d}. Skipping...")
                           
                    except Exception as e:
                        raise HTTPException(status_code=500, detail=f"Error processing file {file}: {str(e)}")

    # Round the values for better readability
    all_years_stats = all_years_stats.round(2)

    # Return the result as JSON
    # return all_years_stats.to_dict(orient="records")
    return {
                "metadata": {
                                "source": {
                                "name": "IMD New High Spatial Resolution",
                                "url": "https://www.imdpune.gov.in/cmpg/Griddata/Rainfall_25_NetCDF.html"
                                },
                                "indic": "Rainfall",
                            }
                              ,
                "data":  all_years_stats.to_dict(orient="records"),
                #"Years": years
            }

