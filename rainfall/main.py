from fastapi import FastAPI, UploadFile, File, HTTPException,Depends, APIRouter, Query
import pandas as pd
from sqlalchemy import create_engine
from dependencies import get_db
from database import engine, connection_params
from . import models, schemas
from io import BytesIO
import psycopg2


router = APIRouter()

db_dependency = Depends(get_db)


@router.post("/rainfall/daily")
async def import_rainfall_daily_data(file: UploadFile = File(...), db=db_dependency):
    try:
        EXPECTED_COLUMNS = ['country', 'region_name', 'rain_gauge_type', 'weather_station_id', 'temperature', 'total_rainfall', 'wind_speed', 'humidity', 'weather_conditions', 'date_time', 'Duration', 'comments', 'data_source']
        # Check if file is an Excel file
        if not file.filename.endswith(".xlsx"):
            raise HTTPException(status_code=400, detail="File must be in Excel format.")
        
        # Read the Excel file as bytes
        file_contents = await file.read()
        # Create a file-like object
        file_like = BytesIO(file_contents)
        
        # Read the Excel file
        df = pd.read_excel(file_like)
        
        # Check if all expected columns are present
        if not set(EXPECTED_COLUMNS).issubset(df.columns):
            raise HTTPException(status_code=400, detail="Columns mismatch. Make sure all required columns are present.")
        
        # Create or replace the table with the required columns
        df.head(0).to_sql('rainfall_daily_data', engine, if_exists='replace', index=False)
        # Insert data into the PostgreSQL database
        df.to_sql('rainfall_daily_data', engine, if_exists='append', index=False)

        return {"message": f"File uploaded successfully and data inserted into the database."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    





@router.get("/rainfall/daily/grouped_by_year_month_all")
async def get_rainfall_grouped_by_year_month_all(): 
    conn = psycopg2.connect(
                dbname=connection_params["dbname"],
                user=connection_params["user"],
                password=connection_params["password"],
                host=connection_params["host"],
                port=connection_params["port"]
                )
    cursor = conn.cursor()
    try:
         # Construct SQL query to retrieve grouped data
        query = """
            SELECT 
                country,
                region_name,
                EXTRACT(year FROM date_time) AS year,
                EXTRACT(month FROM date_time) AS month,
                SUM(total_rainfall) AS total_rainfall,
                AVG(temperature) AS avg_temperature,
                AVG(wind_speed) AS avg_wind_speed,
                AVG(humidity) AS avg_humidity
            FROM 
                rainfall_daily_data
            GROUP BY 
                country,
                region_name,
                EXTRACT(year FROM date_time),
                EXTRACT(month FROM date_time)
            ORDER BY 
                country,
                region_name,
                year,
                month;
        """
        # Execute SQL query
        cursor.execute(query)
        # Fetch results
        results = cursor.fetchall()
        
        # Convert results to list of dictionaries
        rainfall_data_list = []
        for row in results:
            rainfall_data_list.append({
                "country": row[0],
                "region_name": row[1],
                "year": row[2],
                "month": row[3],
                "total_rainfall": row[4],
                "avg_temperature": row[5],
                "avg_wind_speed": row[6],
                "avg_humidity": row[7]
            })
        
        return rainfall_data_list

    except Exception as e:
                raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    

@router.get("/rainfall/daily/grouped_by_year_month")
async def get_rainfall_daily_grouped_by_year_month(
    country: str = Query(..., description="Country name"),
    region_name: str = Query(..., description="Region name"),
    year: int = Query(..., description="Year as int")
    
 ): 
    conn = psycopg2.connect(
                dbname=connection_params["dbname"],
                user=connection_params["user"],
                password=connection_params["password"],
                host=connection_params["host"],
                port=connection_params["port"]
                )
    cursor = conn.cursor()
    try:
         # Construct SQL query to retrieve grouped data with user-supplied parameters
        query = """
            SELECT 
                country,
                region_name,
                EXTRACT(year FROM date_time) AS year,
                EXTRACT(month FROM date_time) AS month,
                SUM(total_rainfall) AS total_rainfall,
                AVG(temperature) AS avg_temperature,
                AVG(wind_speed) AS avg_wind_speed,
                AVG(humidity) AS avg_humidity
            FROM 
                rainfall_daily_data
            WHERE 
                country = %s AND
                region_name = %s AND
                EXTRACT(year FROM date_time) = %s
            GROUP BY 
                country,
                region_name,
                EXTRACT(year FROM date_time),
                EXTRACT(month FROM date_time)
            ORDER BY 
                country,
                region_name,
                year,
                month;
        """
        # Execute SQL query with user-supplied parameters
        cursor.execute(query, (country, region_name, year))
        # Fetch results
        results = cursor.fetchall()
        
        # Convert results to list of dictionaries
        rainfall_data_list = []
        for row in results:
            rainfall_data_list.append({
                "country": row[0],
                "region_name": row[1],
                "year": row[2],
                "month": row[3],
                "total_rainfall": row[4],
                "avg_temperature": row[5],
                "avg_wind_speed": row[6],
                "avg_humidity": row[7]
            })

        rainfall_data_list_length=  len(rainfall_data_list)
        
        return rainfall_data_list

    except Exception as e:
                raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    
@router.post("/rainfall/monthly")
async def import_rainfall_monthly_data(file: UploadFile = File(...), db=db_dependency):
        try:
            EXPECTED_COLUMNS = ['year','month', 'country', 'region_name','avg_rainfall','avg_temperature','quality_control_flags','comments','data_source']
            # Check if file is an Excel file
            if not file.filename.endswith(".xlsx"):
                raise HTTPException(status_code=400, detail="File must be in Excel format.")
            
            # Read the Excel file as bytes
            file_contents = await file.read()
            # Create a file-like object
            file_like = BytesIO(file_contents)
            
            # Read the Excel file
            df = pd.read_excel(file_like)
            
            # Check if all expected columns are present
            if not set(EXPECTED_COLUMNS).issubset(df.columns):
                raise HTTPException(status_code=400, detail="Columns mismatch. Make sure all required columns are present.")
            
            # Create or replace the table with the required columns
            df.head(0).to_sql('rainfall_monthly_data', engine, if_exists='replace', index=False)
            # Insert data into the PostgreSQL database
            df.to_sql('rainfall_monthly_data', engine, if_exists='append', index=False)

            return {"message": f"File uploaded successfully and data inserted into the database."}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
        

@router.post("/rainfall/yearly")
async def import_rainfall_yearly_data(file: UploadFile = File(...), db=db_dependency):
        try:
            EXPECTED_COLUMNS = ['year', 'country', 'region_name','avg_rainfall','avg_temperature','quality_control_flags','data_source', 'comments']
            # Check if file is an Excel file
            if not file.filename.endswith(".xlsx"):
                raise HTTPException(status_code=400, detail="File must be in Excel format.")
            
            # Read the Excel file as bytes
            file_contents = await file.read()
            # Create a file-like object
            file_like = BytesIO(file_contents)
            
            # Read the Excel file
            df = pd.read_excel(file_like)
            
            # Check if all expected columns are present
            if not set(EXPECTED_COLUMNS).issubset(df.columns):
                raise HTTPException(status_code=400, detail="Columns mismatch. Make sure all required columns are present.")
            
            # Create or replace the table with the required columns
            df.head(0).to_sql('rainfall_annual_data', engine, if_exists='replace', index=False)
            # Insert data into the PostgreSQL database
            df.to_sql('rainfall_annual_data', engine, if_exists='append', index=False)

            return {"message": f"File uploaded successfully and data inserted into the database."}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")




