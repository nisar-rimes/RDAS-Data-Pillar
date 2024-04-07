from fastapi import FastAPI, UploadFile, File, HTTPException,Depends, APIRouter
import pandas as pd
from sqlalchemy import create_engine
from dependencies import get_db
from database import engine
from . import models, schemas
from io import BytesIO


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




