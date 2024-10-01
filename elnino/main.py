from fastapi import FastAPI, UploadFile, File, HTTPException,Depends, APIRouter, Query
import pandas as pd
from sqlalchemy import create_engine
from dependencies import get_db
from database import engine, connect_to_db, execute_query,connection_params
from . import models, schemas
from io import BytesIO
import psycopg2
from typing import Optional
import requests
from pydantic import BaseModel
from typing import List
from commen_methods import get_month_name
import json
import calendar
import os
from io import StringIO
from psycopg2.extras import RealDictCursor


db_dependency = Depends(get_db)

router = APIRouter()

class DataRequest(BaseModel):
    source: str
    indic: str
    period: str
    year: str

class DataItem(BaseModel):
    year: int
    month: int
    month_name: str
    value: float

class DataResponse(BaseModel):
    data: List[DataItem]

def process_data(year: str) -> List[DataItem]:
    # Load the data
    current_dir = os.path.dirname(os.path.abspath(__file__))
        # Construct the file path
    file_path = os.path.join(current_dir, 'elnano_data.xlsx')
    df = pd.read_excel(file_path)

    # Drop rows with NaN values
    df_cleaned = df.dropna()

    def season_to_months(year, season, value):
        season_mapping = {
            'DJF': [(year - 1, 12), (year, 1), (year, 2)],
            'JFM': [(year, 1), (year, 2), (year, 3)],
            'FMA': [(year, 2), (year, 3), (year, 4)],
            'MAM': [(year, 3), (year, 4), (year, 5)],
            'AMJ': [(year, 4), (year, 5), (year, 6)],
            'MJJ': [(year, 5), (year, 6), (year, 7)],
            'JJA': [(year, 6), (year, 7), (year, 8)],
            'JAS': [(year, 7), (year, 8), (year, 9)],
            'ASO': [(year, 8), (year, 9), (year, 10)],
            'SON': [(year, 9), (year, 10), (year, 11)],
            'OND': [(year, 10), (year, 11), (year, 12)],
            'NDJ': [(year, 11), (year, 12), (year + 1, 1)],
        }
        return [(y, m, value) for (y, m) in season_mapping[season]]

    # Prepare the data for insertion
    data = []
    for _, row in df_cleaned.iterrows():
        row_year = row['Year']
        for season in df_cleaned.columns[1:]:
            value = row[season]
            if pd.notna(value):  # Use pd.notna() to check for NaN values
                season_data = season_to_months(row_year, season, value)
                data.extend(season_data)  # Ensure extending with the correct list of tuples

    # Flatten the data list
    flat_data = []
    for sublist in data:
        if isinstance(sublist, list):
            flat_data.extend(sublist)
        else:
            flat_data.append(sublist)  # Handle the case where sublist is not iterable

    # Convert flat_data to a list of DataItem objects
    json_data = [
        DataItem(
            year=int(y),
            month=m,
            month_name=calendar.month_abbr[m],
            value=v
        )
        for (y, m, v) in flat_data
    ]

    # Filter data based on the year
    if year != "all":
        json_data = [entry for entry in json_data if entry.year == int(year)]

    return json_data

@router.post("/elnano/data", response_model=DataResponse)
async def get_data(request: DataRequest):
    data = process_data(request.year)
    if not data:
        raise HTTPException(status_code=404, detail="Data not found")
    return DataResponse(data=data)










# Model for request validation
class ElNinoData(BaseModel):
    SEAS: str
    YR: int
    TOTAL: float
    ANOM: float

    # Model for request validation
    #space 
    #space 
  



@router.get("/elnano/insert_data")

async def insert_data():
    # Fetch data from URL
    url = 'https://origin.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt'
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    
    # Read data into a DataFrame
    data = pd.read_csv(StringIO(response.text), delim_whitespace=True, names=['SEAS', 'YR', 'TOTAL', 'ANOM'], skiprows=1)
    
    # Establish database connection
    conn = connect_to_db()


  
    
     # Establish database connection
    conn = connect_to_db()
    cur = conn.cursor()

    # Insert data into table
    for index, row in data.iterrows():
        cur.execute("""
            INSERT INTO el_nino (SEAS, YR, TOTAL, ANOM)
            VALUES (%s, %s, %s, %s)
        """, (row['SEAS'], row['YR'], row['TOTAL'], row['ANOM']))
    
    conn.commit()
    cur.close()
    conn.close()
    
    return {"message": "Data inserted successfully"}



def fetch_years(query: str) -> List[int]:
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute(query)
    years = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return years

@router.get("/elnino/{classification}")
async def get_elnino_years(classification: str):
    # Define SQL queries for each classification
    queries = {
        'Weak': """
            SELECT DISTINCT YR
            FROM el_nino e1
            WHERE ANOM >= 0.5 AND ANOM < 0.9
            AND NOT EXISTS (
                SELECT 1
                FROM el_nino e2
                WHERE e2.YR = e1.YR
                AND (e2.ANOM >= 0.9)
            )
            ORDER BY YR;
        """,
        'Moderate': """
            SELECT DISTINCT YR
                FROM el_nino e1
                WHERE ANOM >= 1.0 AND ANOM < 1.5
                AND NOT EXISTS (
                    SELECT 1
                    FROM el_nino e2
                    WHERE e2.YR = e1.YR
                    AND e2.ANOM >= 1.5
                )
                ORDER BY YR;
        """,
        'Strong': """
            SELECT DISTINCT YR
            FROM el_nino
            WHERE ANOM >= 1.5
            ORDER BY YR;
        """,
        'Neutral': """
            SELECT DISTINCT YR
            FROM el_nino e1
            WHERE ANOM >= -0.4 AND ANOM <= 0.4
            AND NOT EXISTS (
                SELECT 1
                FROM el_nino e2
                WHERE e2.YR = e1.YR
                AND e2.ANOM > 0.4
            )
            ORDER BY YR;
        """
    }



    # Check if the classification is valid
    if classification not in queries:
        raise HTTPException(status_code=400, detail="Invalid classification")

    # Fetch and return years based on the classification
    query = queries[classification]
    years = fetch_years(query)
    # years=execute_query(query)
    
    # return {f"{classification} El Niño years": years}
    return {
            "classification": classification,
            "total_years": len(years),
            "Years": years
        }

  
  





    




    










        








