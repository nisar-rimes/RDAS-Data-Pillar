from fastapi import FastAPI, UploadFile, File, HTTPException,Depends, APIRouter, Query
import pandas as pd
from sqlalchemy import create_engine
from dependencies import get_db
from database import engine, connection_params
from . import models, schemas
from io import BytesIO
import psycopg2
from typing import Optional
import json



router = APIRouter()







db_dependency = Depends(get_db)


@router.post("/rainfall/daily")
async def import_rainfall_daily_data(file: UploadFile = File(...), db=db_dependency):
    try:
        EXPECTED_COLUMNS = ['country', 'region_name','region_type', 'temperature', 'rainfall', 'wind_speed', 'date', 'duration_mins', 'data_source']
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
    

# Function to establish PostgreSQL connection
def connect_to_db():
    try:
        conn = psycopg2.connect(
                dbname=connection_params["dbname"],
                user=connection_params["user"],
                password=connection_params["password"],
                host=connection_params["host"],
                port=connection_params["port"]
        )
        return conn
    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL database:", e)



@router.get("/rainfall/daily/grouped_by_year_month")
async def get_rainfall_daily_grouped_by_year_month(
    country: str = Query(..., description="Country name"),
    region_name: str = Query(..., description="Region name"),
    year: int = Query(..., description="Year as int")
    
 ): 
    conn = connect_to_db()
    cursor = conn.cursor()
    try:
         # Construct SQL query to retrieve grouped data with user-supplied parameters
        query = """
            SELECT 
                country,
                region_name,
                EXTRACT(year FROM date) AS year,
                EXTRACT(month FROM date) AS month,
                AUM(rainfall) AS rainfall,
                AVG(temperature) AS avg_temperature,
                AVG(wind_speed) AS avg_wind_speed
                
            FROM 
                rainfall_daily_data
            WHERE 
                country = %s AND
                region_name = %s AND
                EXTRACT(year FROM date) = %s
            GROUP BY 
                country,
                region_name,
                EXTRACT(year FROM date),
                EXTRACT(month FROM date)
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
                "avg_rainfall": row[4],
                "avg_temperature": row[5],
                "avg_wind_speed": row[6]
                
            })

        rainfall_data_list_length =  len(rainfall_data_list)
        
        return rainfall_data_list

    except Exception as e:
                raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    
@router.post("/rainfall/monthly")
async def import_rainfall_monthly_data(file: UploadFile = File(...), db=db_dependency):
        try:
            EXPECTED_COLUMNS = ['year','month', 'country', 'region_name','avg_rainfall','avg_temperature','data_source']
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
            EXPECTED_COLUMNS = ['year', 'country', 'region_name','avg_rainfall','avg_temperature','quality_control_flags','data_source']
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
        

# Function to execute SQL query
def execute_query(query):
    conn = connect_to_db()
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result

@router.get("/rainfall/weather_data_daily", response_model=list)
def get_weather_data(
    country: Optional[str] = Query(None),
    region_name: Optional[str] = Query(None),
    region_type: Optional[str] = Query(None),
    # temperature: Optional[float] = Query(None),rainfall: Optional[float] = Query(None), wind_speed: Optional[float] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    # duration_mins: Optional[int] = Query(None), data_source: Optional[str] = Query(None)
):
    # Build the SQL query based on provided parameters
    sql_query = "SELECT * FROM rainfall_daily_data WHERE TRUE"

    if country:
        sql_query += f" AND country = '{country}'"
    if region_name:
        sql_query += f" AND region_name = '{region_name}'"
    if region_type:
        sql_query += f" AND region_type = '{region_type}'"
    # if temperature is not None:
    #     sql_query += f" AND temperature = {temperature}"
    # if rainfall is not None:
    #     sql_query += f" AND rainfall = {rainfall}"
    # if wind_speed is not None:
    #     sql_query += f" AND wind_speed = {wind_speed}"
    if start_date and end_date:
        sql_query += f" AND date >= '{start_date}' AND date <= '{end_date}'"
    # if duration_mins is not None:
    #     sql_query += f" AND duration_mins = {duration_mins}"
    # if data_source:
    #     sql_query += f" AND data_source = '{data_source}'"
    # Execute the SQL query
    result = execute_query(sql_query)
    result_list = []
    for row in result:
        result_list.append({"country": row[0], "region_name": row[1],"region_type": row[2],"temperature": row[3], "rainfall": row[4],"wind_speed": row[5],
            "date": row[6], "duration_mins": row[7], "data_source": row[8]
        })
    return result_list


@router.get("/rainfall/weather_data_daily_comparison", response_model=dict  )
def get_weather_data(
    region_type: str,
    region_name1: str,
    region_name2: str,
    country: Optional[str] = Query(None),
    # temperature: Optional[float] = Query(None),
    # rainfall: Optional[float] = Query(None),
    # wind_speed: Optional[float] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    duration_mins: Optional[int] = Query(None),
    data_source: Optional[str] = Query(None)
):
    
    try:
        # Build the SQL query for region_name1
        sql_query1 = f"SELECT * FROM rainfall_daily_data WHERE region_type = '{region_type}' AND region_name = '{region_name1}'"

        # Build the SQL query for region_name2
        sql_query2 = f"SELECT * FROM rainfall_daily_data WHERE region_type = '{region_type}' AND region_name = '{region_name2}'"

        # Append additional filters based on provided parameters
        if country:
            sql_query1 += f" AND country = '{country}'"
            sql_query2 += f" AND country = '{country}'"
        # if temperature is not None:
        #     sql_query1 += f" AND temperature = {temperature}"
        #     sql_query2 += f" AND temperature = {temperature}"
        # if rainfall is not None:
        #     sql_query1 += f" AND rainfall = {rainfall}"
        #     sql_query2 += f" AND rainfall = {rainfall}"
        # if wind_speed is not None:
        #     sql_query1 += f" AND wind_speed = {wind_speed}"
        #     sql_query2 += f" AND wind_speed = {wind_speed}"
        if start_date and end_date:
            sql_query1 += f" AND date >= '{start_date}' AND date <= '{end_date}'"
            sql_query2 += f" AND date >= '{start_date}' AND date <= '{end_date}'"
        if duration_mins is not None:
            sql_query1 += f" AND duration_mins = {duration_mins}"
            sql_query2 += f" AND duration_mins = {duration_mins}"
        if data_source:
            sql_query1 += f" AND data_source = '{data_source}'"
            sql_query2 += f" AND data_source = '{data_source}'"

        # Add ORDER BY clause to both queries
        sql_query1 += " ORDER BY date"
        sql_query2 += " ORDER BY date" 

        # Execute the SQL queries
        result1 = execute_query(sql_query1)
        result2 = execute_query(sql_query2)
        
        # Convert the results to lists of dictionaries  schemas.WeatherData
        result_list1 = []
        for row in result1:
            result_list1.append(schemas.WeatherData(
                country=row[0],
                region_name=row[1],
                region_type=row[2],
                temperature=row[3],
                rainfall=row[4],
                wind_speed=row[5],
                date=row[6],
                duration_mins=row[7],
                data_source=row[8]
            ))
        
        result_list2 = []
        for row in result2:
            result_list2.append(schemas.WeatherData(
                country=row[0],
                region_name=row[1],
                region_type=row[2],
                temperature=row[3],
                rainfall=row[4],
                wind_speed=row[5],
                date=row[6],
                duration_mins=row[7],
                data_source=row[8]
            ))

        return {"result1": result_list1, "result2": result_list2}
    except Exception as e:
            raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")









@router.get("/rainfall/weather_data_grouped", response_model=dict)
def get_weather_data_grouped(
    region_name: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    month: bool = Query(False),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None)
):
    try:
        # Base SQL query
        sql_query = "SELECT EXTRACT(YEAR FROM date) AS year"
        if month:
            sql_query += ", EXTRACT(MONTH FROM date) AS month"
        if region_name:
            sql_query += ", region_name"
        if country:
            sql_query += ", country"
        
        sql_query += ", AVG(rainfall) AS avg_rainfall, AVG(temperature) AS avg_temperature, AVG(wind_speed) AS avg_wind_speed FROM rainfall_daily_data"  

        # Append additional filters based on provided parameters
        where_clause = []
        if country:
            where_clause.append(f"country = '{country}'")
        if start_date and end_date:
            where_clause.append(f"date BETWEEN '{start_date}' AND '{end_date}'")
        elif start_date:
            where_clause.append(f"date >= '{start_date}'")
        elif end_date:
            where_clause.append(f"date <= '{end_date}'")
        if region_name:
            where_clause.append(f"region_name = '{region_name}'")

        if where_clause:
            sql_query += " WHERE " + " AND ".join(where_clause)

        # Group by year, month, and region_name
        group_by_clause = ["EXTRACT(YEAR FROM date)"]
        if month:
            group_by_clause.append("EXTRACT(MONTH FROM date)")
        if region_name:
            group_by_clause.append("region_name")
        if country:
            group_by_clause.append("country")
        sql_query += f" GROUP BY {', '.join(group_by_clause)} ORDER BY year"
        
        # Execute the SQL query
        result = execute_query(sql_query)

        # Check if the result set is empty
        if not result:
            return {"grouped_weather_data": []}

        # Convert the result to a list of dictionaries
        result_list = []
        for row in result:
            result_dict = {}
            column_index = 0  # Initialize the column index
            if month:
                result_dict["month"] = int(row[column_index])
                column_index += 1
            result_dict["year"] = int(row[column_index])
            column_index += 1
            if region_name:
                result_dict["region_name"] = row[column_index]
                column_index += 1
            if country:
                result_dict["country"] = row[column_index]
                column_index += 1
            result_dict["avg_rainfall"] = float(row[column_index])
            result_dict["avg_temperature"] = float(row[column_index + 1])
            result_dict["avg_wind_speed"] = float(row[column_index + 2])

            result_list.append(result_dict)

        return {"grouped_weather_data": result_list}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

        








