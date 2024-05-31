from fastapi import FastAPI, HTTPException, Depends, APIRouter, Path
from dependencies import get_db
from database import engine, connect_to_db, execute_query,connection_params
from . import models, schemas
from typing import Optional
from pydantic import BaseModel
from typing import List
import psycopg2
import http.client
from codecs import encode
import json
import time
from psycopg2.extras import RealDictCursor



from sqlalchemy.sql import text 



db_dependency = Depends(get_db)

router = APIRouter()




@router.get("/pakistan/provinces", response_model=List[schemas.Province])
def read_provinces():
    sql_query = "SELECT province_id, province FROM public.master_province"
    raw_results = execute_query(sql_query)

    # Transform raw results into a list of Province objects
    results = [schemas.Province(province_id=row[0], province=row[1]) for row in raw_results]

    return results

@router.get("/pakistan/get_districts_by_province/{province_id}", response_model=List[schemas.District])
def get_district_by_province_id(province_id: str = Path(..., description="The ID of the province to retrieve districts for")):
    sql_query = "SELECT province_id, province, district_id, district FROM public.master_district WHERE province_id = %s"
    raw_results = execute_query(sql_query, (province_id,))

    if not raw_results:
        raise HTTPException(status_code=404, detail="No districts found for this province")

    districts = [schemas.District(province_id=row[0], province=row[1], district_id=row[2], district=row[3]) for row in raw_results]

    return districts


@router.get("/pakistan/get_tehsil_by_district_id/{district_id}", response_model=List[schemas.Tehsil])
def get_tehsil_by_district_id(district_id: str = Path(..., description="The ID of the district to retrieve tehsils for")):
    sql_query = "SELECT province_id, province, district_id, district, tehsil_id, tehsil, region_type, region FROM public.master_tehsil WHERE district_id = %s"
    raw_results = execute_query(sql_query, (district_id,))

    if not raw_results:
        raise HTTPException(status_code=404, detail="No tehsils found for this district")

    tehsils = [schemas.Tehsil(
        province_id=row[0],
        province=row[1],
        district_id=row[2],
        district=row[3],
        tehsil_id=row[4],
        tehsil=row[5],
        region_type=row[6],
        region=row[7]
    ) for row in raw_results]

    return tehsils





@router.get("/pakistan/provinces/{province_id}", response_model=schemas.Province)
def read_province_by_id(province_id: str = Path(..., description="The ID of the province to retrieve")):
    sql_query = "SELECT province_id, province FROM public.master_province WHERE province_id = %s"
    raw_results = execute_query(sql_query, (province_id,))

    if not raw_results:
        raise HTTPException(status_code=404, detail="Province not found")

    # Transform the raw result into a Province object
    province = schemas.Province(province_id=raw_results[0][0], province=raw_results[0][1])

    return province







def get_db_connection():
    return psycopg2.connect(
                dbname=connection_params["dbname"],
                user=connection_params["user"],
                password=connection_params["password"],
                host=connection_params["host"],
                port=connection_params["port"]

    )

def authenticate():
    conn = http.client.HTTPSConnection("dataex.rimes.int")
    boundary = 'wL36Yn8afVp8Ag7AmP8qZ0SA4n1v9T'
    
    body = f"--{boundary}\r\n"
    body += 'Content-Disposition: form-data; name="username";\r\n\r\nnishanthi@rimes.int\r\n'
    body += f"--{boundary}\r\n"
    body += 'Content-Disposition: form-data; name="password";\r\n\r\nNisha@123\r\n'
    body += f"--{boundary}--\r\n"

    headers = {
        'Content-type': f'multipart/form-data; boundary={boundary}'
    }

    conn.request("POST", "/user_auth/get_token/", body.encode(), headers)
    res = conn.getresponse()
    data = res.read()
    return data.decode("utf-8")

def get_data(token: str, reducer: str):
    conn = http.client.HTTPSConnection("dataex.rimes.int")
    payload = json.dumps({
        "asset_identifier": "e18fc866-d139-4043-88ba-21847f6dfc26",
        "unique_field": "tehsil_id",
        "reducer": reducer
    })
    headers = {
        'Authorization': token,
        'Content-Type': 'application/json'
    }
    conn.request("POST", "/forecast_anls/get_ecmwf_hres_region_data/", payload, headers)
    res = conn.getresponse()
    data = res.read()
    return data.decode("utf-8")

def execute_db_query(query: str, params: tuple):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as error:
        print(f"Database error: {error}")
        raise

    


def insert_data_to_db(tehsil_id: str, date: str, tmax: float):
    # Insert or update based on existence
    query = """
        INSERT INTO public.temp_data (tehsil_id, tmax, date)
        VALUES (%s, %s, %s)
    """
    execute_db_query(query, (tehsil_id, tmax, date))
    print(f"Inserted/Updated data: tehsil_id={tehsil_id}, date={date}, tmax={tmax}")

def update_data_to_db(tehsil_id: str, date: str, tmin: float):
    query = """
        UPDATE public.temp_data
        SET tmin = %s
        WHERE tehsil_id = %s AND date = %s
    """
    execute_db_query(query, (tmin, tehsil_id, date))
    # print(f"Updated data: tehsil_id={tehsil_id}, date={date}, tmin={tmin}")

def process_data(auth_response: str, reducer: str, update: bool = False):
    token = json.loads(auth_response).get('token')
    if not token:
        raise HTTPException(status_code=500, detail="Failed to authenticate, no token received.")
    
    data_response = get_data(token, reducer)
    data_dict = json.loads(data_response)

    if 'data' not in data_dict:
        raise HTTPException(status_code=500, detail="Unexpected data format received from API.")

    r_data = data_dict['data'].get('r_data', {})
    
    for key in r_data:
        if 'time' in r_data[key] and 'value' in r_data[key]:
            for time_period, value in zip(r_data[key]['time'], r_data[key]['value']):
                date = time_period[0][:10]
                tehsil_id = key
                if update:
                    update_data_to_db(tehsil_id, date, value)
                else:
                    insert_data_to_db(tehsil_id, date, value)
        else:
            raise HTTPException(status_code=500, detail=f"No 'time' or 'value' field found for key: {key}")

@router.get("/dataex/fetch_and_store_data/")
def fetch_and_store_data():
    start_time = time.time()  # Get current time before execution
    auth_response = authenticate()
    process_data(auth_response, 'tmax_daily_tmax_region')
    process_data(auth_response, 'tmin_daily_tmin_region', update=True)
    
    end_time = time.time()  
    total_time = end_time - start_time 
    return {"status": "success", "message": f"Data fetched and stored successfully. Total time taken: {total_time} seconds."}



def execute_query1(query, params=None):
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(query, params)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result 

 
@router.get("/dataex/get_data_temp_data", response_model=List[schemas.TempData])
def get_data_temp_data(tehsil_id: Optional[str] = None):
    try:
        if tehsil_id:
            query = "SELECT tehsil_id, tmax, tmin, date FROM public.temp_data WHERE tehsil_id = %s"
            params = (tehsil_id,)
        else:
            query = "SELECT tehsil_id, tmax, tmin, date FROM public.temp_data"
            params = None
        
        rows = execute_query1(query, params)
        data = [
            schemas.TempData(
                tehsil_id=row['tehsil_id'],  
                tmax=row['tmax'],
                tmin=row['tmin'] if row['tmin'] is not None else None,
                date=row['date'].isoformat() if 'date' in row else None 
            ) for row in rows
        ]
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))







