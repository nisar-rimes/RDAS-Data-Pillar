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








def authenticate():
    conn = http.client.HTTPSConnection("dataex.rimes.int")
    dataList = []
    boundary = 'wL36Yn8afVp8Ag7AmP8qZ0SA4n1v9T'

    dataList.append(encode('--' + boundary))
    dataList.append(encode('Content-Disposition: form-data; name=username;'))
    dataList.append(encode('Content-Type: text/plain'))
    dataList.append(encode(''))
    dataList.append(encode("nishanthi@rimes.int"))

    dataList.append(encode('--' + boundary))
    dataList.append(encode('Content-Disposition: form-data; name=password;'))
    dataList.append(encode('Content-Type: text/plain'))
    dataList.append(encode(''))
    dataList.append(encode("Nisha@123"))

    dataList.append(encode('--'+boundary+'--'))
    dataList.append(encode(''))

    body = b'\r\n'.join(dataList)
    headers = {
       'Content-type': 'multipart/form-data; boundary={}'.format(boundary) 
    }

    conn.request("POST", "/user_auth/get_token/", body, headers)
    res = conn.getresponse()
    data = res.read()
    return data.decode("utf-8")

def get_data(token):
    conn = http.client.HTTPSConnection("dataex.rimes.int")
    payload = json.dumps({
      "asset_identifier": "e18fc866-d139-4043-88ba-21847f6dfc26",
      "unique_field": "tehsil_id",
      "reducer": "tmax_daily_tmax_region"
    })
    headers = {
      'Authorization': token,
      'Content-Type': 'application/json'
    }
    conn.request("POST", "/forecast_anls/get_ecmwf_hres_region_data/", payload, headers)
    res = conn.getresponse()
    data = res.read()
    return data.decode("utf-8")

def insert_data_to_db(tehsil_id, date, tmax):
    try:
        conn = psycopg2.connect(
                dbname=connection_params["dbname"],
                user=connection_params["user"],
                password=connection_params["password"],
                host=connection_params["host"],
                port=connection_params["port"]
                )
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO public.temp_data (tehsil_id, tmax, date)
        VALUES (%s, %s, %s)
         """


        cursor.execute(insert_query, (tehsil_id ,tmax, date))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Inserted/Updated data: tehsil_id={tehsil_id}, date={date}, tmax={tmax}")
    except Exception as error:
        print(f"Error inserting data: {error}")
        raise

@router.get("/dataex/fetch_and_store_data/")
def fetch_and_store_data():
    # Step 1: Authenticate and get the token
    auth_response = authenticate()

    # Extract token from the response (this part might need adjustment based on actual response format)
    token = json.loads(auth_response).get('token')

    if not token:
        raise HTTPException(status_code=500, detail="Failed to authenticate, no token received.")

    # Step 2: Use the token to get the data
    data_response = get_data(token)
    data_dict = json.loads(data_response)

    if 'data' not in data_dict:
        raise HTTPException(status_code=500, detail="Unexpected data format received from API.")

    r_data = data_dict['data'].get('r_data', {})
    
    # Iterate over the keys in r_data (since the key is dynamic)
    for key in r_data:
        if 'time' in r_data[key] and 'value' in r_data[key]:
            # Iterate over the time and value fields together
            for time_period, value in zip(r_data[key]['time'], r_data[key]['value']):
                # Extract the date from the 'time' field and the corresponding value
                date = time_period[0][:10]
                tmax = value
                tehsil_id = key

                # Insert data into the database
                insert_data_to_db(tehsil_id, date, tmax)
        else:
            raise HTTPException(status_code=500, detail=f"No 'time' or 'value' field found for key: {key}")

    return {"status": "success", "message": "Data fetched and stored successfully ."}










    











  
  





    




    










        








