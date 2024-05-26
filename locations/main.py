from fastapi import FastAPI, HTTPException, Depends, APIRouter, Path
from dependencies import get_db
from database import engine, connect_to_db, execute_query
from . import models, schemas
from typing import Optional
from pydantic import BaseModel
from typing import List
import psycopg2

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





    











  
  





    




    










        








