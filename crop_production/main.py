from fastapi import FastAPI, HTTPException, Depends, APIRouter, Path
from dependencies import get_db
from database import engine, connect_to_db, execute_query,connection_params
from . import models, schemas
from typing import Optional
from pydantic import BaseModel
from typing import List
import pandas as pd






db_dependency = Depends(get_db)

router = APIRouter()





@router.get("/crop_production", response_model=List[schemas.CropProduction])
def get_crop_production(
    district_id: Optional[int] = None,
    province_id: Optional[str] = None,
    crop_id: Optional[int] = None
):
    query = """
    SELECT crop_production_id, crop_id, district_id, crop_area, crop_production, crop_yield, 
           crop_production_period, province_id, province 
    FROM public.crop_production 
    """
    filters = []
    params = []

    if district_id:
        filters.append("district_id = %s")
        params.append(district_id)
    if province_id:
        filters.append("province_id = %s")
        params.append(province_id)
    if crop_id:
        filters.append("crop_id = %s")
        params.append(crop_id)

    if filters:
        query += " WHERE " + " AND ".join(filters)
    

    try:
        results = execute_query(query, tuple(params))
        crop_production_list = [schemas.CropProduction(
            crop_production_id=row[0],
            crop_id=row[1],
            district_id=row[2],
            crop_area=row[3],
            crop_production=row[4],
            crop_yield=row[5],
            crop_production_period=row[6],
            province_id=row[7],
            province=row[8]
        ) for row in results]
        return crop_production_list
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




