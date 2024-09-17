from fastapi import FastAPI, HTTPException, Depends, APIRouter, Path, UploadFile, File
from dependencies import get_db
from database import engine, connect_to_db, execute_query,connection_params
from . import models, schemas
from typing import Optional
from pydantic import BaseModel
from typing import List

import pandas as pd
from io import BytesIO
import numpy as np






db_dependency = Depends(get_db)

router = APIRouter()

@router.get("/crops", response_model=List[schemas.Crop])
def get_crops(crop_id: Optional[int] = None):
    if crop_id:
        query = "SELECT crop_id, crop_name, min_gdd, max_gdd, min_period_days, max_period_days, base_temp FROM public.master_crops WHERE crop_id = %s"
        params = (crop_id,)
    else:
        query = "SELECT crop_id, crop_name, min_gdd, max_gdd, min_period_days, max_period_days, base_temp FROM public.master_crops"
        params = None
    
    try:
        results = execute_query(query, params)
        crops = [schemas.Crop(
            crop_id=row[0],
            crop_name=row[1],
            min_gdd=row[2],
            max_gdd=row[3],
            min_period_days=row[4],
            max_period_days=row[5],
            base_temp=row[6]
        ) for row in results]
        return crops
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

 # Define the route to upload the Excel file
@router.post("/upload-excel/")
async def upload_excel(file: UploadFile = File(...)):
    try:
        # Read the uploaded Excel file into a pandas DataFrame
        df = pd.read_excel(BytesIO(await file.read()), engine='openpyxl')

        # Optional: Inspect and modify the DataFrame if necessary
        # Ensure column names match those in your PostgreSQL table
        # Assuming the Excel columns match the table structure exactly

        # Insert data into the PostgreSQL table
        # Replace '.' with NaN (or directly with None)
        df.replace('.', np.nan, inplace=True)
        df.to_sql('crop_production_india', engine, if_exists='append', index=False)

        return {"status": "success", "message": "Data uploaded successfully"}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




