from pydantic import BaseModel
from typing import  Optional

class CropProduction(BaseModel):
    crop_production_id: int
    crop_id: int
    district_id_: Optional[str]
    crop_area: float
    crop_production: float
    crop_yield: float
    crop_production_period: int
    # district_id_: Optional[str]
    province_id: Optional[str]
    province: Optional[str]