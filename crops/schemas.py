from pydantic import BaseModel
from typing import  Optional

class Crop(BaseModel):
    crop_id: int
    crop_name: str
    min_gdd: int
    max_gdd: int
    min_period_days: int
    max_period_days: int
    base_temp: int