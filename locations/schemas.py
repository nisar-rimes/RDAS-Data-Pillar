from pydantic import BaseModel

class Province(BaseModel):
    province_id: str
    province: str

class District(BaseModel):
    province_id: str
    province: str
    district_id: str
    district: str

class Tehsil(BaseModel):
    province_id: str
    province: str
    district_id: str
    district: str
    tehsil_id: str
    tehsil: str
    region_type: str
    region: str