from pydantic import BaseModel
from typing import Optional
from datetime import datetime




# class ChoiceBase(BaseModel):
#         choice_text:str
#         is_correct:bool

# class QuestionBase(BaseModel):
#         question_text:str
#         choices:List[ChoiceBase]

# Pydantic model 
class RainfallDataBase(BaseModel):
        id: Optional[int]
        date: datetime
        longitude: float
        latitude: float
        rainfall: float

class WeatherData(BaseModel):
    country: str
    region_name: str
    region_type: str
    temperature: float
    rainfall: float
    wind_speed: float
    date: datetime
    duration_mins: int
    data_source: str