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
class rainfall_dataBase(BaseModel):
        id: Optional[int]
        date: datetime
        longitude: float
        latitude: float
        rainfall: float