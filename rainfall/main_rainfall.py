from fastapi import HTTPException,Depends, APIRouter
from dependencies import get_db
import requests
from pydantic import BaseModel
from typing import List




router1 = APIRouter()
db_dependency = Depends(get_db)




class DataRequest(BaseModel):
    source: str
    indic: str
    period: str
    country: str
    district: List[str]
    start_date: str
    end_date: str
    cache: bool

class DataResponse(BaseModel):
    metadata: dict
    data: List[dict]

@router1.post("/data/get", response_model=DataResponse)
async def get_data(request: DataRequest):
    url = "https://api.rdas.live/data/get"
    payload = request.model_dump()
    
    response = requests.post(url, json=payload)
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Failed to fetch data")
    
    data = response.json()
    
    # Process data to separate date and month into separate objects
    for entry in data['data']:
        entry['date'] = {
            'day': int(entry['date'].split('-')[2]),
            'month': {
                'number': int(entry['date'].split('-')[1]),
                'name': {
                    'short': entry['date'].split('-')[1],
                    'full': get_month_name(entry['date'].split('-')[1])
                }
            },
            'year': int(entry['date'].split('-')[0])
        }
        del entry['date']

    return data

def get_month_name(month_number):
    month_names = {
        '01': 'January',
        '02': 'February',
        '03': 'March',
        '04': 'April',
        '05': 'May',
        '06': 'June',
        '07': 'July',
        '08': 'August',
        '09': 'September',
        '10': 'October',
        '11': 'November',
        '12': 'December'
    }
    return month_names[month_number]









