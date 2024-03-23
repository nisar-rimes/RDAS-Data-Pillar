from  sqlalchemy import Boolean, Column, Integer, ForeignKey, String,TIMESTAMP 
from database import Base
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
# from typing import Annotated

# class Questions(Base):
#     __tablename__ = 'questions'
#     id = Column(Integer, primary_key=True, index=True)
#     question_text=Column(String , index=True)

# class Choices(Base):
#     __tablename__ = 'choices'
#     id = Column (Integer , primary_key =True, index=True)
#     choice_text= Column(String , index=True)
#     is_correct= Column(Boolean, default = False)
#     question_id= Column(Integer , ForeignKey("questions.id"), index=True)



class Rainfalldata(Base):
    __tablename__ = 'rainfalldata'
    id = Column (Integer , primary_key =True, index=True)
    date= Column(TIMESTAMP , index=True)
    longitude= Column(DOUBLE_PRECISION, index=True)
    latitude= Column(DOUBLE_PRECISION, index=True)
    rainfall= Column(DOUBLE_PRECISION, index=True)
    


 