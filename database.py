from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

URL_DATABASE = "postgresql://postgres:postgres@localhost:5432/RDAS"

engine = create_engine(URL_DATABASE)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

connection_params = {
    "dbname": "RDAS",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}

