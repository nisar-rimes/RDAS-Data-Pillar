from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from psycopg2 import sql
import psycopg2
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine



#URL_DATABASE = "postgresql://postgres:postgres@localhost:5432/RDAS"
URL_DATABASE = "postgresql://postgres:postgres@203.156.108.67:14543/RDAS"


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


# connection_params = {
#     "dbname": "RDAS",
#     "user": "postgres",
#     "password": "postgres",
#     "host": "203.156.108.67",
#     "port": "14543"
# }


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def connect_to_db():
    try:
        conn = psycopg2.connect(
                dbname=connection_params["dbname"],
                user=connection_params["user"],
                password=connection_params["password"],
                host=connection_params["host"],
                port=connection_params["port"]
        )
        return conn
    
    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL database:", e)
        return None
    

    # Function to execute SQL query
def execute_query(query,  params=None):
    conn = connect_to_db()
    cursor = conn.cursor()
    cursor.execute(query, params)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result

# def execute_query(query, params=None):
#     with  connect_to_db() as conn:
#         with conn.cursor() as cursor:
#             cursor.execute(query, params)
#             results = cursor.fetchall()
#             return results






