from fastapi import FastAPI
from database import engine
from quiz   import main as quiz_main
from quiz   import models as quiz_model
from rainfall   import main as rainfall_main
from elnino import main as elnino_main
from locations import main as locations_main
from crops import main as crops_main
from crop_production import main as crop_production_main
# import quiz.main  
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()


origins = [
    "http://localhost",
    "http://localhost:8000",  # Example: React development server
    # Add other origins as needed
]

# app.add_middleware(
#     CORSMiddleware,
#     # allow_origins=origins,
#     allow_origins=["*"],  # Allow requests from any origin
#     allow_credentials=True,
#     allow_methods=["GET", "POST", "PUT", "DELETE"],
#     allow_headers=["*"],
# )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust this list to specify which origins are allowed
    allow_credentials=True,
    allow_methods=["*"],  # Specify the methods you want to allow, e.g., ["GET", "POST"]
    allow_headers=["*"],  # Specify the headers you want to allow
)



quiz_model.Base.metadata.create_all(bind=engine)


app.include_router(quiz_main.router)
app.include_router(rainfall_main.router)
app.include_router(elnino_main.router)
app.include_router(locations_main.router)
app.include_router(crops_main.router)
app.include_router(crop_production_main.router)
