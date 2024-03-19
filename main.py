from fastapi import FastAPI
from database import engine
from quiz   import main as quiz_main
from quiz   import models as quiz_model
from rainfall   import main as rainfall_main
# import quiz.main  

app = FastAPI()
quiz_model.Base.metadata.create_all(bind=engine)


app.include_router(quiz_main.router)
app.include_router(rainfall_main.router)
