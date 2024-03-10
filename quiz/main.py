from fastapi import FastAPI, HTTPException, Depends, status, Response, APIRouter
# from pydantic import BaseModel
# from schemas import quiz 
# import models
# import  schemas
from . import models,schemas


from dependencies import get_db 


# from . import schema/quiz

# @app = FastAPI()
# models.Base.metadata.create_all(bind=engine)

router= APIRouter()


db_dependency = Depends(get_db)


@router.get("/question/{question_id}")
async def read_question(question_id: int, db= db_dependency ):
    result= db.query(models.Questions).filter(models.Questions.id==question_id).first()
    if not result:
        raise HTTPException(status_code=404, detail="Question not found ")
    return result
@router.get("/choices/{question_id}")
async def read_choices(question_id: int, db= db_dependency ):
    choices= db.query(models.Choices).filter(models.Choices.question_id==question_id).all()
    if not choices:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Choices not found")
    return choices 

@router.post("/question/", status_code=status.HTTP_201_CREATED)
async def create_questions(question: schemas.QuestionBase, db= db_dependency):
    db_question = models.Questions(question_text=question.question_text)
    db.add(db_question)
    db.commit()
    db.refresh(db_question)
    
    for choice in question.choices:
        db_choices = models.Choices(choice_text=choice.choice_text, is_correct=choice.is_correct, question_id=db_question.id)
        db.add(db_choices)
        db.commit()
    
    return {"message": "Question and choices created successfully"}
