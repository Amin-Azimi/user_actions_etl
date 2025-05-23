from sqlalchemy.orm import Session
from database.model import FactUserAction
from scripts.utils.db import get_engine, get_session

def load_data(cleaned_data):
    engine = get_engine()
    with get_session(engine) as session:
        for entry in cleaned_data:
            action = FactUserAction(**entry)
            session.add(action)
        session.commit()