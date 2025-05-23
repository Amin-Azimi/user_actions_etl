from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from scripts.config import DATABASE_URL

def get_engine():
    return create_engine(DATABASE_URL)

def get_session(engine):
    session = sessionmaker(bind=engine)
    return session()
