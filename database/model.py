from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class DimUser(Base):
    __tablename__ = 'dim_users'
    user_key = Column(Integer, primary_key=True)
    user_id = Column(String, nullable=False, unique=True)

class DimAction(Base):
    __tablename__ = 'dim_actions'
    action_key = Column(Integer, primary_key=True)
    action_type = Column(String, nullable=False, unique=True)

class FactUserAction(Base):
    __tablename__ = 'fact_user_actions'
    user_action_key = Column(Integer , primary_key=True)
    user_key = Column(Integer, ForeignKey('dim_users.user_key'), nullable=False)
    action_key = Column(Integer, ForeignKey('dim_actions.action_key'), nullable=False)
    timestamp = Column(String, nullable=False)
    device = Column(String)
    location = Column(String)