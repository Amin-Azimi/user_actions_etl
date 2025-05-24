from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class DimUsers(Base):
    __tablename__ = 'dim_users'
    user_key = Column(Integer, primary_key=True)
    user_id = Column(String, nullable=False, unique=True)

    def __repr__(self):
        return f"<DimUsers(user_id='{self.user_id}')>"

class DimActions(Base):
    __tablename__ = 'dim_actions'
    action_key = Column(Integer, primary_key=True, autoincrement=True)
    action_type = Column(String(100), unique=True, nullable=False)

    def __repr__(self):
        return f"<DimActions(action_key={self.action_key}, action_type='{self.action_type}')>"

class DimDevices(Base):
    __tablename__ = 'dim_devices'
    device_key = Column(Integer, primary_key=True, autoincrement=True)
    device_type = Column(String(50), unique=True, nullable=False)

    def __repr__(self):
        return f"<DimDevices(device_key={self.device_key}, device_type='{self.device_type}')>"

class DimLocations(Base):
    __tablename__ = 'dim_locations'
    location_key = Column(Integer, primary_key=True, autoincrement=True)
    location_name = Column(String(100), unique=True, nullable=False)

    def __repr__(self):
        return f"<DimLocations(location_key={self.location_key}, location_name='{self.location_name}')>"

class FactUserActions(Base):
    __tablename__ = 'fact_user_actions'
    action_event_id = Column(Integer, primary_key=True, autoincrement=True)
    user_key = Column(Integer, ForeignKey('dim_users.user_key'), nullable=False)
    action_key = Column(Integer, ForeignKey('dim_actions.action_key'), nullable=False)
    device_key = Column(Integer, ForeignKey('dim_devices.device_key'), nullable=True)
    location_key = Column(Integer, ForeignKey('dim_locations.location_key'), nullable=True)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    def __repr__(self):
        return (
            f"<FactUserActions(action_event_id={self.action_event_id}, "
            f"user_key='{self.user_key}', action_key={self.action_key}, "
            f"timestamp='{self.timestamp}')>"
        )
