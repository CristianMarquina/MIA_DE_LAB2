from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Driver(Base):
    __tablename__ = 'dim_drivers'
    driver_id = Column(Integer, primary_key=True)
    driver_ref = Column(String(255))
    full_name = Column(String(255))
    dob = Column(Date)
    nationality = Column(String(255))

class Constructor(Base):
    __tablename__ = 'dim_constructors'
    constructor_id = Column(Integer, primary_key=True)
    constructor_ref = Column(String(255))
    name = Column(String(255))
    nationality = Column(String(255))

class Circuit(Base):
    __tablename__ = 'dim_circuits'
    circuit_id = Column(Integer, primary_key=True)
    circuit_ref = Column(String(255))
    name = Column(String(255))
    location = Column(String(255))
    country = Column(String(255))

class Race(Base):
    __tablename__ = 'dim_races'
    race_id = Column(Integer, primary_key=True)
    year = Column(Integer)
    round = Column(Integer)
    circuit_id = Column(Integer, ForeignKey('dim_circuits.circuit_id'))
    name = Column(String(255))
    date = Column(Date)
    time = Column(String(255))

class Status(Base):
    __tablename__ = 'dim_status'
    status_id = Column(Integer, primary_key=True)
    status_description = Column(String(255))

class Qualifying(Base):
    __tablename__ = 'fact_qualifying'
    qualify_id = Column(Integer, primary_key=True)
    race_id = Column(Integer, ForeignKey('dim_races.race_id'))
    driver_id = Column(Integer, ForeignKey('dim_drivers.driver_id'))
    constructor_id = Column(Integer, ForeignKey('dim_constructors.constructor_id'))
    position = Column(Integer)
    q1_time_ms = Column(Integer)
    q2_time_ms = Column(Integer)
    q3_time_ms = Column(Integer)

class PitStop(Base):
    __tablename__ = 'fact_pit_stops'
    pit_stop_id = Column(Integer, primary_key=True)
    race_id = Column(Integer, ForeignKey('dim_races.race_id'))
    driver_id = Column(Integer, ForeignKey('dim_drivers.driver_id'))
    constructor_id = Column(Integer, ForeignKey('dim_constructors.constructor_id'))
    lap = Column(Integer)
    stop_number = Column(Integer)
    duration_ms = Column(Integer)

class RaceResult(Base):
    __tablename__ = 'fact_race_results'
    result_id = Column(Integer, primary_key=True)
    race_id = Column(Integer, ForeignKey('dim_races.race_id'))
    driver_id = Column(Integer, ForeignKey('dim_drivers.driver_id'))
    constructor_id = Column(Integer, ForeignKey('dim_constructors.constructor_id'))
    status_id = Column(Integer, ForeignKey('dim_status.status_id'))
    position = Column(Integer)
    grid = Column(Integer)
    positions_gained = Column(Float)
    points = Column(Float)
    laps = Column(Integer)
    milliseconds = Column(Float)
    fastest_lap = Column(Integer)
    rank = Column(Integer)
    fastest_lap_time_ms = Column(Integer)
    fastest_lap_speed = Column(Float)

def create_all_tables(engine):
    print("Dropping all existing tables...")
    Base.metadata.drop_all(engine)
    print("Creating all tables...")
    Base.metadata.create_all(engine)
    print("All tables created successfully.")