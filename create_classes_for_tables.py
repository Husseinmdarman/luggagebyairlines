
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String

Base = declarative_base()   

class Airline(Base):
    """
    Represents the Airline table in the database.
    """
    __tablename__ = "Airline" 

    IATA = Column(String(2), primary_key=True) # e.g., "AA" 
    Airline = Column(String(100), nullable=False) # Airline name 
    Country = Column(String(100)) # Country of origin 
    Region = Column(String(100)) # Region (e.g., Europe, Asia)
   


