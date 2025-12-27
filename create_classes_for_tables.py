
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, Integer, String, ForeignKey 

Base = declarative_base()   

class CountryRegion(Base):
    """
    Represents the CountryRegion table in the database.
    """
    __tablename__ = "CountryRegion" 

    id = Column(Integer, primary_key=True, autoincrement=True) 
    Country = Column(String(100), nullable=False, unique=True) # Country name 
    Region = Column(String(100), nullable=False) # Region (e.g., Europe, Asia)

    # Relationships
    airlines = relationship("Airline", back_populates="country_region")
    airports = relationship("Airport", back_populates="country_region")

class Airline(Base):
    """
    Represents the Airline table in the database.
    """
    __tablename__ = "Airline" 

    IATA = Column(String(2), primary_key=True) # e.g., "AA" 
    Airline = Column(String(100), nullable=False) # Airline name 
    
    country_region_id = Column(Integer, ForeignKey('CountryRegion.id'))
    country_region = relationship("CountryRegion", back_populates="airlines")

class Airport(Base):
    """
    Represents the Airline table in the database.
    """
    __tablename__ = "Airport" 

    IATA = Column(String(3), primary_key=True) # e.g., "ATL for Atlanta" 
    Airport_name = Column(String(100), nullable=False) # Airline name
    City = Column(String(100)) # Country of origin 
    country_region_id = Column(Integer, ForeignKey('CountryRegion.id'))
    country_region = relationship("CountryRegion", back_populates="airports")
   
