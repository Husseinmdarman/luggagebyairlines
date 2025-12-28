
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, Integer, String, ForeignKey, Date, UniqueConstraint 
from sqlalchemy.orm import DeclarativeMeta
from typing import Type

Base = declarative_base()

class Passanger(Base):
    """
    Represents the Passanger table in the database.
    """
    __tablename__ = "Passanger" 

    PassangerID = Column(Integer, primary_key=True, autoincrement=True) 
    family_name = Column(String(50), nullable=False) 
    given_name = Column(String(50), nullable=False) 
    gender = Column(String(20), nullable=False)
    date_of_birth = Column(Date, nullable=False)
    email = Column(String(100), nullable=False) 
    phone_number = Column(String(50), nullable=False)

    #due to the csv having no unique identifier other than a combination of columns, we create a unique constraint
    __table_args__ = (
                        UniqueConstraint('family_name', 'given_name', 'date_of_birth', name = "uq_passenger_identity"),
                        )

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
   
def get_unique_columns(model = Type[DeclarativeMeta]) -> List[str]: 
    """ Returns a list of column names participating in a UniqueConstraint. If none exist, returns an empty list. """ 
    
    unique_cols = [] 
    
    for constraint in model.__table__.constraints: 
        if isinstance(constraint, UniqueConstraint): 
            unique_cols.extend([col.name for col in constraint.columns]) 

        return unique_cols