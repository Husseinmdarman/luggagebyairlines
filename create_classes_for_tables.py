
from sqlalchemy.orm import declarative_base, relationship, DeclarativeMeta
from sqlalchemy import Column, Integer, String, ForeignKey, Date, UniqueConstraint, CheckConstraint, Time
from sqlalchemy import Enum as SAEnum
from typing import Type
from pir_type import PIRType

Base = declarative_base()

class FactPIR(Base):
    """
    Represents a Simplified Propert Irregularity Report (PIR) fact table.
    One row = One PIR event for one bag.
    """

    __tablename__ = "FactPIR"

    PIR_ID = Column(Integer, primary_key=True, autoincrement=True)  # Unique identifier for each PIR event

    # Foreign keys
    bag_luggage_id = Column(Integer, ForeignKey('BookedLuggage.ID'), nullable=False)  # Foreign key to BookedLuggage
    passanger_id = Column(Integer, ForeignKey('Passanger.passangerID'), nullable=False)  # Foreign key to Passanger
    bokked_flight_id = Column(Integer, ForeignKey('BookedFlight.ID'), nullable=False)  # Foreign key to BookedFlight
    airport_iata = Column(String(3), ForeignKey('Airport.IATA'), nullable=False)  # Foreign key to Airport
    airline_iata = Column(String(2), ForeignKey('Airline.IATA'), nullable=False)  # Foreign key to Airline

    # PIR details
    pir_date = Column(Date, nullable=False)  # Date of the PIR event
    pir_time = Column(Time, nullable=False)  # Time of the PIR event
    pir_type = Column(SAEnum(PIRType, name="pir_type_enum"), nullable=False ) # Type of PIR (e.g., delayed, lost, damaged)

    # Relationships
    bag = relationship("BookedLuggage", back_populates = "pir_reports")  # Relationship to BookedLuggage
    passenger = relationship("Passanger", back_populates = "pir_reports")  # Relationship to passenger
    booked_flight = relationship("BookedFlight", back_populates = "pir_reports")  # Relationship to BookedFlight
    airport = relationship("Airport", back_populates = "pir_reports")  # Relationship to airport   
    airline = relationship("Airline", back_populates = "pir_reports")  # Relationship to Airline

class BookedFlight(Base):
    """
    Represents the Booked_flight table in the database.
    """
    __tablename__ = "BookedFlight" 

    ID = Column(Integer, primary_key=True, autoincrement=True) # Unique identifier for each booked flight
    passangerID = Column(Integer, ForeignKey('Passanger.passangerID'), nullable=False, unique = True)  # Foreign key to Passanger 
    flight_number = Column(String(10), ForeignKey('Flight_Details.flight_number'), nullable=False)  # Foreign key to Flight_Details
    

    # Relationships
    passenger = relationship("Passanger", back_populates="booked_flights") #   Relationship to Passanger
    flight_details = relationship("Flight_Details", back_populates="booked_flights") # Relationship to Flight_Details
    luggages = relationship("BookedLuggage", back_populates="booked_flight") # Relationship to BookedLuggage
    pir_reports = relationship("FactPIR", back_populates="booked_flight")  # Relationship to FactPIR
    
    __table_args__  = (
        UniqueConstraint("passangerID", "flight_number", name="uq_passenger_flight"), # Ensures a passenger cannot book the same flight multiple times
        UniqueConstraint("passangerID", name="uq_passenger_one_flight"), # Ensures a passenger can only have one booked flight at a time
    )

class BookedLuggage(Base):
    """
    Represents the Booked_luggage table in the database.
    """
    __tablename__ = "BookedLuggage" 

    ID = Column(Integer, primary_key=True, autoincrement=True) # Unique identifier for each luggage entry
    bag_tag = Column(String(20), nullable=False)  # e.g., "Bag Tags can be reissued or reused so not unique" 
    passangerID = Column(Integer, ForeignKey('Passanger.passangerID'), nullable=False)  # Foreign key to Passanger
    BookedFlightID = Column(Integer, ForeignKey("BookedFlight.ID"), nullable=False) # Foreign key to BookedFlight
    weight_kg = Column(Integer, nullable=False) # Weight of the luggage in kilograms
    dimensions_cm = Column(String(50), nullable=False)  # e.g., "55x40x20" 

    # Relationships
    passenger = relationship("Passanger", back_populates="luggages") # Relationship to Passanger
    booked_flight = relationship("BookedFlight", back_populates="luggages") # Relationship to BookedFlight
    pir_reports = relationship("FactPIR", back_populates="bag")  # Relationship to FactPIR

    __table_args__ = (
        UniqueConstraint('bag_tag', 'passangerID', name="uq_bag_tag_passanger"), # Ensures a passenger cannot have duplicate bag tags
        CheckConstraint( "(split_part(dimensions_cm, 'x', 1)::int + " " split_part(dimensions_cm, 'x', 2)::int + " " split_part(dimensions_cm, 'x', 3)::int) <= 158", 
                            name='chk_bag_dimensions' ), # Total dimensions must be <= 158 cm
        CheckConstraint( "weight_kg <= 32", name='chk_bag_weight' )  # Weight must be <= 32 kg as based on airline industry standards
    )

class Passanger(Base):
    """
    Represents the Passanger table in the database.
    """
    __tablename__ = "Passanger" 

    passangerID = Column(Integer, primary_key=True, autoincrement=True) # Unique identifier for each passenger
    family_name = Column(String(50), nullable=False) # Family name / surname
    given_name = Column(String(50), nullable=False)  # Given name / first name
    gender = Column(String(20), nullable=False) #   Gender
    date_of_birth = Column(Date, nullable=False) # Date of birth
    email = Column(String(100), nullable=False)  # Email address
    phone_number = Column(String(50), nullable=False) # Phone number

    luggages = relationship("BookedLuggage", back_populates="passenger") # Relationship to BookedLuggage
    booked_flights = relationship("BookedFlight", back_populates="passenger", uselist=False) # Relationship to BookedFlight
    pir_reports = relationship("FactPIR", back_populates="passenger")  # Relationship to FactPIR

    #due to the csv having no unique identifier other than a combination of columns, we create a unique constraint
    __table_args__ = (
                        UniqueConstraint('family_name', 'given_name',"gender" ,'date_of_birth', "email", name = "uq_passanger_identity"),
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
    
    # Reverse relationship to Flight_Details
    flights =  relationship("Flight_Details", back_populates="airline")
    pir_reports = relationship("FactPIR", back_populates="airline")  # Relationship to FactPIR

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

    # Reverse relationship to Flight_Details
    departing_flights = relationship(
        "Flight_Details", 
        back_populates = "departure_airport",
        foreign_keys = "Flight_Details.Departure_IATA" 
    )
    arriving_flights = relationship(
        "Flight_Details",
        back_populates = "arrival_airport",
        foreign_keys = "Flight_Details.Arrival_IATA"
    )
    pir_reports = relationship("FactPIR", back_populates="airport")  # Relationship to FactPIR  

class Flight_Details(Base):
    """
    Represents the Flight_Details table in the database.
    """
    __tablename__ = "Flight_Details"

    flight_number = Column(String(10), primary_key=True)

    # Foreign keys 
    Departure_IATA = Column(String(3), ForeignKey("Airport.IATA"), nullable=False) 
    Arrival_IATA = Column(String(3), ForeignKey("Airport.IATA"), nullable=False) 
    Airline_IATA = Column(String(2), ForeignKey("Airline.IATA"), nullable=False)

    flight_date = Column(Date, nullable=False)  

    booked_flights = relationship("BookedFlight", back_populates="flight_details")
    # Forward Relationship to Airport
    departure_airport = relationship(
        "Airport",
        foreign_keys = [Departure_IATA],
        back_populates = "departing_flights"
    )
    arrival_airport = relationship(
        "Airport",
        foreign_keys = [Arrival_IATA],
        back_populates = "arriving_flights"
    )
    # Forward Relationship to Airline
    airline = relationship(
        "Airline",
        foreign_keys= [Airline_IATA],
        back_populates="flights"
    )
    
   
def get_unique_columns(model = Type[DeclarativeMeta]) -> List[str]: 
    """ Returns a list of column names participating in a UniqueConstraint. If none exist, returns an empty list. """ 
    
    unique_cols = [] 
    
    for constraint in model.__table__.constraints: 
        if isinstance(constraint, UniqueConstraint):  # checks if constraint is a uniqueconstraint
            unique_cols.extend([col.name for col in constraint.columns])  # add column names to the list

        return unique_cols