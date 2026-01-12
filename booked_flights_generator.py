from sqlalchemy import create_engine, insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from create_classes_for_tables import Flight_Details, Passanger, BookedFlight
import pandas as pd
import yaml  

class BookFlightGenerator:
    """
    A class to generate synthetic booked flight data.
    """
    def __init__(self, engine: Engine):
        """
        Initializes the BookFlightGenerator with a database engine.
        Args:
            engine (Engine): SQLAlchemy engine connected to the target database.
        """

        self.engine = engine

    def load_flight_details_from_db(self, table_name: str) -> pd.DataFrame:
        """
        Loads flight details from the specified database table.
        Args:
            table_name (str): Name of the table containing flight details.
        Returns:
            pd.DataFrame: DataFrame containing flight details.
        """
        with Session(self.engine) as session:
            flight_details = session.query(Flight_Details).all()
            
        self.flight_details_df = pd.DataFrame([
                {
                    "flight_number": fd.flight_number,
                    "departure_airport": fd.Departure_IATA,
                    "arrival_airport": fd.Arrival_IATA,
                    "airline": fd.Airline_IATA,
                    "flight_date": fd.flight_date
                }
                for fd in flight_details
            ])

    def load_passengers_from_db(self, table_name: str) -> pd.DataFrame:
        """
        Loads passenger details from the specified database table.
        Args:
            table_name (str): Name of the table containing passenger details.
        Returns:
            pd.DataFrame: DataFrame containing passenger details.
        """
        with Session(self.engine) as session:
            passanger = session.query(Passanger).all()

        self.passanger_df = pd.DataFrame([  
                {
                    "passangerID": p.passangerID,
                    "family_name": p.family_name,
                    "given_name": p.given_name,
                    "gender": p.gender,
                    "date_of_birth": p.date_of_birth,
                    "email": p.email,
                    "phone_number": p.phone_number
                }
                for p in passanger
            ])

    def _assign_flights(self, max_capacity: int = 20) -> pd.DataFrame:
        """
        Assigns passengers to flights randomly, ensuring no flight exceeds its maximum capacity.
        Args:
            max_capacity (int): Maximum number of passengers per flight.
        Returns:
            pd.DataFrame: DataFrame containing booked flight records.
        """

        flights = self.flight_details_df["flight_number"].tolist()
        total_capacity = len(flights) * max_capacity
        num_passengers = len(self.passanger_df)

        if num_passengers > total_capacity: 
            raise ValueError("All flights are full")

        assingment_pool = (
            self.flight_details_df["flight_number"].repeat(max_capacity).tolist()
        )

        self.passanger_df["flight_number"] = assingment_pool[:num_passengers]

    def generate_booked_flights(self):

        self._assign_flights()

        booked_flight_df = self.passanger_df[["passangerID", "flight_number"]]

        return booked_flight_df



