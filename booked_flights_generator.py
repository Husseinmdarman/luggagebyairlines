from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from create_classes_for_tables import Flight_Details, Passanger, BookedFlight
from collections import defaultdict
import pandas as pd
import yaml  
import random

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
        Assign passengers to flights with the following rules:
        - Every flight must have at least one passenger.
        - A passenger may appear on multiple flights.
        - A passenger cannot appear on two flights occurring on the same flight_date.
        - No flight exceeds max_capacity.
        """

        # Extract flight info
        flights_df = self.flight_details_df.copy()
        passengers_df = self.passanger_df.copy()

        # Convert to lists for easier handling
        passengers = passengers_df["passangerID"].tolist()
        random.shuffle(passengers)  # randomize passenger order

        # Group flights by date → {date: [flight1, flight2, ...]}
        flights_by_date = (
            flights_df.groupby("flight_date")["flight_number"]
            .apply(list)
            .to_dict()
        )

        # Track which passengers are already booked on each date
        # Example: booked_on_date["2025-01-01"] = {12, 44, 91}
        booked_on_date = defaultdict(set)

        # Store final assignments as list of tuples
        assignments = []

        # ---------------------------------------------------------
        # STEP 1 — Guarantee each flight gets at least one passenger
        # ---------------------------------------------------------
        for date, flights in flights_by_date.items():

            for flight in flights:

                # Find the first passenger NOT booked on this date
                assigned = False
                for p in passengers:
                    if p not in booked_on_date[date]:
                        assignments.append((p, flight, date))
                        booked_on_date[date].add(p)
                        assigned = True
                        break

                if not assigned:
                    raise ValueError(
                        f"No available passengers for date {date}. "
                        "Not enough unique passengers to guarantee one per flight."
                    )

        # ---------------------------------------------------------
        # STEP 2 — Fill remaining seats up to max_capacity
        # ---------------------------------------------------------
        for date, flights in flights_by_date.items():

            for flight in flights:

                # Count how many passengers already assigned to this flight
                current_count = sum(1 for p, f, d in assignments if f == flight)
                remaining_capacity = max_capacity - current_count

                if remaining_capacity <= 0:
                    continue  # flight is full

                # Passengers who are NOT booked on this date
                available_passengers = [
                    p for p in passengers if p not in booked_on_date[date]
                ]
                random.shuffle(available_passengers)

                # Assign up to remaining capacity
                for p in available_passengers[:remaining_capacity]:
                    assignments.append((p, flight, date))
                    booked_on_date[date].add(p)

        # ---------------------------------------------------------
        # STEP 3 — Convert assignments to DataFrame
        # ---------------------------------------------------------
        self.passanger_df = pd.DataFrame(
            assignments,
            columns=["passangerID", "flight_number", "flight_date"]
        )



    def generate_booked_flights(self):

        self._assign_flights()

        booked_flight_df = self.passanger_df[["passangerID", "flight_number", "flight_date"]]

        return booked_flight_df



