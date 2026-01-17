from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy import select
from create_classes_for_tables import BookedFlight
import pandas as pd
import random
import string

class BookedLuggageGenerator:
    def __init__(self, engine: Engine):
        """
        Initializes the BookedLuggageGenerator with a database engine.
        Args:
            engine (Engine): SQLAlchemy Engine instance for database connection.

        """    
        self.engine = engine 
        
        self.bag_count_probabilities = {
            1: 0.70,
            2: 0.25,
            3: 0.05
        }  # Probabilities for number of bags
        self.dimenstions = { 
            "55x40x20", "60x45x23", "50x38x22",
            "65x45x25", "70x50x28"
        } # Possible luggage dimensions

    def _generate_bag_tag(self) -> str:
        """
        Generates a random bag tag consisting of 10 uppercase letters and digits.
        Returns:
            str: A randomly generated bag tag.
        """        
        characters = string.ascii_uppercase + string.digits
        bag_tag = ''.join(random.choices(characters, k=10))
        return bag_tag
    
    def _random_bag_weight(self) -> int:
        """
        Generates a random bag weight between 10 and 32 kg.
        Returns:
            int: A randomly generated bag weight.
        """        
        return random.randint(10, 32)
    def _random_bag_count(self) -> int:
        """
        Determines the number of bags based on predefined probabilities.
        Returns:
            int: The number of bags (1, 2, or 3).
        """        
        
        bag_count = random.choices(
            population=list(self.bag_count_probabilities.keys()),
            weights=list(self.bag_count_probabilities.values()),
            k=1
        )[0]  #pick a bag count based on defined probabilities and returns first and only element in the list


        return bag_count

    def generate_booked_luggage(self) -> None:
        """
        Generates booked luggage data for a given booked flight.
        Args:
            booked_flight (BookedFlight): The booked flight instance.
        
        """

        with Session(self.engine) as session:
            rows = session.execute(select(BookedFlight.ID, BookedFlight.passangerID )).all()

        data = []
        
        for booked_flight_id, passenger_id in rows:
            
            bag_count = self._random_bag_count()
            
            for _ in range(bag_count):
                
                bag_tag = self._generate_bag_tag()
                weight = self._random_bag_weight()
                dimensions = random.choice(list(self.dimenstions))
                
                data.append({  
                        "bag_tag": bag_tag,
                        "passangerID": passenger_id,
                        "BookedFlightID": booked_flight_id,
                        "weight_kg": weight,
                        "dimensions_cm": dimensions
                })
        
        return pd.DataFrame(data)
                
            
            





