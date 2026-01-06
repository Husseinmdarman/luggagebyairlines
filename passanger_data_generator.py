from faker import Faker
import random
from datetime import date, timedelta
import pandas as pd
import os


fake = Faker()

def generate_passanger_row(): 
    """Generates a single row of passenger data.
    Returns:
    dict: A dictionary representing a passenger's data.
    """
    
    given  =  fake.first_name() 
    family =  fake.last_name() # DOB realism: 14 days old → 85 years old 
    today  =  date.today() 
    min_dob = today.replace(year=today.year - 16)
    max_dob = today.replace(year=today.year - 85) 
    dob = fake.date_between(start_date=max_dob, end_date=min_dob) 
    
    return { 
            "family_name": family, 
            "given_name": given, 
            "gender": random.choice(["M", "F"]), 
            "date_of_birth": dob, 
            "phone_number": fake.phone_number(), 
            "email": f"{given.lower()}.{family.lower()}@{fake.free_email_domain()}" 
            }

def passanger_generator(num_passangers: int): 
    """
    Generator function that yields a specified number of passenger data rows.
    Args:
        num_passangers (int): The number of passenger rows to generate.
    Yields:
        dict: A dictionary representing a passenger's data.    
        """
   
    for _ in range(num_passangers): 
        yield generate_passanger_row()

def write_passangers_to_csv(n, chunk_size=50000, path="passengers.csv"): 
    
    gen = passanger_generator(n) 
    first = not os.path.exists(path)
    
    while True: 
        chunk = [] 
        try: 
            for _ in range(chunk_size): 
                chunk.append(next(gen)) 
        except StopIteration: 
            pass 
            
        if not chunk: 
            break 
            
        df = pd.DataFrame(chunk) 
        df.to_csv(path, mode="a", header=first, index=False) 
        first = False

if __name__ == "__main__":
    write_passangers_to_csv(20000, path = "Data/Passenger details/passengers.csv")  # Generate 20k passenger records