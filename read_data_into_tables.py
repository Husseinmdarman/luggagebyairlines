from database_connection_utils import create_engine_from_creds
from create_classes_for_tables import Airline, Airport
import pandas as pd
import os

def read_airline_data_into_table(csv_file_path):
    """
    Reads airline data from a CSV file and inserts it into the Airline table in the database.
    Args:
        csv_file_path (str): Path to the CSV file containing airline data.
    Returns:
        None        
    """
    if not os.path.exists(csv_file_path):
         raise FileNotFoundError(f"File not found: {csv_file_path}") # Check if the file exists
    
    engine = create_engine_from_creds() # Create database engine
   
    # Read CSV data into a DataFrame
    airline_df = pd.read_csv(csv_file_path)

    # Insert data into the Airline table
    airline_df.to_sql(Airline.__tablename__, engine, if_exists='append', index=False)
    print(f"Inserted {len(airline_df)} records into the Airline table.")

def read_airport_data_into_table(csv_file_path):
    """
    Reads airline data from a CSV file and inserts it into the Airline table in the database.
    Args:
        csv_file_path (str): Path to the CSV file containing airline data.
    Returns:
        None        
    """
    if not os.path.exists(csv_file_path):
         raise FileNotFoundError(f"File not found: {csv_file_path}") # Check if the file exists
    
    engine = create_engine_from_creds() # Create database engine
    # Desired order of columns in the Airport table
    desired_ordered_columns = ["IATA Code", "Airport Name", "City", "Country", "Region"]  
    # Read CSV data into a DataFrame
    airport_df = pd.read_csv(csv_file_path, usecols=desired_ordered_columns)

    # Insert data into the Airline table
    airport_df.to_sql(Airport.__tablename__, engine, if_exists='append', index=False)
    print(f"Inserted {len(airport_df)} records into the Airport table.")

if __name__ == "__main__":

    read_airline_data_into_table("Data/airline.csv")
    read_airport_data_into_table("Data/airports.csv")