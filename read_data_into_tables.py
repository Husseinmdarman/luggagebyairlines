from database_connection_utils import create_engine_from_creds
from create_classes_for_tables import Airline, Airport
from sqlalchemy.orm import DeclarativeMeta
from typing import Iterable, Optional, Type
import pandas as pd
import os

def read_data_into_table(csv_file_path: str, Table_to_be_loaded: Type[DeclarativeMeta] , desired_columns: Optional[Iterable[str]] = None ):
    """
    Reads data from a CSV file and inserts it into the table in the database.
    Args:
        csv_file_path (str): Path to the CSV file containing airline data.
        Table_to_be_loaded (Type[DeclarativeMeta]): The SQLAlchemy ORM class representing the table to load data into.
        desired_columns (Optional[Iterable[str]]): List of columns to read from the CSV. If None, all columns are read.
    Returns:
        None        
    """
    if not os.path.exists(csv_file_path):
         raise FileNotFoundError(f"File not found: {csv_file_path}") # Check if the file exists
    
    engine = create_engine_from_creds() # Create database engine
   
    # Read CSV data into a DataFrame
    
    table_to_be_loaded_df = pd.read_csv(csv_file_path, usecols=desired_columns or None)
    

    # Insert data into the Airline table
    table_to_be_loaded_df.to_sql(Table_to_be_loaded.__tablename__, engine, if_exists='replace', index=False)
    print(f"Inserted {len(table_to_be_loaded_df)} records into the {Table_to_be_loaded.__tablename__} table.")


if __name__ == "__main__":

    read_data_into_table("Data/airline.csv", Airline)

    desired_ordered_columns = ["IATA Code", "Airport Name", "City", "Country", "Region"] 
    read_data_into_table("Data/airports.csv", Airport, desired_columns=desired_ordered_columns)
    