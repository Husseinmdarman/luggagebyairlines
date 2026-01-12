from database_connection_utils import create_engine_from_creds
from create_classes_for_tables import Airline, Airport,BookedFlight ,CountryRegion, Passanger,Flight_Details,Base, get_unique_columns
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import DeclarativeMeta, Session
from sqlalchemy.dialects.postgresql import insert
from typing import Iterable, Optional, Type
from pathlib import Path
from cleaning_data import clean_passenger_df
from booked_flights_generator import BookFlightGenerator
import pandas as pd
import os

engine = create_engine_from_creds() 
Base.metadata.create_all(engine)

def build_key(df, cols): 
    """
    Builds a unique key by concatenating specified columns with a delimiter.
    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        cols (List[str]): List of column names to concatenate.
    Returns:
        pd.Series: A Series containing the concatenated keys.
    """
    if df.empty: 
        return pd.Series(dtype=str)  
        
    return df[cols].astype(str).agg("|".join, axis=1)

def process_folder(folder_path: str, desired_columns=None):
    """
    Processes all CSV files in a specified folder.
    Args:
        folder_path (str): The path to the folder containing CSV files.
    """

    folder = Path(folder_path)
    csv_files = sorted(folder.glob("*.csv")) 
    
    if not csv_files: 
        print("No CSV files found.") 
        return
    
    for csv_file in csv_files:
        
        loaded_dataframe = pd.read_csv(csv_file, usecols=desired_columns or None)
        yield loaded_dataframe


def read_csv_data_into_dataframe(csv_file_path: str, desired_columns: Optional[Iterable[str]] = None ):
    """
    Reads CSV data into a pandas DataFrame.
    Args:
        csv_file_path (str): The path to the CSV file.  
        desired_columns (Optional[Iterable[str]]): List of columns to read from the CSV. If None, all columns are read.
        Returns:
        pd.DataFrame: The loaded DataFrame.
    
    """
    if not os.path.exists(csv_file_path):
         raise FileNotFoundError(f"File not found: {csv_file_path}") # Check if the file exists
   
    # Read CSV data into a DataFrame
    
    loaded_dataframe = pd.read_csv(csv_file_path, usecols=desired_columns or None)

    return loaded_dataframe

    
def load_df_sql(dataframe_to_upload: pd.DataFrame, Table_to_be_loaded: Type[DeclarativeMeta]):
    """
    Loads a dataframe into a SQL table.
    Args:
        dataframe_to_upload (pd.DataFrame): The DataFrame to upload to the database.
        Table_to_be_loaded (Type[DeclarativeMeta]): The SQLAlchemy ORM class representing the table to load data into.
    Returns:
        None        
    """
    #Primary Key column name
    primary_key_name = Table_to_be_loaded.__table__.primary_key.columns[0].name

    if primary_key_name in dataframe_to_upload.columns:
        existing = pd.read_sql(f'SELECT "{primary_key_name}" FROM "{Table_to_be_loaded.__tablename__}"', engine)
        
        #creates a boolean mask to check for conflicts and only leaves new rows in the dataframe
        dataframe_to_upload = dataframe_to_upload[~dataframe_to_upload[primary_key_name].isin(existing[primary_key_name])] 
    
    # Inspect model metadata, will return a empty list if there is no UniqueConstraint
    unique_cols = get_unique_columns(Table_to_be_loaded)
    
    if unique_cols:
        cols_str = ", ".join(unique_cols)
        print(cols_str)

        existing = pd.read_sql( f'SELECT {cols_str} FROM "{Table_to_be_loaded.__tablename__}"', engine )
        # Example: detect duplicates before insert 

        dataframe_to_upload["__key__"] = build_key(dataframe_to_upload, unique_cols) 
        existing["__key__"] = build_key(existing, unique_cols)

        dataframe_to_upload = dataframe_to_upload[~dataframe_to_upload["__key__"].isin(existing["__key__"])]
        dataframe_to_upload = dataframe_to_upload.drop(columns="__key__")

    # Insert data into the specified table
    unique_constraints = [ 
        c for c in Table_to_be_loaded.__table__.constraints 
        if isinstance(c, UniqueConstraint) 
        ]  # checks for unique constraints in the table definition

    uc = unique_constraints[0] if unique_constraints else None  # Get the first unique constraint if it exists
    
    constraint_name = uc.name if uc else None  # Get the name of the unique constraint if it exists
    
    with Session(engine) as session:  # Create a new session
        for row in dataframe_to_upload.to_dict(orient="records"): # Iterate over each row in the DataFrame 
            stmt = insert(Table_to_be_loaded).values(**row) # Create an insert statement for the row
            
            if constraint_name: # If a unique constraint exists, handle conflicts
                stmt = stmt.on_conflict_do_nothing( constraint=constraint_name ) # Do nothing on conflict based on the unique constraint
                
            session.execute(stmt)  # Execute the statement within the session
                
            session.commit() # Commit the transaction

def create_countryregion_table(airline_csv_file_path: str,airport_csv_file_path: str,Table_to_be_loaded: Type[DeclarativeMeta]):
    
    airlines_df= read_csv_data_into_dataframe(airline_csv_file_path)
    airports_df = read_csv_data_into_dataframe(airport_csv_file_path, desired_columns=["IATA Code", "Airport Name", "City", "Country", "Region"])
    
    #Extract unique combinations of country and region from dataframe
    # concatenate the country and region columns from both dataframes, drop duplicates, and reset the index
    country_region_df = ( 
        pd.concat([ 
            airlines_df[['Country', 'Region']], 
            airports_df[['Country', 'Region']] ])
            .drop_duplicates()
            .reset_index(drop=True) )

    
    
    # Insert data into the CountryRegion table
    existing = pd.read_sql('SELECT "Country" FROM "CountryRegion"', engine)
     
    country_region_df = country_region_df[~country_region_df["Country"].isin(existing["Country"])]

    country_region_df.to_sql(Table_to_be_loaded.__tablename__, engine, if_exists='append', index=False)
    
    print(f"Inserted {len(country_region_df)} records into the {Table_to_be_loaded.__tablename__} table.")
    print(country_region_df.head(10))

    #after creating the CountryRegion table, we need to update the Airline and Airport tables to reference it

    cr_lookup = pd.read_sql(f'SELECT ID, "Country", "Region" FROM "{Table_to_be_loaded.__tablename__}"', engine)
    
    airlines_df = airlines_df.merge(cr_lookup, on=["Country", "Region"], how="left" ) 
    airlines_df = airlines_df.rename(columns={"id": "country_region_id"}) 
    airlines_df = airlines_df.drop(columns=["Country", "Region"])
    
    airports_df = airports_df.merge( cr_lookup, on=["Country", "Region"], how="left" ) 
    airports_df = airports_df.rename(columns={"id": "country_region_id"}) 
    airports_df = airports_df.drop(columns=["Country", "Region"])

    return airports_df, airlines_df

if __name__ == "__main__":
   
#    airports_df, airlines_df= create_countryregion_table("Data/airline.csv","Data/airports.csv", CountryRegion)
#    airports_df.rename(columns={"Airport Name": "Airport_name", "IATA Code": "IATA"}, inplace=True)
   
#    load_df_sql(airports_df, Airport)
#    load_df_sql(airlines_df, Airline)

#    for passanger_df in process_folder("Data/Passenger details"):
#         passanger_df = clean_passenger_df(passanger_df)
#         print(f"passanger_date_of_birth datatype: {passanger_df['date_of_birth'].dtype}")
#         load_df_sql(passanger_df, Passanger)
        
#    for flight_details_df in process_folder("Data/flights_details"):
#         print(flight_details_df)
#         print(f"flight_details_df columns: {flight_details_df.columns.tolist()}")
#         load_df_sql(flight_details_df, Flight_Details)    
#         print("flight details loaded")

    booking_flights_for_passangers = BookFlightGenerator(engine)
    booking_flights_for_passangers.load_flight_details_from_db("Flight_Details")
    booking_flights_for_passangers.load_passengers_from_db("Passanger")
    booked_flights_df = booking_flights_for_passangers.generate_booked_flights()
    load_df_sql(booked_flights_df, BookedFlight)
