from database_connection_utils import create_engine_from_creds
from create_classes_for_tables import Airline, Airport, CountryRegion, Base
from sqlalchemy.orm import DeclarativeMeta
from typing import Iterable, Optional, Type
import pandas as pd
import os

engine = create_engine_from_creds() 
Base.metadata.create_all(engine)

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
    
    existing = pd.read_sql(f'SELECT "{primary_key_name}" FROM "{Table_to_be_loaded.__tablename__}"', engine)
    dataframe_to_upload = dataframe_to_upload[~dataframe_to_upload[primary_key_name].isin(existing[primary_key_name])]
    # Insert data into the specified table
    dataframe_to_upload.to_sql(Table_to_be_loaded.__tablename__, engine, if_exists='append', index=False)
    print(f"Inserted {len(dataframe_to_upload)} records into the {Table_to_be_loaded.__tablename__} table.")

def create_countryregion_table(airline_csv_file_path: str,airport_csv_file_path: str,Table_to_be_loaded: Type[DeclarativeMeta]):
    
    airlines_df= read_csv_data_into_dataframe(airline_csv_file_path)
    airports_df = read_csv_data_into_dataframe(airport_csv_file_path, desired_columns=["IATA Code", "Airport Name", "City", "Country", "Region"])
    
    #Extract unique combinations of country and region from dataframe
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
   
   airports_df, airlines_df= create_countryregion_table("Data/airline.csv","Data/airports.csv", CountryRegion)
   airports_df.rename(columns={"Airport Name": "Airport_name", "IATA Code": "IATA"}, inplace=True)
   print(airports_df.head(10)) 
   load_df_sql(airports_df, Airport)
   load_df_sql(airlines_df, Airline)
   