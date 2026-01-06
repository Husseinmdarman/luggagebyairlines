import pandas as pd

def clean_passenger_df(df: DataFrame) -> pd.DataFrame: 
    
    df["family_name"] = df["family_name"].str.strip().str.title() 
    df["given_name"] = df["given_name"].str.strip().str.title() 
    df["email"] = df["email"].str.replace(" ", "").str.lower() 
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth']) # Ensure date_of_birth is in datetime format otherwise will read as an object

    return df

def hello(strin: str = "hello"):
    return strin