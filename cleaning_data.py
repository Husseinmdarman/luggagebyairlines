import pandas as pd

def clean_passenger_df(df: DataFrame) -> pd.DataFrame: 
    
    df["family_name"] = df["family_name"].str.strip().str.title() 
    df["given_name"] = df["given_name"].str.strip().str.title() 
    df["email"] = df["email"].str.replace(" ", "").str.lower() 
    
    return df