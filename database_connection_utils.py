import yaml
from sqlalchemy import create_engine, text

def create_engine_from_creds():
    """
    Creates a SQLAlchemy engine using database credentials stored in a YAML file.
    Args:
        None
    Returns:
        engine: SQLAlchemy engine object connected to the specified database.
    """

    database_creds = yaml.safe_load(open("database_creds.yaml")) # Load database credentials from YAML file

    engine = create_engine(f'{database_creds["driver"]}://{database_creds["username"]}:{database_creds["password"]}@{database_creds["host"]}:{database_creds["port"]}/{database_creds["database"]}')  #creating the database engine from the credentials

    try:
        with engine.connect() as connection: 
            version = connection.execute(text("SELECT version();")).scalar() 
            print(version) # confirming connection by printing database version
    except ConnectionError as e:
        print(f"An Connection error has occured to the database : {e}") # if connection fails, print error message
    
    return engine