import pandas as pd
import numpy as np
from typing import List, Tuple, Dict

class FlightDetailsGenerator:
    """
    A class to generate synthetic flight details for a given year.
    Products Randomized flight numbers, airport, airline, and dates
    """

    def __init__(
        self,
        airline_csv_file_path: str,
        airport_csv_file_path: str,
        year: int,
        flights_per_quarter: int,
        seed: int = 42
        
    ) -> None:
        """
        Initializes the FlightDetailsGenerator with airline and airport data.
        Args:
            airline_csv_file_path (str): Path to the CSV file containing airline data.
            airport_csv_file_path (str): Path to the CSV file containing airport data.
            year (int): The year for which to generate flight details.
            flights_per_quarter (int): Number of flights to generate per quarter.
            seed (int): Random seed for reproducibility.
        """
        airline_codes = pd.read_csv(airline_csv_file_path)
        self.airline_IATA = airline_codes["IATA"].dropna().unique().tolist()
        airport_codes = pd.read_csv(airport_csv_file_path)
        self.airports_IATA = airport_codes["IATA Code"].dropna().unique().tolist()
        self.year = year
        self.flights_per_quarter = flights_per_quarter
        
        # Random generator for all randomness in the class
        self.rng = np.random.default_rng(seed)

    def _quarter_ranges(self) -> Dict[str, Tuple[str,str]]:
        """
        Returns a dictionary with quarter names as keys and their corresponding start and end dates as values.
        """
        return {
            "Q1": (f"{self.year}-01-01", f"{self.year}-03-31"),
            "Q2": (f"{self.year}-04-01", f"{self.year}-06-30"),
            "Q3": (f"{self.year}-07-01", f"{self.year}-09-30"),
            "Q4": (f"{self.year}-10-01", f"{self.year}-12-31"),
        }

    def _random_dates(self, start_date: str, end_date: str, n: int) -> pd.Series:
        """
        Generates a list of random dates between start_date and end_date.
        Args:
            start_date (str): The start date in 'YYYY-MM-DD'
            end_date (str): The end date in 'YYYY-MM-DD'
            n (int): Number of random dates to generate.
        Returns:
            pd.Series: A Series of random dates.
        """
        start = np.datetime64(start_date)
        end = np.datetime64(end_date)

        #Total number of days in the range 
        days = (end - start).astype(int) + 1 # the +1 makes the end date inclusive

        #random offset fomr the start date
        random_days = self.rng.integers(0, days, size=n)

        return pd.to_datetime(start + random_days) 

    def _generate_flightnumbers(self, n: int) -> Tuple[np.ndarray, List[str]]:
        """
        Generates unique flight numbers using: Airline IATA code and 6 random digits

        Args:
            n (int): Number of flight numbers to generate.

        Returns:
            Tuple[np.ndarray, List[str]]: A array of airline codes, list of unique flightnumbers
        """

        # Random airline assingment per row
        airline_codes = self.rng.choice(self.airline_IATA, size=n, replace= True)

        # Generate random 6 digit numbers
        random_numbers = self.rng.integers(0, 1_000_000, size=n)
        nums_str = [f"{num:06d}" for num in random_numbers]

        # Combine Airline + Number

        flight_numbers = [airline_codes[i] + nums_str[i] for i in range(n)]

        # Ensure uniqueness
        if len(set(flight_numbers)) != len(flight_numbers):
            seen = set()
            final = []
            for fn in flight_numbers:
                while fn in seen:
                    new_num = self.rng.integers(0, 1_000_000)
                    fn = fn[:2] + f"{new_num:06d}"
                seen.add(fn)
                final.append(fn)
            flight_numbers = final

        return airline_codes, flight_numbers

    def _generate_quarter(self, start:str, end: str) -> pd.DataFrame:
        """
        Generates a Dataframe of flights for a single quarter.
        Args:
            start (str): Start date of the quarter in 'YYYY-MM-DD'
            end (str): End date of the quarter in 'YYYY-MM-DD'
        Returns:
            pd.DataFrame: DataFrame containing flight details for the quarter.
        """
        n = self.flights_per_quarter
        
        # Generate airline choices and flight numbers
        airline_choices, flight_numbers = self._generate_flightnumbers(n)

        # Random departure and arrival airports
        departure_airport = self.rng.integers(0, len(self.airports_IATA), size = n)
        arrival_airport = self.rng.integers(0, len(self.airports_IATA), size = n)

        departure_airport = [self.airports_IATA[i] for i in departure_airport]
        arrival_airport = [self.airports_IATA[i] for i in arrival_airport] 

        # Ensure the departue and arrival airport is not the same

        for i in range(n):
            if departure_airport[i] == arrival_airport[i]:
                new_idx = self.rng.integers(0, len(self.airports_IATA))
                while self.airports_IATA[new_idx] == departure_airport[i]:
                    new_idx = self.rng.integers(0, len(self.airports_IATA))
                arrival_airport[i] = self.airports_IATA[new_idx]

        dates = self._random_dates(start, end, n)
        dates = pd.Series(dates)

        return pd.DataFrame({ 
            "flightnumber": flight_numbers, 
            "DepartureIATA": departure_airport, 
            "ArrivalIATA": arrival_airport, 
            "AirlineIATA": airline_choices, 
            "flightdate": dates.dt.strftime("%Y-%m-%d") 
            })

    #  Public method to generate full data set
    def generate(self) -> pd.DataFrame:
        """
        Generates a DataFrame containing flight details for the entire year.
        Returns:
            pd.DataFrame: DataFrame containing flight details for the year.
        """
        print("Start generating flight details")
        all_quarters = []
        quarter_ranges = self._quarter_ranges()
        print("first quarter")
        for quarter, (start, end) in quarter_ranges.items():
            
            quarter_df = self._generate_quarter(start, end)
            print("next quarter")
            all_quarters.append(quarter_df)

        return pd.concat(all_quarters, ignore_index=True)
        


gen = FlightDetailsGenerator( "Data/airline.csv", "Data/airports.csv", 2023, flights_per_quarter=2000 ) 
df = gen.generate() 
df = df.sort_values(by=["flightdate"], ascending=True).reset_index(drop=True)
df.to_csv("Data/flights_details/flight_details_2023.csv", index=False)
