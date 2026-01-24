"""
 # Foreign keys
    bag_luggage_id = Column(Integer, ForeignKey('BookedLuggage.ID'), nullable=False)  # Foreign key to BookedLuggage # can get from boooked luggage
    passanger_id = Column(Integer, ForeignKey('Passanger.passangerID'), nullable=False)  # Foreign key to Passanger # can get from boooked luggage
    bokked_flight_id = Column(Integer, ForeignKey('BookedFlight.ID'), nullable=False)  # Foreign key to BookedFlight # can get from boooked luggage
    airport_iata = Column(String(3), ForeignKey('Airport.IATA'), nullable=False)  # Foreign key to Airport # The gotten bookedflight ID can be use to get flight_details and AIRport IATA  
    airline_iata = Column(String(2), ForeignKey('Airline.IATA'), nullable=False)  # Foreign key to Airline can get froom booked flight

    # PIR details
    pir_date = Column(Date, nullable=False)  # Date of the PIR event # can get froom booked flight
    pir_time = Column(Time, nullable=False)  # Time of the PIR event # can generate randomly
    pir_type = Column(SAEnum(PIRType, name="pir_type_enum"), nullable=False ) # Type of PIR (e.g., delayed, lost, damaged) # can generate randomly

"""
