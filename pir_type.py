from enum import Enum

class PIRType(Enum): 
    """
    Enum types for the PIR report
    """
    
    LOST = "lost" 
    DELAYED = "delayed" 
    DAMAGED = "damaged"

