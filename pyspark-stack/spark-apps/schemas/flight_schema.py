from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def get_flight_schema():
    return StructType([
        StructField("icao24", StringType(), True),
        StructField("firstSeen", IntegerType(), True),
        StructField("lastSeen", IntegerType(), True),
        StructField("estDepartureAirport", StringType(), True),
        StructField("estArrivalAirport", StringType(), True),
        StructField("callsign", StringType(), True),
        StructField("estDepartureAirportHorizDistance", IntegerType(), True),
        StructField("estDepartureAirportVertDistance", IntegerType(), True),
        StructField("estArrivalAirportHorizDistance", IntegerType(), True),
        StructField("estArrivalAirportVertDistance", IntegerType(), True),
        StructField("departureAirportCandidatesCount", IntegerType(), True),
        StructField("arrivalAirportCandidatesCount", IntegerType(), True),
    ])
