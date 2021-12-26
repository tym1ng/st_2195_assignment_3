import sqlite3
import os
import pandas as pd

#change to the directory where the files are stored
os.chdir("../Downloads/ST2195 Assignment/Assignment 3")

try:
    os.remove('airline3.db')
except OSError:
    pass

#===== create database =====
conn = sqlite3.connect('airline3.db')

#===== create tables =====
airports = pd.read_csv("airports.csv")
carriers = pd.read_csv("carriers.csv")
planes = pd.read_csv("plane-data.csv")

airports.to_sql('airports',con=conn, index=False)
carriers.to_sql('carriers',con=conn, index=False)
planes.to_sql('planes',con=conn, index=False)

c = conn.cursor()
c.execute(''' 
CREATE TABLE ontime (
    Year int,
    Month int,
    DayofMonth int,
    DayofWeek int,
    DepTime int,
    CRSDepTime int,
    ArrTime int,
    CRSArrTime int,
    UniqueCarrier varchar(5),
    FlightNum int,
    TailNum varchar(8),
    ActualElapsedTime int,
    CRSElapsedTime int,
    AirTime int,
    ArrDelay int,
    DepDelay int,
    Origin varchar(3),
    Dest varchar(3),
    Distance int,
    TaxiIn int,
    TaxiOut int,
    Cancelled int,
    CancellationCode varchar(1),
    CarrierDelay int,
    WeatherDelay int,
    NASDelay int,
    SecurityDelay int,
    LateAircraftDelay int
    )
''')

conn.commit()

for year in range (2000, 2006):
    ontime =pd.read_csv(str(year)+".csv")
    ontime.to_sql('ontime', con=conn, if_exists='append', index=False)
