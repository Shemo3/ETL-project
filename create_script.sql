CREATE TABLE Trips (
Id INT IDENTITY(1, 1) PRIMARY KEY,
TpepPickupDateTime DATETIME,
TpepDropoffDateTime DATETIME,
PassengerCount INT,
TripDistance DECIMAL(10, 2),
StoreAndFwdFlag VARCHAR(5),
PULocationId INT,
DOLocationId INT,
FareAmount DECIMAL(10, 2),
TipAmount DECIMAL(10, 2)
)

CREATE INDEX IX_Trips_Dedup
ON Trips (TpepPickupDateTime, TpepDropoffDateTime, PassengerCount);

CREATE INDEX IX_Trips_PULocationId
ON Trips (PULocationId);

CREATE INDEX IX_Trips_TripDistance
ON Trips (TripDistance DESC);

ALTER TABLE Trips
ADD TravelTime AS DATEDIFF(SECOND, TpepPickupDateTime, TpepDropoffDateTime);

CREATE INDEX IX_Trips_TravelTime
ON Trips (TravelTime DESC);

