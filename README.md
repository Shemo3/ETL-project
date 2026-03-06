
## Results

Rows in table after removing duplicates: 29840

Rows that could not be processed (invalid rows): 49

## Project Overview

This project implements an ETL pipeline that processes trip data from a CSV file
and loads it into a Microsoft SQL Server database.

The pipeline performs:
- Data extraction from CSV
- Data transformation
- Batch loading into a table
- Duplicate detection and logging

## Changes for larger CSV files

The current implementation processes the file using streaming and batch-based loading, which ensures constant memory usage regardless of file size. If the dataset grows significantly, further optimizations could include increasing the batch size, temporarily disabling indexes during loading, and using table locks with SqlBulkCopy.

## Pipeline

The system uses the following pipeline:

    CSV File
   
      ↓
   
    Extractor (reads data line by line)
   
      ↓
   
    Transformer (converts raw data to typed model)
   
      ↓
   
    Loader (bulk inserts into SQL Server)
   
      ↓
   
    Deduplicator (detects and removes duplicates)
   
      ↓
   
    DuplicateExporter (exports duplicates to CSV)

## Database Schema

```sql
Trips
Id INT IDENTITY(1,1) PRIMARY KEY,
TpepPickupDateTime DATETIME,
TpepDropoffDateTime DATETIME,
PassengerCount INT,
TripDistance DECIMAL(10,2),
StoreAndFwdFlag VARCHAR(5),
PULocationId INT,
DOLocationId INT,
FareAmount DECIMAL(10,2),
TipAmount DECIMAL(10,2),

TravelTime AS DATEDIFF(SECOND, TpepPickupDateTime, TpepDropoffDateTime)
```

Indexes used for query optimization:
- IX_Trips_PULocationId on Trips (PULocationId)
- IX_Trips_TripDistance on Trips (TripDistance DESC)
- IX_Trips_TravelTime on Trips (TravelTime DESC)
- IX_Trips_Dedup on Trips (TpepPickupDateTime, TpepDropoffDateTime, PassengerCount)

## How to Run

1. Restore dependencies
2. Build the project

dotnet build

3. Run the ETL pipeline:

dotnet run -- import --file "data.csv" --connection "Server=...;Database=..."
