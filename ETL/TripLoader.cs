using Microsoft.Data.SqlClient;

namespace ETL_project;

using System.Data;

public class TripLoader
{
    private readonly string _connectionString;

    public TripLoader(string connectionString)
    {
        _connectionString = connectionString;
    }

    public void BulkInsert(List<TripEntity> batch)
    {
        var table = CreateDataTable(batch);

        using var connection = new SqlConnection(_connectionString);
        connection.Open();

        using var bulkCopy = new SqlBulkCopy(connection)
        {
            DestinationTableName = "Trips",
            BatchSize = batch.Count
        };

        bulkCopy.ColumnMappings.Add("TpepPickupDatetime", "TpepPickupDateTime");
        bulkCopy.ColumnMappings.Add("TpepDropoffDatetime", "TpepDropoffDateTime");
        bulkCopy.ColumnMappings.Add("PassengerCount", "PassengerCount");
        bulkCopy.ColumnMappings.Add("TripDistance", "TripDistance");
        bulkCopy.ColumnMappings.Add("StoreAndFwdFlag", "StoreAndFwdFlag");
        bulkCopy.ColumnMappings.Add("PULocationID", "PULocationId");
        bulkCopy.ColumnMappings.Add("DOLocationID", "DOLocationId");
        bulkCopy.ColumnMappings.Add("FareAmount", "FareAmount");
        bulkCopy.ColumnMappings.Add("TipAmount", "TipAmount");

        bulkCopy.WriteToServer(table);
    }

    private DataTable CreateDataTable(List<TripEntity> batch)
    {
        var table = new DataTable();

        table.Columns.Add("TpepPickupDatetime", typeof(DateTime));
        table.Columns.Add("TpepDropoffDatetime", typeof(DateTime));
        table.Columns.Add("PassengerCount", typeof(int));
        table.Columns.Add("TripDistance", typeof(double));
        table.Columns.Add("StoreAndFwdFlag", typeof(string));
        table.Columns.Add("PULocationID", typeof(int));
        table.Columns.Add("DOLocationID", typeof(int));
        table.Columns.Add("FareAmount", typeof(decimal));
        table.Columns.Add("TipAmount", typeof(decimal));

        foreach (var trip in batch)
        {
            table.Rows.Add(
                trip.TpepPickupDatetime,
                trip.TpepDropoffDatetime,
                trip.PassengerCount,
                trip.TripDistance,
                trip.StoreAndFwdFlag,
                trip.PULocationID,
                trip.DOLocationID,
                trip.FareAmount,
                trip.TipAmount
            );
        }

        return table;
    }

}