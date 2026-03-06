using Microsoft.Data.SqlClient;

namespace ETL_project;

using System.Data;
using System.Data.SqlClient;
using Dapper;

public class TripDeduplicator
{
    private readonly string _connectionString;

    public TripDeduplicator(string connectionString)
    {
        _connectionString = connectionString;
    }

    public List<TripEntity> GetDuplicates()
    {
        using var connection = new SqlConnection(_connectionString);

        var sql = @"
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY TpepPickupDatetime,
                                        TpepDropoffDatetime,
                                        PassengerCount
                           ORDER BY Id
                       ) AS rn
                FROM Trips
            ) t
            WHERE rn > 1
        ";

        return connection.Query<TripEntity>(sql).ToList();
    }

    public void DeleteDuplicates()
    {
        using var connection = new SqlConnection(_connectionString);

        var sql = @"
            DELETE FROM Trips
            WHERE Id IN (
                SELECT Id
                FROM (
                    SELECT Id,
                           ROW_NUMBER() OVER (
                               PARTITION BY TpepPickupDatetime,
                                            TpepDropoffDatetime,
                                            PassengerCount
                               ORDER BY Id
                           ) AS rn
                    FROM Trips
                ) t
                WHERE rn > 1
            )
        ";

        connection.Execute(sql);
    }
}