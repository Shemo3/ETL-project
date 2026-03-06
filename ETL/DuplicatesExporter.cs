namespace ETL_project;

using System.Globalization;
using CsvHelper;

public class DuplicateExporter
{
    public void ExportToCsv(List<TripEntity> duplicates, string filePath)
    {
        using var writer = new StreamWriter(filePath);
        using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);

        csv.WriteRecords(duplicates);
    }
}