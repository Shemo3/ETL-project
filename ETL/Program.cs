using ETL_project;
using Microsoft.Data.SqlClient;

internal class Program
{
    static void Main(string[] args)
    {
        if (args.Length == 0)
        {
            ShowUsage();
            return;
        }

        if (args[0] != "import")
        {
            Console.WriteLine("Unknown command.");
            ShowUsage();
            return;
        }

        string? filePath = null;
        string? connectionString = null;

        for (int i = 1; i < args.Length; i++)
        {
            if (args[i] == "--file" && i + 1 < args.Length)
            {
                filePath = args[i + 1];
                i++;
            }
            else if (args[i] == "--connection" && i + 1 < args.Length)
            {
                connectionString = args[i + 1];
                i++;
            }
        }

        if (filePath == null || connectionString == null)
        {
            Console.WriteLine("Missing required arguments.");
            ShowUsage();
            return;
        }

        if (!File.Exists(filePath))
        {
            Console.WriteLine("File is not found. File path: " + filePath);
            return;
        }

        try
        {
            using var fileStream = File.Open(filePath, FileMode.Open);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            return;
        }

        try
        {
            using var connection = new SqlConnection(connectionString);
        }
        catch (Exception e)
        {
            Console.WriteLine("Cannot connect to the database");
            return;
        }

        Console.WriteLine($"File path: {filePath}");
        Console.WriteLine($"Connection string: {connectionString}");
        
        var extractor = new Extractor(filePath);
        var transformer = new DataTransformer();
        var loader = new TripLoader(connectionString);
        var tripDeduplicator = new TripDeduplicator(connectionString);
        var duplicatesExporter = new DuplicateExporter();
        long nOfInvalidRows = 0;

        const int batchSize = 1000;
        var buffer = new List<TripEntity>(batchSize);
        
        foreach (var rawRow in extractor.Extract())
        {
            var transformationResult = transformer.Transform(rawRow);
            if (transformationResult.IsSuccess)
            {
                buffer.Add(transformationResult.Entity);
            }
            else
            {
                nOfInvalidRows++;
            }

            if (buffer.Count >= batchSize)
            {
                loader.BulkInsert(buffer);
                buffer.Clear();
            }
        }

        if (buffer.Count > 0)
        {
            loader.BulkInsert(buffer);
            buffer.Clear();
        }

        var duplicates = tripDeduplicator.GetDuplicates();
        
        duplicatesExporter.ExportToCsv(duplicates, "duplicates.csv");
        
        tripDeduplicator.DeleteDuplicates();

        Console.WriteLine("Successfully imported data from csv file to database and removed duplicates");
        Console.WriteLine("Number of invalid rows: " + nOfInvalidRows);
    }

    static void ShowUsage()
    {
        Console.WriteLine("Usage:");
        Console.WriteLine("ETL import --file <csv_path> --connection <connection_string>");
    }
    
}
