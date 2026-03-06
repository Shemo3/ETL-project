using System.Globalization;
using CsvHelper;

namespace ETL_project;

public class Extractor
{
    private readonly string _filePath;

    public Extractor(string filePath)
    {
        _filePath = filePath;
    }
    public IEnumerable<string[]> Extract()
    {
        using var reader = new StreamReader(_filePath);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);

        csv.Read();
        csv.ReadHeader();

        while (csv.Read())
        {
            yield return csv.Parser.Record;
        }    
    }
}