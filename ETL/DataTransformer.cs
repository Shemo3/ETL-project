using System.Globalization;

namespace ETL_project;

public class DataTransformer
{
    public TransformationResult Transform(string[] row)
    {
        var est = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
        string format = "MM/dd/yyyy hh:mm:ss tt";
        try
        {
            var entity = new TripEntity()
            {
                TpepPickupDatetime =
                    TimeZoneInfo.ConvertTimeToUtc(DateTime.ParseExact(row[1], format, CultureInfo.InvariantCulture),
                        est),
                TpepDropoffDatetime =
                    TimeZoneInfo.ConvertTimeToUtc(DateTime.ParseExact(row[2], format, CultureInfo.InvariantCulture),
                        est),
                PassengerCount = Int32.Parse(row[3]),
                TripDistance = Double.Parse(row[4], CultureInfo.InvariantCulture),
                StoreAndFwdFlag = (row[6].Trim() == "Y") ? "Yes" : "No",
                PULocationID = Int32.Parse(row[7]),
                DOLocationID = Int32.Parse(row[8]),
                FareAmount = Decimal.Parse(row[10], CultureInfo.InvariantCulture),
                TipAmount = Decimal.Parse(row[13], CultureInfo.InvariantCulture)
            };

            return new TransformationResult()
            {
                Entity = entity
            };
        }
        catch (Exception e)
        {
            return new TransformationResult()
            {
                ErrorMessage = e.Message
            };
        }
    }
}