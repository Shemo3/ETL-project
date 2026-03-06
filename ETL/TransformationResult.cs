namespace ETL_project;

public class TransformationResult
{
    public TripEntity? Entity { get; set; }

    public string? ErrorMessage { get; set; }

    public bool IsSuccess => Entity != null;
}