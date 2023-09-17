namespace Billing
{
    public interface IBillingDataflow
    {
        Task ProcessData(TimeSpan validityDuration);
    }
}