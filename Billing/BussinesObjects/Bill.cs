namespace Billing
{
    public class Bill
    {
        public Guid Id { get; set; }
        public string LicensePlate { get; set; }
        public int Amount { get; set; }
        public int ExpresswayCode { get; set; }
        public DateTime DateTime { get; set; }  
    }
}
