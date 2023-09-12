namespace Billing
{
    public class TollGatePassInfo
    {
        
        public int Id { get; set; }
        public string LicensePlate { get; set; }   
        public DateTime DateTime { get; set; }       
        public int ExpresswayCode { get; set; }    
        public int GateCode { get; set; }   
        public int CameraCode { get; set; }
        public bool IsValid { get; set; }
        public bool IsDuplicate { get; set; }
        public DateTime LifeTime { get; set; }
     
    }
}