using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;

namespace Billing
{
    public abstract class BillingAbstraction : IBillingDataflow
    {
        protected List<TollGatePassInfo> tollGatePassInfos = new List<TollGatePassInfo>();
        protected List<Bill> sentToPolice = new List<Bill>();
        protected List<Bill> listBills = new List<Bill>();
        protected ConcurrentBag<TollGatePassInfo> invalidLicensePlates = new ConcurrentBag<TollGatePassInfo>();
        protected ConcurrentBag<TollGatePassInfo> duplicateLicensePlates = new ConcurrentBag<TollGatePassInfo>();

        protected ActionBlock<TollGatePassInfo> logInvalidLicensePlateBlock, logDuplicateBlock;
        protected TransformBlock<TollGatePassInfo, Bill> createBillBlock;
        protected BatchBlock<Bill> bundleItemsToBatchesBlock;
        protected ActionBlock<IEnumerable<Bill>> sendToPoliceBlock, addToBillListBlock;
        protected TransformBlock<TollGatePassInfo, TollGatePassInfo> validateLicensePlateBlock;

        protected long processCount = 0;
        protected abstract Task SpecificMethods(TimeSpan validityDuration);

        public async Task ProcessData(TimeSpan validityDuration)
        {
            dataProvider();

            await SpecificMethods(validityDuration);

        }

        // LicensePlate = new Random().Next(100000000, 999999999).ToString(),
        //LicensePlate = columns[1],

        private void dataProvider()
        {
            var readData = from line in File.ReadAllLines(@"C:\Data\Traffic_Mock_Data2.csv").Skip(1)
                           let columns = line.Split(',')
                           select new TollGatePassInfo
                           {
                               Id = int.Parse(columns[0]),
                               LicensePlate = new Random().Next(100000000, 999999999).ToString(),
                               DateTime = new DateTime(DateTime.Parse(columns[2]).Year, DateTime.Parse(columns[2]).Month, DateTime.Parse(columns[2]).Day, DateTime.Parse(columns[3]).Hour, DateTime.Parse(columns[3]).Minute, DateTime.Parse(columns[3]).Second),
                               ExpresswayCode = int.Parse(columns[4]),
                               GateCode = int.Parse(columns[5]),
                               CameraCode = int.Parse(columns[6])
                           };

            tollGatePassInfos.AddRange(readData);


        }
        protected Bill calculateBill(TollGatePassInfo info)
        {
            var bill = new Bill()
            {
                Id = Guid.NewGuid(),
                Amount = info.ExpresswayCode * 1000,
                DateTime = info.DateTime,
                ExpresswayCode = info.ExpresswayCode,
                LicensePlate = info.LicensePlate
            };

            return bill;
        }
        protected TollGatePassInfo plaqueValidation(TollGatePassInfo info)
        {
            if (info.LicensePlate.Length == 9)
            {
                info.IsValid = true;
            }
            else
            {
                Interlocked.Increment(ref processCount);

                info.IsValid = false;
            }
            return info;
        }
        protected Task postDataTask()
        {
            return Task.Run(() =>
            {
                Parallel.ForEach(tollGatePassInfos, trafficInfo => { validateLicensePlateBlock.Post(trafficInfo); });

                //validateLicensePlateBlock.Complete();
            });
        }


    }
}
