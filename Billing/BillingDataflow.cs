using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;

namespace Billing
{
    public class BillingDataflow
    {
        List<TollGatePassInfo> tollGatePassInfos = new List<TollGatePassInfo>();
        List<Bill> sentToPolice = new List<Bill>();
        List<Bill> listBills = new List<Bill>();
        ConcurrentBag<TollGatePassInfo> invalidLicensePlates = new ConcurrentBag<TollGatePassInfo>();
        ConcurrentBag<TollGatePassInfo> duplicateLicensePlates = new ConcurrentBag<TollGatePassInfo>();
        ConcurrentBag<TollGatePassInfo> LicensePlates = new ConcurrentBag<TollGatePassInfo>();
        public void DataProvider()
        {
            var readData = from line in File.ReadAllLines(@"C:\Users\m.kashi\Downloads\Traffic_Mock_Data.csv").Skip(1)
                           let columns = line.Split(',')
                           select new TollGatePassInfo
                           {
                               Id = int.Parse(columns[0]),
                               LicensePlate = columns[1],
                               DateTime = new DateTime(DateTime.Parse(columns[2]).Year, DateTime.Parse(columns[2]).Month, DateTime.Parse(columns[2]).Day, DateTime.Parse(columns[3]).Hour, DateTime.Parse(columns[3]).Minute, DateTime.Parse(columns[3]).Second),
                               ExpresswayCode = int.Parse(columns[4]),
                               GateCode = int.Parse(columns[5]),
                               CameraCode = int.Parse(columns[6])
                           };
            tollGatePassInfos.AddRange(readData);
        }

        public async Task ProcessData(int validityDuration, int catchLifeTime)
        {
            var validateLicensePlateBlock = new TransformBlock<TollGatePassInfo, TollGatePassInfo>(plaqueValidation, new ExecutionDataflowBlockOptions { EnsureOrdered = false });
            var logInvalidLicensePlateBlock = new ActionBlock<TollGatePassInfo>(i => invalidLicensePlates.Add(i));
            var checkDuplicationBlock = new TransformBlock<TollGatePassInfo, TollGatePassInfo>(i => checkDuplicationPlaque(i, validityDuration));
            var logDuplicateBlock = new ActionBlock<TollGatePassInfo>(i => duplicateLicensePlates.Add(i));
            var createBillBlock = new TransformBlock<TollGatePassInfo, Bill>(calculateBill);
            var bundleItemsToBatchesBlock = new BatchBlock<Bill>(100);
            var broadcastBlock = new BroadcastBlock<IEnumerable<Bill>>(i => i);
            var sendToPoliceBlock = new ActionBlock<IEnumerable<Bill>>(i => sentToPolice.AddRange(i));
            var addToBillListBlock = new ActionBlock<IEnumerable<Bill>>(i => listBills.AddRange(i));

            validateLicensePlateBlock
                .Link(logInvalidLicensePlateBlock, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsValid == false);

            validateLicensePlateBlock
                .Link(checkDuplicationBlock, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsValid == true)
                .Link(logDuplicateBlock, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsDuplicate == true);

            checkDuplicationBlock
                .Link(createBillBlock, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsDuplicate == false)
                .Link(bundleItemsToBatchesBlock, new DataflowLinkOptions { PropagateCompletion = true })
                .Link(broadcastBlock, new DataflowLinkOptions { PropagateCompletion = true })
                .Link(sendToPoliceBlock, new DataflowLinkOptions { PropagateCompletion = true });

            broadcastBlock
                .Link(addToBillListBlock, new DataflowLinkOptions { PropagateCompletion = true });

            Parallel.ForEach(tollGatePassInfos, trafficInfo => validateLicensePlateBlock.SendAsync(trafficInfo).ConfigureAwait(false));

            validateLicensePlateBlock.Complete();

            var catchControll = Task.Run(async () =>
              {
                  while (LicensePlates.TryTake(out var tollGatePassInfo))
                  {
                      await Task.Delay(catchLifeTime);
                  }
              });

            await Task.WhenAll(sendToPoliceBlock.Completion, addToBillListBlock.Completion, logInvalidLicensePlateBlock.Completion, logDuplicateBlock.Completion, catchControll).ConfigureAwait(false);
        }

        private Bill calculateBill(TollGatePassInfo info)
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
        private TollGatePassInfo checkDuplicationPlaque(TollGatePassInfo info, int validityDuration)
        {
            if (LicensePlates.Any(i => i.LicensePlate == info.LicensePlate && Math.Abs((double)(i.DateTime - info.DateTime).TotalMinutes) < validityDuration))
            {
                info.IsDuplicate = true;
            }
            else
            {
                info.IsDuplicate = false;
                LicensePlates.Add(info);
            }
            return info;
        }

        private TollGatePassInfo plaqueValidation(TollGatePassInfo info)
        {
            if (info.LicensePlate.Length == 9)
            {
                info.IsValid = true;
            }
            else
            {
                info.IsValid = false;
            }
            return info;
        }
    }
}
