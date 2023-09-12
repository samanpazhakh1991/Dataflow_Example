using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;
using System.Xml.Linq;
namespace Billing
{
    public class BillingDataflow
    {
        List<TollGatePassInfo> tollGatePassInfos = new List<TollGatePassInfo>();
        List<Bill> sentToPolice = new List<Bill>();
        List<Bill> listBills = new List<Bill>();
        ConcurrentBag<TollGatePassInfo> invalidLicensePlates = new ConcurrentBag<TollGatePassInfo>();
        ConcurrentBag<TollGatePassInfo> duplicateLicensePlates = new ConcurrentBag<TollGatePassInfo>();
        ConcurrentDictionary<Guid, TollGatePassInfo> LicensePlates = new ConcurrentDictionary<Guid, TollGatePassInfo>();
        public void DataProvider()
        {
            var readData = from line in File.ReadAllLines(@"C:\Users\m.kashi\Downloads\Traffic_Mock_Data.csv").Skip(1)
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

        public async Task ProcessData(int validityDuration, int catchLifeTime)
        {
            var validateLicensePlateBlock = new TransformBlock<TollGatePassInfo, TollGatePassInfo>(plaqueValidation, new ExecutionDataflowBlockOptions { EnsureOrdered = false });
            var logInvalidLicensePlateBlock = new ActionBlock<TollGatePassInfo>(i => invalidLicensePlates.Add(i));
            var checkDuplicationBlock = new TransformBlock<TollGatePassInfo, TollGatePassInfo>(i => checkDuplicationPlaque(i, validityDuration, catchLifeTime), new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 3 });
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

            Parallel.ForEach(tollGatePassInfos, trafficInfo => validateLicensePlateBlock.SendAsync(trafficInfo));

            validateLicensePlateBlock.Complete();

            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            CancellationToken token = cancellationTokenSource.Token;

            var catchControll = Task.Factory.StartNew(async () =>
              {
                  while (!token.IsCancellationRequested)
                  {
                      var expires = LicensePlates.TakeWhile(i => i.Value.LifeTime.Ticks < DateTime.Now.Ticks);
                      Parallel.ForEach(expires, i => { LicensePlates.TryRemove(i.Key, out var _); });
                      await Task.Delay(100);
                  }
              });

            var flowMonitor = Task.Factory.StartNew(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    await Task.Delay(50);
                    Console.Clear();
                    Console.WriteLine($"validateLicensePlateBlock Input Count : {validateLicensePlateBlock.InputCount}");
                    Console.WriteLine($"logInvalidLicensePlateBlock Input Count : {logInvalidLicensePlateBlock.InputCount}");
                    Console.WriteLine($"checkDuplicationBlock Input Count : {checkDuplicationBlock.InputCount}");
                    Console.WriteLine($"logDuplicateBlock Input Count : {logDuplicateBlock.InputCount}");
                    Console.WriteLine($"createBillBlock Input Count : {createBillBlock.InputCount}");
                    Console.WriteLine($"bundleItemsToBatchesBlock Input Count : {bundleItemsToBatchesBlock.OutputCount}");
                    Console.WriteLine($"addToBillListBlock Input Count : {addToBillListBlock.InputCount}");
                    Console.WriteLine($"sendToPoliceBlock Input Count : {sendToPoliceBlock.InputCount}");
                    Console.WriteLine("____________________________________");
                }
            });

            await Task.WhenAll(sendToPoliceBlock.Completion, addToBillListBlock.Completion, logInvalidLicensePlateBlock.Completion, logDuplicateBlock.Completion).ContinueWith(async delegate
            {
                cancellationTokenSource.Cancel(); cancellationTokenSource.Dispose();
                await Task.WhenAll(catchControll, flowMonitor);
            }).ConfigureAwait(false);

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
        private TollGatePassInfo checkDuplicationPlaque(TollGatePassInfo info, int validityDuration, int catchLifeTime)
        {
            info.LifeTime = DateTime.Now.AddMilliseconds(catchLifeTime);
            if (LicensePlates.Any(i => i.Value.LicensePlate == info.LicensePlate && Math.Abs((double)(i.Value.DateTime - info.DateTime).TotalMinutes) < validityDuration))
            {
                info.IsDuplicate = true;
            }
            else
            {
                info.IsDuplicate = false;
                LicensePlates.TryAdd(Guid.NewGuid(), info);
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
