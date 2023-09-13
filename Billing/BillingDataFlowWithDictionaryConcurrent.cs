using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;
using System.Xml.Linq;
namespace Billing
{
    public class BillingDataFlowWithDictionaryConcurrent
    {
        List<TollGatePassInfo> tollGatePassInfos = new List<TollGatePassInfo>();
        List<Bill> sentToPolice = new List<Bill>();
        List<Bill> listBills = new List<Bill>();
        ConcurrentBag<TollGatePassInfo> invalidLicensePlates = new ConcurrentBag<TollGatePassInfo>();
        ConcurrentBag<TollGatePassInfo> duplicateLicensePlates = new ConcurrentBag<TollGatePassInfo>();
        SortedDictionary<string, long> LicensePlates1 = new();
        SortedDictionary<string, long> LicensePlates2 = new();
        SortedDictionary<string, long> LicensePlates3 = new();
        SortedDictionary<string, long> LicensePlates4 = new();
        SortedDictionary<string, long> LicensePlates5 = new();
        long processCount;
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
        TransformBlock<TollGatePassInfo, TollGatePassInfo> validateLicensePlateBlock, checkDuplicationBlock1, checkDuplicationBlock2, checkDuplicationBlock3, checkDuplicationBlock4, checkDuplicationBlock5;
        ActionBlock<TollGatePassInfo> logInvalidLicensePlateBlock, logDuplicateBlock;
        TransformBlock<TollGatePassInfo, Bill> createBillBlock;
        BatchBlock<Bill> bundleItemsToBatchesBlock;
        ActionBlock<IEnumerable<Bill>> sendToPoliceBlock, addToBillListBlock;
        private CancellationTokenSource cancellationTokenSource;

        public async Task ProcessData(TimeSpan validityDuration)
        {
            processCount = 0;
            cancellationTokenSource = new CancellationTokenSource();

            assemblePipeLine(validityDuration);

            Task postingTask = postDataTask();

            //Task<Task> catchControll = emptyCacheTask(catchLifeTime);

            Task<Task> flowMonitor = monitorTask();

            await Task.WhenAll(postingTask, sendToPoliceBlock.Completion, addToBillListBlock.Completion, logInvalidLicensePlateBlock.Completion, logDuplicateBlock.Completion);

            //cancellationTokenSource.Cancel();

            await await flowMonitor.ConfigureAwait(false);
        }

        private Task<Task> monitorTask()
        {
            var s = 1;
            return Task.Factory.StartNew(async () =>
            {
                while (processCount < 1501000)
                {
                    var oldCount = processCount;
                    await Task.Delay(s * 1000);
                    Console.Clear();
                    Console.WriteLine($"Processed items count : {processCount }");
                    Console.WriteLine($"Processed items per second : {(processCount - oldCount) / s}");
                    Console.WriteLine($"validateLicensePlateBlock Input Count : {validateLicensePlateBlock.InputCount}");
                    Console.WriteLine($"logInvalidLicensePlateBlock Input Count : {logInvalidLicensePlateBlock.InputCount}");
                    Console.WriteLine($"checkDuplicationBlock Input Count : {checkDuplicationBlock1.InputCount}");
                    Console.WriteLine($"checkDuplicationBlock Input Count : {checkDuplicationBlock2.InputCount}");
                    Console.WriteLine($"checkDuplicationBlock Input Count : {checkDuplicationBlock3.InputCount}");
                    Console.WriteLine($"checkDuplicationBlock Input Count : {checkDuplicationBlock4.InputCount}");
                    Console.WriteLine($"checkDuplicationBlock Input Count : {checkDuplicationBlock5.InputCount}");
                    Console.WriteLine($"logDuplicateBlock Input Count : {logDuplicateBlock.InputCount}");
                    Console.WriteLine($"createBillBlock Input Count : {createBillBlock.InputCount}");
                    Console.WriteLine($"bundleItemsToBatchesBlock Input Count : {bundleItemsToBatchesBlock.OutputCount}");
                    Console.WriteLine($"addToBillListBlock Input Count : {addToBillListBlock.InputCount}");
                    Console.WriteLine($"sendToPoliceBlock Input Count : {sendToPoliceBlock.InputCount}");
                    Console.WriteLine("____________________________________");

                }
                validateLicensePlateBlock.Complete();
                Console.WriteLine($"Finished monitoring...");
            });
        }

        //private Task<Task> emptyCacheTask(int catchLifeTime)
        //{
        //    return Task.Factory.StartNew(async () =>
        //    {
        //        while (!cancellationTokenSource.Token.IsCancellationRequested)
        //        {
        //            var expires = LicensePlates.TakeWhile(i => i.Value.LifeTime.Ticks < DateTime.Now.Ticks);
        //            Parallel.ForEach(expires, i => { LicensePlates.TryRemove(i.Key, out var _); });
        //            await Task.Delay(2*catchLifeTime);
        //        }
        //    });
        //}

        private Task postDataTask()
        {
            return Task.Run(() =>
            {
                Parallel.ForEach(tollGatePassInfos, trafficInfo => validateLicensePlateBlock.Post(trafficInfo));

                //validateLicensePlateBlock.Complete();
            });
        }

        private void assemblePipeLine(TimeSpan validityDuration)
        {
            validateLicensePlateBlock = new TransformBlock<TollGatePassInfo, TollGatePassInfo>(plaqueValidation, new ExecutionDataflowBlockOptions { EnsureOrdered = false });
            logInvalidLicensePlateBlock = new ActionBlock<TollGatePassInfo>(i => invalidLicensePlates.Add(i));
            checkDuplicationBlock1 = new TransformBlock<TollGatePassInfo, TollGatePassInfo>(i => checkDuplicationPlaque(i, validityDuration, LicensePlates1), new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
            checkDuplicationBlock2 = new TransformBlock<TollGatePassInfo, TollGatePassInfo>(i => checkDuplicationPlaque(i, validityDuration, LicensePlates2), new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
            checkDuplicationBlock3 = new TransformBlock<TollGatePassInfo, TollGatePassInfo>(i => checkDuplicationPlaque(i, validityDuration, LicensePlates3), new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
            checkDuplicationBlock4 = new TransformBlock<TollGatePassInfo, TollGatePassInfo>(i => checkDuplicationPlaque(i, validityDuration, LicensePlates4), new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
            checkDuplicationBlock5 = new TransformBlock<TollGatePassInfo, TollGatePassInfo>(i => checkDuplicationPlaque(i, validityDuration, LicensePlates5), new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
            logDuplicateBlock = new ActionBlock<TollGatePassInfo>(i => duplicateLicensePlates.Add(i));
            createBillBlock = new TransformBlock<TollGatePassInfo, Bill>(calculateBill);
            bundleItemsToBatchesBlock = new BatchBlock<Bill>(100);
            var broadcastBlock = new BroadcastBlock<IEnumerable<Bill>>(i => i);
            sendToPoliceBlock = new ActionBlock<IEnumerable<Bill>>(i => sentToPolice.AddRange(i));
            addToBillListBlock = new ActionBlock<IEnumerable<Bill>>(i => listBills.AddRange(i));
            validateLicensePlateBlock
                .Link(logInvalidLicensePlateBlock, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsValid == false);

            validateLicensePlateBlock
                .Link(checkDuplicationBlock1, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsValid == true && "12".Contains(i.LicensePlate[0]))
                .Link(logDuplicateBlock, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsDuplicate == true);
            validateLicensePlateBlock
                .Link(checkDuplicationBlock2, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsValid == true && "34".Contains(i.LicensePlate[0]))
                .Link(logDuplicateBlock, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsDuplicate == true);
            checkDuplicationBlock2.Link(createBillBlock, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsDuplicate == false);
            validateLicensePlateBlock
                .Link(checkDuplicationBlock3, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsValid == true && "56".Contains(i.LicensePlate[0]))
                .Link(logDuplicateBlock, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsDuplicate == true);
            checkDuplicationBlock3.Link(createBillBlock, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsDuplicate == false);
            validateLicensePlateBlock
                 .Link(checkDuplicationBlock4, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsValid == true && "78".Contains(i.LicensePlate[0]))
                 .Link(logDuplicateBlock, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsDuplicate == true);
            checkDuplicationBlock4.Link(createBillBlock, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsDuplicate == false);
            validateLicensePlateBlock
                             .Link(checkDuplicationBlock5, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsValid == true && "9".Contains(i.LicensePlate[0]))
                 .Link(logDuplicateBlock, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsDuplicate == true);
            checkDuplicationBlock5.Link(createBillBlock, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsDuplicate == false);


            checkDuplicationBlock1
                .Link(createBillBlock, new DataflowLinkOptions { PropagateCompletion = true }, i => i.IsDuplicate == false)
                .Link(bundleItemsToBatchesBlock, new DataflowLinkOptions { PropagateCompletion = true })
                .Link(broadcastBlock, new DataflowLinkOptions { PropagateCompletion = true })
                .Link(sendToPoliceBlock, new DataflowLinkOptions { PropagateCompletion = true });

            broadcastBlock
                .Link(addToBillListBlock, new DataflowLinkOptions { PropagateCompletion = true });
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
        private TollGatePassInfo checkDuplicationPlaque(TollGatePassInfo info, TimeSpan validityDuration, SortedDictionary<string, long> list)
        {
            if (list.ContainsKey(info.LicensePlate) && Math.Abs((double)(list[info.LicensePlate] - info.DateTime.Ticks)) < validityDuration.Ticks)
            {
                info.IsDuplicate = true;
            }
            else
            {
                info.IsDuplicate = false;
                //info.LifeTime = DateTime.Now.AddMilliseconds(catchLifeTime);

                list[info.LicensePlate] = Math.Max(list.ContainsKey(info.LicensePlate) ? list[info.LicensePlate] : 0, info.DateTime.Ticks);
            }
            Interlocked.Increment(ref processCount);
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
