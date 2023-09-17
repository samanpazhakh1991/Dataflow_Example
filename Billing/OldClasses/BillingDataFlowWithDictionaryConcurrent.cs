using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;
namespace Billing
{
    public class BillingDataFlowWithDictionaryConcurrent : IBillingDataflow
    {
        List<TollGatePassInfo> tollGatePassInfos = new List<TollGatePassInfo>();
        List<Bill> sentToPolice = new List<Bill>();
        List<Bill> listBills = new List<Bill>();
        ConcurrentBag<TollGatePassInfo> invalidLicensePlates = new ConcurrentBag<TollGatePassInfo>();
        ConcurrentBag<TollGatePassInfo> duplicateLicensePlates = new ConcurrentBag<TollGatePassInfo>();
        ConcurrentDictionary<string, long> LicensePlates = new();

        long processCount;
        long dataCount = 0;

        //LicensePlate = new Random().Next(100000000, 999999999).ToString(),
        //LicensePlate = columns[1]
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

        TransformBlock<TollGatePassInfo, TollGatePassInfo> validateLicensePlateBlock, checkDuplicationBlock;

        ActionBlock<TollGatePassInfo> logInvalidLicensePlateBlock, logDuplicateBlock;
        TransformBlock<TollGatePassInfo, Bill> createBillBlock;
        BatchBlock<Bill> bundleItemsToBatchesBlock;
        ActionBlock<IEnumerable<Bill>> sendToPoliceBlock, addToBillListBlock;
        private CancellationTokenSource cancellationTokenSource;

        public async Task ProcessData(TimeSpan validityDuration)
        {
            dataProvider();
            processCount = 0;
            cancellationTokenSource = new CancellationTokenSource();

            assemblePipeLine(validityDuration);

            Task postingTask = postDataTask();

            //Task<Task> catchControll = emptyCacheTask(catchLifeTime);

            Task<Task> flowMonitor = monitorTask();

            await Task.WhenAll(postingTask, sendToPoliceBlock.Completion, addToBillListBlock.Completion, logInvalidLicensePlateBlock.Completion, logDuplicateBlock.Completion);



            await await flowMonitor.ConfigureAwait(false);


        }

        private Task<Task> monitorTask()
        {
            var maxProcess = new List<double>() { 0 };
            var s = 1;
            return Task.Factory.StartNew(async () =>
            {
                while (processCount < tollGatePassInfos.Count)
                {
                    var oldCount = processCount;
                    await Task.Delay((int)(s * 1000));
                    Console.CursorTop = 0;
                    Console.CursorLeft = 0;
                    Console.WriteLine($"Processed items count : {processCount}");
                    Console.WriteLine($"Processed items per second : {(processCount - oldCount) / s}");
                    Console.WriteLine($"validateLicensePlateBlock Input Count : {validateLicensePlateBlock.InputCount}");
                    Console.WriteLine($"logInvalidLicensePlateBlock Input Count : {logInvalidLicensePlateBlock.InputCount}");
                    Console.WriteLine($"checkDuplicationBlock Input Count : {checkDuplicationBlock.InputCount}");
                    Console.WriteLine($"logDuplicateBlock Input Count : {logDuplicateBlock.InputCount}");
                    Console.WriteLine($"createBillBlock Input Count : {createBillBlock.InputCount}");
                    Console.WriteLine($"bundleItemsToBatchesBlock ' Input Count : {bundleItemsToBatchesBlock.OutputCount}");
                    Console.WriteLine($"addToBillListBlock Input Count : {addToBillListBlock.InputCount}");
                    Console.WriteLine($"sendToPoliceBlock Input Count : {sendToPoliceBlock.InputCount}");
                    Console.WriteLine("____________________________________");

                    maxProcess.Add((processCount - oldCount) / s);
                }
                validateLicensePlateBlock.Complete();
                Console.WriteLine($"Maximum proceed data per Second : {maxProcess.Max()}");
                Console.WriteLine($"Monitoring Duration  : {maxProcess.Count - 1}");
                Console.WriteLine($"Sent To Police {sentToPolice.Count}");
                Console.WriteLine($"Duplicated License count : {duplicateLicensePlates.Count}");
                Console.WriteLine($"Invalid License count : {invalidLicensePlates.Count}");
                Console.WriteLine($"Finished monitoring...");
                Console.WriteLine($"This amount of inputs did not proccesed :  {tollGatePassInfos.Count - (sentToPolice.Count + invalidLicensePlates.Count + duplicateLicensePlates.Count)}");
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
                Parallel.ForEach(tollGatePassInfos, trafficInfo => { validateLicensePlateBlock.Post(trafficInfo); });

                //validateLicensePlateBlock.Complete();
            });
        }

        private void assemblePipeLine(TimeSpan validityDuration)
        {
            validateLicensePlateBlock = new TransformBlock<TollGatePassInfo, TollGatePassInfo>(plaqueValidation, new ExecutionDataflowBlockOptions { EnsureOrdered = false });

            logInvalidLicensePlateBlock = new ActionBlock<TollGatePassInfo>(i => invalidLicensePlates.Add(i));

            checkDuplicationBlock = new TransformBlock<TollGatePassInfo, TollGatePassInfo>(i => checkDuplicationPlaque(i, validityDuration), new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });


            logDuplicateBlock = new ActionBlock<TollGatePassInfo>(i => duplicateLicensePlates.Add(i));
            createBillBlock = new TransformBlock<TollGatePassInfo, Bill>(calculateBill);
            bundleItemsToBatchesBlock = new BatchBlock<Bill>(100);
            var broadcastBlock = new BroadcastBlock<IEnumerable<Bill>>(i => i);
            sendToPoliceBlock = new ActionBlock<IEnumerable<Bill>>(i => sentToPolice.AddRange(i));
            addToBillListBlock = new ActionBlock<IEnumerable<Bill>>(i => listBills.AddRange(i));


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
        private TollGatePassInfo checkDuplicationPlaque(TollGatePassInfo info, TimeSpan validityDuration)
        {
            if (LicensePlates.ContainsKey(info.LicensePlate) && Math.Abs((double)(LicensePlates[info.LicensePlate] - info.DateTime.Ticks)) < validityDuration.Ticks)
            {
                info.IsDuplicate = true;
            }
            else
            {
                info.IsDuplicate = false;
                //info.LifeTime = DateTime.Now.AddMilliseconds(catchLifeTime);

                LicensePlates[info.LicensePlate] = Math.Max(LicensePlates.ContainsKey(info.LicensePlate) ? LicensePlates[info.LicensePlate] : 0, info.DateTime.Ticks);
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
                Interlocked.Increment(ref processCount);

                info.IsValid = false;
            }
            return info;
        }
    }
}
