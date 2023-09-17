using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;
namespace Billing
{
    public class BillingDataflow : IBillingDataflow
    {
        protected List<TollGatePassInfo> tollGatePassInfos = new List<TollGatePassInfo>();
        protected List<Bill> sentToPolice = new List<Bill>();
        protected List<Bill> listBills = new List<Bill>();
        protected ConcurrentBag<TollGatePassInfo> invalidLicensePlates = new ConcurrentBag<TollGatePassInfo>();
        protected ConcurrentBag<TollGatePassInfo> duplicateLicensePlates = new ConcurrentBag<TollGatePassInfo>();
        SortedDictionary<string, long> LicensePlates1 = new();
        SortedDictionary<string, long> LicensePlates2 = new();
        SortedDictionary<string, long> LicensePlates3 = new();
        SortedDictionary<string, long> LicensePlates4 = new();
        SortedDictionary<string, long> LicensePlates5 = new();

        long processCount;
        // LicensePlate = new Random().Next(100000000, 999999999).ToString(),
        //LicensePlate = columns[1],
        private void dataProvider()
        {
            var readData = from line in File.ReadAllLines(@"C:\Data\Traffic_Mock_Data2.csv").Skip(1)
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

            var invalids = tollGatePassInfos.Count(l => l.LicensePlate.Length < 9);
            var dup = tollGatePassInfos.Where(l => l.LicensePlate.Count() > 1).Count();
            Console.WriteLine(invalids);
            Console.WriteLine(dup);

        }
        TransformBlock<TollGatePassInfo, TollGatePassInfo> validateLicensePlateBlock, checkDuplicationBlock1, checkDuplicationBlock2, checkDuplicationBlock3, checkDuplicationBlock4, checkDuplicationBlock5;
        ActionBlock<TollGatePassInfo> logInvalidLicensePlateBlock, logDuplicateBlock;
        TransformBlock<TollGatePassInfo, Bill> createBillBlock;
        BatchBlock<Bill> bundleItemsToBatchesBlock;
        ActionBlock<IEnumerable<Bill>> sendToPoliceBlock, addToBillListBlock;
        private CancellationTokenSource cancellationTokenSource;

        public async Task ProcessData(TimeSpan validityDuration)
        {
            dataProvider();

            cancellationTokenSource = new CancellationTokenSource();

            assemblePipeLine(validityDuration);

            Task postingTask = postDataTask();

            //Task<Task> catchControll = emptyCacheTask(catchLifeTime);

            Task<Task> flowMonitor = monitorTask();

            await Task.WhenAll(postingTask, sendToPoliceBlock.Completion, addToBillListBlock.Completion, logInvalidLicensePlateBlock.Completion, logDuplicateBlock.Completion).ConfigureAwait(false);

            //cancellationTokenSource.Cancel();

            await await flowMonitor.ConfigureAwait(false);




        }

        private Task<Task> monitorTask()
        {

            var maxProcess = new List<double>() { 0 };
            var s = 1;
            return Task.Factory.StartNew(async () =>
            {

                Console.Clear();
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
                    Console.WriteLine($"checkDuplicationBlock 'First' Input Count : {checkDuplicationBlock1.InputCount}");
                    Console.WriteLine($"checkDuplicationBlock 'Second' Input Count : {checkDuplicationBlock2.InputCount}");
                    Console.WriteLine($"checkDuplicationBlock 'Third' Input Count : {checkDuplicationBlock3.InputCount}");
                    Console.WriteLine($"checkDuplicationBlock 'Fourth' Input Count : {checkDuplicationBlock4.InputCount}");
                    Console.WriteLine($"checkDuplicationBlock 'Fifth' Input Count : {checkDuplicationBlock5.InputCount}");
                    Console.WriteLine($"logDuplicateBlock Input Count : {logDuplicateBlock.InputCount}");
                    Console.WriteLine($"createBillBlock Input Count : {createBillBlock.InputCount}");
                    Console.WriteLine($"bundleItemsToBatchesBlock Input Count : {bundleItemsToBatchesBlock.OutputCount}");
                    Console.WriteLine($"addToBillListBlock Input Count : {addToBillListBlock.InputCount}");
                    Console.WriteLine($"sendToPoliceBlock Input Count : {sendToPoliceBlock.InputCount}");
                    Console.WriteLine("____________________________________");

                    maxProcess.Add((processCount - oldCount) / s);

                }

                var invalids = tollGatePassInfos.Count(l => l.LicensePlate.Length < 9);

                Console.WriteLine(invalids);
                //Console.WriteLine(dup);

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
                Parallel.ForEach(tollGatePassInfos, trafficInfo => validateLicensePlateBlock.Post(trafficInfo));

                //validateLicensePlateBlock.Complete();
            });
        }

        private void assemblePipeLine(TimeSpan validityDuration)
        {
            var createOption = new ExecutionDataflowBlockOptions { EnsureOrdered = false };
            validateLicensePlateBlock = new TransformBlock<TollGatePassInfo, TollGatePassInfo>(plaqueValidation, createOption);
            logInvalidLicensePlateBlock = new ActionBlock<TollGatePassInfo>(i => invalidLicensePlates.Add(i), createOption);
            checkDuplicationBlock1 = new TransformBlock<TollGatePassInfo, TollGatePassInfo>(i => checkDuplicationPlaque(i, validityDuration, LicensePlates1), createOption);
            checkDuplicationBlock2 = new TransformBlock<TollGatePassInfo, TollGatePassInfo>(i => checkDuplicationPlaque(i, validityDuration, LicensePlates2), createOption);
            checkDuplicationBlock3 = new TransformBlock<TollGatePassInfo, TollGatePassInfo>(i => checkDuplicationPlaque(i, validityDuration, LicensePlates3), createOption);
            checkDuplicationBlock4 = new TransformBlock<TollGatePassInfo, TollGatePassInfo>(i => checkDuplicationPlaque(i, validityDuration, LicensePlates4), createOption);
            checkDuplicationBlock5 = new TransformBlock<TollGatePassInfo, TollGatePassInfo>(i => checkDuplicationPlaque(i, validityDuration, LicensePlates5), createOption);
            logDuplicateBlock = new ActionBlock<TollGatePassInfo>(i => duplicateLicensePlates.Add(i), createOption);
            createBillBlock = new TransformBlock<TollGatePassInfo, Bill>(calculateBill, createOption);
            bundleItemsToBatchesBlock = new BatchBlock<Bill>(100);
            var broadcastBlock = new BroadcastBlock<IEnumerable<Bill>>(i => i, createOption);
            sendToPoliceBlock = new ActionBlock<IEnumerable<Bill>>(i => sentToPolice.AddRange(i), createOption);
            addToBillListBlock = new ActionBlock<IEnumerable<Bill>>(i => listBills.AddRange(i), createOption);

            var linkOption = new DataflowLinkOptions { PropagateCompletion = true };
            validateLicensePlateBlock
                .Link(logInvalidLicensePlateBlock, linkOption, i => i.IsValid == false);

            validateLicensePlateBlock
                .Link(checkDuplicationBlock1, linkOption, i => i.IsValid == true && "01".Contains(i.LicensePlate[0]))
                .Link(logDuplicateBlock, linkOption, i => i.IsDuplicate == true);
            validateLicensePlateBlock
                .Link(checkDuplicationBlock2, linkOption, i => i.IsValid == true && "23".Contains(i.LicensePlate[0]))
                .Link(logDuplicateBlock, linkOption, i => i.IsDuplicate == true);
            checkDuplicationBlock2.Link(createBillBlock, linkOption, i => i.IsDuplicate == false);
            validateLicensePlateBlock
                .Link(checkDuplicationBlock3, linkOption, i => i.IsValid == true && "45".Contains(i.LicensePlate[0]))
                .Link(logDuplicateBlock, linkOption, i => i.IsDuplicate == true);
            checkDuplicationBlock3.Link(createBillBlock, linkOption, i => i.IsDuplicate == false);
            validateLicensePlateBlock
                 .Link(checkDuplicationBlock4, linkOption, i => i.IsValid == true && "67".Contains(i.LicensePlate[0]))
                 .Link(logDuplicateBlock, linkOption, i => i.IsDuplicate == true);
            checkDuplicationBlock4.Link(createBillBlock, linkOption, i => i.IsDuplicate == false);
            validateLicensePlateBlock
                             .Link(checkDuplicationBlock5, linkOption, i => i.IsValid == true && "89".Contains(i.LicensePlate[0]))
                 .Link(logDuplicateBlock, linkOption, i => i.IsDuplicate == true);
            checkDuplicationBlock5.Link(createBillBlock, linkOption, i => i.IsDuplicate == false);


            checkDuplicationBlock1
                .Link(createBillBlock, linkOption, i => i.IsDuplicate == false)
                .Link(bundleItemsToBatchesBlock, linkOption)
                .Link(broadcastBlock, linkOption)
                .Link(sendToPoliceBlock, linkOption);

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
                Interlocked.Increment(ref processCount);

                info.IsValid = false;
            }
            return info;
        }
    }
}
