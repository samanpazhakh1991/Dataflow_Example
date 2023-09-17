using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;

namespace Billing
{
    public class BillingWithConccurentDictionary : BillingAbstraction, IBillingDataflow
    {
        ConcurrentDictionary<string, long> LicensePlates = new();
        TransformBlock<TollGatePassInfo, TollGatePassInfo> checkDuplicationBlock;
        protected override async Task<Task> SpecificMethods(TimeSpan validityDuration)
        {
            assemblePipeLine(validityDuration);
            await postDataTask();
            Task monitor = monitorTask();
            await Task.WhenAll(
                monitor,
                sendToPoliceBlock.Completion,
                addToBillListBlock.Completion,
                logInvalidLicensePlateBlock.Completion,
                logDuplicateBlock.Completion
                ).ConfigureAwait(false);

            return Task.CompletedTask;

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
        private Task<Task> monitorTask()
        {
            var maxProcess = new List<double>() { 0 };
            var s = 1;//Seconds
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
        private TollGatePassInfo checkDuplicationPlaque(TollGatePassInfo info, TimeSpan validityDuration)
        {
            if (LicensePlates.ContainsKey(info.LicensePlate) && Math.Abs((double)(LicensePlates[info.LicensePlate] - info.DateTime.Ticks)) < validityDuration.Ticks)
            {
                info.IsDuplicate = true;
            }
            else
            {
                info.IsDuplicate = false;
                LicensePlates[info.LicensePlate] = Math.Max(LicensePlates.ContainsKey(info.LicensePlate) ? LicensePlates[info.LicensePlate] : 0, info.DateTime.Ticks);
            }

            Interlocked.Increment(ref processCount);
            return info;
        }

    }


}
