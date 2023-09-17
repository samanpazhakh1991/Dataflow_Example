using System.Threading.Tasks.Dataflow;

namespace Billing
{
    public class BillingWithSortedDictionary : BillingAbstraction, IBillingDataflow
    {
        SortedDictionary<string, long> LicensePlates1 = new();
        SortedDictionary<string, long> LicensePlates2 = new();
        SortedDictionary<string, long> LicensePlates3 = new();
        SortedDictionary<string, long> LicensePlates4 = new();
        SortedDictionary<string, long> LicensePlates5 = new();
        TransformBlock<TollGatePassInfo, TollGatePassInfo> checkDuplicationBlock1, checkDuplicationBlock2, checkDuplicationBlock3, checkDuplicationBlock4, checkDuplicationBlock5;
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
        private Task<Task> monitorTask()
        {
            var maxProcess = new List<double>() { 0 };
            var s = 1;//Seconds
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
    }
}
