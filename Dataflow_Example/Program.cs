// See https://aka.ms/new-console-template for more information;
using Billing;
using System.Diagnostics;

public class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Hello, World!");

        var stopWatch = new Stopwatch();
        stopWatch.Start();

        await startSharded(new BillingWithConccurentDictionary()).ConfigureAwait(false);
        // await startSharded(new BillingWithSortedDictionary()).ConfigureAwait(false);
        stopWatch.Stop();
        Console.WriteLine($"Duration : {stopWatch.Elapsed.TotalSeconds}");
        Console.WriteLine("Finished! press any key to Exit...");
        Console.ReadKey();
    }

    private static async Task startSharded(IBillingDataflow billingDataflow)
    {
        await billingDataflow.ProcessData(TimeSpan.FromMinutes(2)).ConfigureAwait(false);
    }
}

