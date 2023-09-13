// See https://aka.ms/new-console-template for more information;
using Billing;
using System.Collections.Concurrent;

public class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Hello, World!");

       
        BillingDataflow billingDataflow = new BillingDataflow();

        billingDataflow.DataProvider();
        await billingDataflow.ProcessData(TimeSpan.FromMinutes(2)).ConfigureAwait(false);
        Console.WriteLine("Finished! press any key to Exit...");
        Console.ReadKey();
    }
}

