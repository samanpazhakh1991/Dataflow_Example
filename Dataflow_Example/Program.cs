﻿// See https://aka.ms/new-console-template for more information;
using Billing;
public class Program
{
    public static void Main(string[] args)
    {
        Console.WriteLine("Hello, World!");


        BillingDataflow billingDataflow = new BillingDataflow();

        billingDataflow.DataProvider();
        billingDataflow.ProcessData(10).GetAwaiter().GetResult();

        Console.ReadKey();
    }
}

