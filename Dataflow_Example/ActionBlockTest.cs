using System.Threading.Tasks.Dataflow;

namespace Dataflow_Example
{
    public static class ActionBlockTest
    {
        public static void Do(int number)
        {
            var checkNumber = new ActionBlock<int>(t =>
            {
                if (t % 2 == 0)
                {
                    Console.WriteLine($"{t} is even");
                }
                else
                {
                    Console.WriteLine($"{t} is odd");
                }
            });

            checkNumber.Post(number);
            checkNumber.Complete();
        }
    }
}
