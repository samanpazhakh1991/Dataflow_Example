using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace DataFlowExploreTest
{
    public class Test
    {
        [Fact]
        public void test1()
        {
            var numbers = new ConcurrentDictionary<int,int>();

            for (int i = 0; i < 10; i++)
            {
                numbers.TryAdd(i,i);
                Thread.Sleep(100);
            }

            var d = numbers.TakeWhile(i => i.Key < 5);


            
            numbers.TryRemove(1,out var item);
            


        }
    }
}
