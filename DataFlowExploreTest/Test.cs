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
            var numbers = new ConcurrentDictionary<int, int>();

            for (int i = 0; i < 10; i++)
            {
                numbers.TryAdd(i, i);
                Thread.Sleep(100);
            }

            var d = numbers.TakeWhile(i => i.Key < 5);



            numbers.TryRemove(1, out var item);



        }
        [Fact]
        public void Test12()
        {
            var dic = new ConcurrentDictionary<long, int>();
            var count = 1000000;
            var failed = 0;
            for (int i = 0; i < count; i++)
            {
                if (!dic.TryAdd(DateTime.Now.Ticks, 0))
                    failed++;
                var a = 1;
            }


            Assert.Equal(0, failed);
        }
    }
}
