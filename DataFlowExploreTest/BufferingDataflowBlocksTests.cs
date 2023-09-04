using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Xunit;
using Xunit.Abstractions;

namespace DataFlowExploreTest
{
    public class BufferingDataflowBlocksTests
    {
        readonly ITestOutputHelper output;

        public BufferingDataflowBlocksTests(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public async Task Simple_BufferBlock_Test()

        {
            var actualList = new List<int>();
            var expectedList = new List<int> { 5, 7, 9, 11, 13, -4, -2, 0, 2, 4 };

            var headBufferBlock = new BufferBlock<int>();

            var increaseBlock = new TransformBlock<int, int>(increaseNumber);
            var decreaseBlock = new TransformBlock<int, int>(decreaseNumberAsync);

            var terminalBufferBlock = new BufferBlock<int>();

            headBufferBlock.LinkTo(increaseBlock, new DataflowLinkOptions { PropagateCompletion = false }, n => n % 2 == 0);

            headBufferBlock.LinkTo(decreaseBlock, new DataflowLinkOptions { PropagateCompletion = false }, n => n % 2 != 0);

            increaseBlock.LinkTo(terminalBufferBlock, new DataflowLinkOptions { PropagateCompletion = false });

            decreaseBlock.LinkTo(terminalBufferBlock, new DataflowLinkOptions { PropagateCompletion = false });

            for (int i = 0; i < 10; i++)
            {
                await headBufferBlock.SendAsync(i).ConfigureAwait(false);
            }

            await headBufferBlock.SendAsync(-1000).ConfigureAwait(false);
            await headBufferBlock.SendAsync(-1001).ConfigureAwait(false);

            var task = Task.Factory.StartNew(async () =>
            {
                var c = 0;
                while (true)
                {
                    if (c == 2) return;
                    try
                    {
                        var num = await terminalBufferBlock.ReceiveAsync().ConfigureAwait(false);
                        if ((num == -1000) || (num == -1001))
                        {
                            c++;
                            continue;
                        }
                        actualList.Add(num);

                    }
                    catch (System.Exception)
                    {
                        break;
                    }
                }
            });

            await await task;

            actualList.Sort();
            expectedList.Sort();

            for (int i = 0; i < expectedList.Count; i++)
            {
                Assert.Equal(expectedList[i], actualList[i]);
            }

        }

        private async Task<int> decreaseNumberAsync(int num)
        {
            output.WriteLine(num.ToString());
            if (num == -1001) return num;
            await Task.Delay(10);
            return num - 5;
        }

        private int increaseNumber(int num)
        {
            output.WriteLine(num.ToString());
            if (num == -1000) return num;
            return num + 5;
        }

        //[Fact]
        //public async Task PropagateCompletion_Test()
        //{

        //    var actualList = new List<int>();
        //    var expectedList = new List<int> { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

        //    var headBufferBlock = new BufferBlock<int>();

        //    var increaseBlock = new TransformBlock<int, int>(increaseNumber);
        //    var decreaseBlock = new TransformBlock<int, int>(decreaseNumberAsync);

        //    var terminalBufferBlock = new BufferBlock<int>();

        //    headBufferBlock.LinkTo(increaseBlock, new DataflowLinkOptions { PropagateCompletion = true }, n => n % 2 == 0);

        //    headBufferBlock.LinkTo(decreaseBlock, new DataflowLinkOptions { PropagateCompletion = true }, n => n % 2 != 0);

        //    increaseBlock.LinkTo(terminalBufferBlock);
        //    decreaseBlock.LinkTo(terminalBufferBlock);

        //    for (int i = 0; i < 10; i++)
        //    {
        //        await headBufferBlock.SendAsync(i).ConfigureAwait(false);
        //    }

        //    headBufferBlock.Complete();

        //    await Task.WhenAll(increaseBlock.Completion, decreaseBlock.Completion).ConfigureAwait(false);

        //    while (terminalBufferBlock.TryReceive(out int n))
        //    {
        //        actualList.Add(n);
        //    }

        //    actualList.Sort();

        //    Assert.Equal(expectedList.Count, actualList.Count);
        //    for (int i = 0; i < expectedList.Count; i++)
        //    {
        //        Assert.Equal(expectedList[i], actualList[i]);
        //    }

        //    async Task<int> decreaseNumberAsync(int num)
        //    {
        //        await Task.Delay(10);
        //        return num - 1;
        //    }

        //    int increaseNumber(int num)
        //    {
        //        return num + 1;
        //    }
        //}

        [Fact]
        public async Task Simple_BroadcastBlock_Test()
        {
            var actualList = new ConcurrentBag<int>();
            var expectedList = new List<int> { 3, 4, 8, 12 };
            const int num = 2;

            var broadcastBlock = new BroadcastBlock<int>(i => i);

            var divideBlock = new ActionBlock<int>((i) =>
            {
                var res = i / num;
                actualList.Add(res);
            });
            var multiplyBlock = new ActionBlock<int>(i =>
            {
                var res = i * num;
                actualList.Add(res);
            });
            var addBlock = new ActionBlock<int>(i =>
            {
                var res = i + num;
                actualList.Add(res);
            });
            var substractBlock = new ActionBlock<int>(i =>
            {
                var res = i - num;
                actualList.Add(res);
            });

            broadcastBlock.LinkTo(divideBlock, new DataflowLinkOptions { PropagateCompletion = true });
            broadcastBlock.LinkTo(multiplyBlock, new DataflowLinkOptions { PropagateCompletion = true });
            broadcastBlock.LinkTo(addBlock, new DataflowLinkOptions { PropagateCompletion = true });
            broadcastBlock.LinkTo(substractBlock, new DataflowLinkOptions { PropagateCompletion = true });

            await broadcastBlock.SendAsync(6).ConfigureAwait(false);
            broadcastBlock.Complete();

            await Task.WhenAll(substractBlock.Completion, divideBlock.Completion, addBlock.Completion, multiplyBlock.Completion).ConfigureAwait(false);

            Assert.Equal(expectedList.Count, actualList.Count);
            while (actualList.TryTake(out int n))
            {
                Assert.True(expectedList.Exists(x => x == n));
            }
        }

        [Fact]
        public async Task Simple_WriteOnceBlock_Test()
        {
            var acual = 0;
            var expected = 5;

            var woBlock = new WriteOnceBlock<int>(i => i);

            await woBlock.SendAsync(5);
            acual = await woBlock.ReceiveAsync();
            output.WriteLine(acual.ToString());

            await woBlock.SendAsync(90);
            acual = await woBlock.ReceiveAsync();
            output.WriteLine(acual.ToString());

            Assert.Equal(expected, acual);
        }

        [Fact]
        public void Try_Parallel_Write_WriteOnceBlock_Test()
        {
            int acual = 0;
            int Expected = 0;

            var woBlock = new WriteOnceBlock<int>(i =>
              {
                  return i;
              });

            Parallel.Invoke(
                () => woBlock.Post(1),
                () => woBlock.Post(2),
                () => woBlock.Post(3));

            acual = woBlock.Receive();
            Expected = woBlock.Receive();

            woBlock.Post(10);

            acual = woBlock.Receive();

            Assert.Equal(Expected, acual);
        }


    }
}

