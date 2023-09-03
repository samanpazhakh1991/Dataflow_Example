using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Xunit;
using Xunit.Abstractions;

namespace DataFlowExploreTest
{
    public class GroupingBlocksTests
    {
        private readonly ITestOutputHelper output;

        public GroupingBlocksTests(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public async Task Simple_BatchBlock_Test()
        {
            int actualNumber = 0;
            int expectedNumber = 10;

            var batchBlock = new BatchBlock<int>(5);

            var actionBlock = new ActionBlock<IEnumerable<int>>(n => actualNumber = n.Sum());

            batchBlock.LinkTo(actionBlock, new DataflowLinkOptions { PropagateCompletion = true });

            for (int i = 0; i < 5; i++)
            {
                await batchBlock.SendAsync(i).ConfigureAwait(false);
            }

            batchBlock.Complete();

            await actionBlock.Completion.ConfigureAwait(false);

            Assert.Equal(expectedNumber, actualNumber);
        }

        [Fact]
        public async Task BatchBlock_With_Greedy_true_Test()
        {
            var actualList = new List<int>();
            var expectedList = new List<int> { 0, 1, 2, 3, 4, 5, 6, 7, 8 };

            var producer1 = new BufferBlock<int>();
            var producer2 = new BufferBlock<int>();

            var batchBlock = new BatchBlock<int>(4, new GroupingDataflowBlockOptions
            {
                Greedy = true // this is the default so you can omit
            });

            var actionBlock = new ActionBlock<IEnumerable<int>>(i => actualList.AddRange(i));

            producer1.LinkTo(batchBlock, new DataflowLinkOptions { PropagateCompletion = true });
            producer2.LinkTo(batchBlock, new DataflowLinkOptions { PropagateCompletion = true });

            batchBlock.LinkTo(actionBlock, new DataflowLinkOptions { PropagateCompletion = true });

            var producer1Send = Task.Factory.StartNew(async () =>
           {
               for (int i = 0; i < 9; i++)
               {
                   await producer1.SendAsync(i).ConfigureAwait(false);
               }
           });
            var producer2Send = Task.Factory.StartNew(async () =>
            {
                await Task.Delay(10);
                for (int i = 100; i < 109; i++)
                {
                    await producer2.SendAsync(i).ConfigureAwait(false);
                }
            });

            await Task.WhenAll(producer1Send.Result, producer2Send.Result).ConfigureAwait(false);

            batchBlock.Complete();

            await actionBlock.Completion.ConfigureAwait(false);

            for (int i = 0; i < expectedList.Count; i++)
            {
                Assert.Equal(expectedList[i], actualList[i]);
            }
        }

        [Fact]
        public async Task BatchBlock_With_Greedy_false_Test()
        {
            var actualList = new List<int>();
            var expectedList = new List<int> { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

            var producer1 = new BufferBlock<int>();
            var producer2 = new BufferBlock<int>();

            var batchBlock = new BatchBlock<int>(2, new GroupingDataflowBlockOptions
            {
                Greedy = false // this is the default so you can omit
            });

            var actionBlock = new ActionBlock<IEnumerable<int>>(i => actualList.AddRange(i));

            producer1.LinkTo(batchBlock, new DataflowLinkOptions { PropagateCompletion = true });
            producer2.LinkTo(batchBlock, new DataflowLinkOptions { PropagateCompletion = true });

            batchBlock.LinkTo(actionBlock, new DataflowLinkOptions { PropagateCompletion = true });

            var producer1Send = Task.Factory.StartNew(async () =>
            {
                for (int i = 0; i < 5; i++)
                {
                    await Task.Delay(10);
                    await producer1.SendAsync(i);
                }
            });

            var producer2Send = Task.Factory.StartNew(async () =>
            {
                for (int i = 5; i < 10; i++)
                {
                    await producer2.SendAsync(i);
                }
            });


            await Task.WhenAll(producer1Send.Result, producer2Send.Result).ConfigureAwait(false);

            batchBlock.Complete();

            await actionBlock.Completion.ConfigureAwait(false);

            actualList.Sort();

            for (int i = 0; i < expectedList.Count; i++)
            {
                Assert.Equal(expectedList[i], actualList[i]);
            }
        }

        [Fact]
        public async Task BatchBlock_with_sources_bigger_than_batch_size_and_greedy_false_Test()
        {
            var actualList = new List<List<int>>();

            var source1 = new BufferBlock<int>();
            var source2 = new BufferBlock<int>();
            var source3 = new BufferBlock<int>();

            var batchBlock = new BatchBlock<int>(2, new GroupingDataflowBlockOptions { Greedy = false });

            var terminalBlock = new ActionBlock<IEnumerable<int>>(i => actualList.Add(i.ToList()));

            source1.LinkTo(batchBlock);
            source2.LinkTo(batchBlock);
            source3.LinkTo(batchBlock);

            batchBlock.LinkTo(terminalBlock, new DataflowLinkOptions { PropagateCompletion = true });

            var t1 = Task.Factory.StartNew(async () =>
            {
                for (int i = 1; i <= 5; i++)
                {
                    await source1.SendAsync(i).ConfigureAwait(false);
                }
                source1.Complete();
            });

            var t2 = Task.Factory.StartNew(async () =>
            {
                for (int i = 6; i <= 10; i++)
                {
                    await source2.SendAsync(i).ConfigureAwait(false);
                }
                source2.Complete();
            });

            var t3 = Task.Factory.StartNew(async () =>
            {
                for (int i = 11; i <= 15; i++)
                {
                    await source3.SendAsync(i).ConfigureAwait(false);
                }
                source3.Complete();
            });

            await Task.WhenAll(t1.Result, t2.Result, t3.Result).ConfigureAwait(false);
            var i = 0;
            while (i < 2)
            {
                if (source1.Completion.IsCompleted) i++;
                if (source2.Completion.IsCompleted) i++;
                if (source3.Completion.IsCompleted) i++;
                await Task.Delay(1).ConfigureAwait(false);
            };
            batchBlock.Complete();
            await terminalBlock.Completion.ConfigureAwait(false);

            var s1Count = actualList.SelectMany(x => x).Count(x => x >= 1 && x <= 5);
            var s2Count = actualList.SelectMany(x => x).Count(x => x >= 6 && x <= 10);
            var s3Count = actualList.SelectMany(x => x).Count(x => x >= 11 && x <= 15);

            output.WriteLine($"S1: {s1Count}|| S2: {s2Count}|| S3: {s3Count}");
            i = 0;
            if (s1Count == 5) i++;
            if (s2Count == 5) i++;
            if (s3Count == 5) i++;
            Assert.Equal(2, i);
            Assert.True(s1Count + s2Count + s3Count >= 12);
            Assert.True(s1Count + s2Count + s3Count <= 14);
        }

        [Fact]
        public async Task BatchBlock_with_batch_size_bigger_than_sources_and_greedy_false_Test()
        {

            var actualList = new List<string>();

            var source1 = new BufferBlock<string>();
            var source2 = new BufferBlock<string>();
            var source3 = new BufferBlock<string>();

            var batchBlock = new BatchBlock<string>(4, new GroupingDataflowBlockOptions { Greedy = false });

            var terminalBlock = new ActionBlock<IEnumerable<string>>(i => actualList.AddRange(i));

            source1.LinkTo(batchBlock, new DataflowLinkOptions { PropagateCompletion = true });
            source2.LinkTo(batchBlock, new DataflowLinkOptions { PropagateCompletion = true });
            source3.LinkTo(batchBlock, new DataflowLinkOptions { PropagateCompletion = true });

            batchBlock.LinkTo(terminalBlock, new DataflowLinkOptions { PropagateCompletion = true });

            var t1 = Task.Run(async () =>
            {
                for (int i = 1; i <= 5; i++)
                {
                    await source1.SendAsync($"source1 item {i}").ConfigureAwait(false);
                }
            });

            var t2 = Task.Run(async () =>
            {
                for (int i = 1; i <= 5; i++)
                {
                    await source2.SendAsync($"source2 item {i}").ConfigureAwait(false);
                    await Task.Delay(1);
                }
            });

            var t3 = Task.Run(async () =>
            {
                for (int i = 1; i <= 5; i++)
                {
                    await source3.SendAsync($"source3 item {i}").ConfigureAwait(false);
                }
            });

            await Task.WhenAll(t1, t2, t3).ConfigureAwait(false);

            source1.Complete();

            await Task.WhenAny(terminalBlock.Completion, Task.Delay(1)).ConfigureAwait(false);

            Assert.Empty(actualList);

        }

        [Fact]
        public async Task BatchBlock_with_sources_equal_to_batch_size_and_greedy_false_Test()
        {
            var actualList = new List<string>();

            var source1 = new BufferBlock<string>();
            var source2 = new BufferBlock<string>();
            var source3 = new BufferBlock<string>();

            var batchBlock = new BatchBlock<string>(3, new GroupingDataflowBlockOptions { Greedy = false });

            var terminalBlock = new ActionBlock<IEnumerable<string>>(i => actualList.AddRange(i));

            source1.LinkTo(batchBlock, new DataflowLinkOptions { PropagateCompletion = true });
            source2.LinkTo(batchBlock, new DataflowLinkOptions { PropagateCompletion = true });
            source3.LinkTo(batchBlock, new DataflowLinkOptions { PropagateCompletion = true });

            batchBlock.LinkTo(terminalBlock, new DataflowLinkOptions { PropagateCompletion = true });

            var t1 = Task.Run(async () =>
            {
                for (int i = 1; i <= 5; i++)
                {
                    await source1.SendAsync($"source1 item {i}").ConfigureAwait(false);
                }
            });

            var t2 = Task.Run(async () =>
            {
                for (int i = 1; i <= 5; i++)
                {
                    await source2.SendAsync($"source2 item {i}").ConfigureAwait(false);
                    await Task.Delay(1);
                }
            });

            var t3 = Task.Run(async () =>
            {
                for (int i = 1; i <= 5; i++)
                {
                    await source3.SendAsync($"source3 item {i}").ConfigureAwait(false);
                }
            });

            await Task.WhenAll(t1, t2, t3).ConfigureAwait(false);
            source1.Complete();

            await terminalBlock.Completion.ConfigureAwait(false);

            Assert.Equal(15, actualList.Count);

        }

        [Fact]
        public async Task Simple_JoinBlock_Test()
        {
            var actual = new List<string>();
            var expected = new List<string>()
            {
                "Person : Person1 Id is: 1",
                "Person : Person2 Id is: 2",
                "Person : Person3 Id is: 3",
                "Person : Person4 Id is: 4",
                "Person : Person5 Id is: 5",
                "Person : Person6 Id is: 6",

        };

            var namesBufferBlock = new BufferBlock<string>();
            var idBufferBlock = new BufferBlock<int>();

            var joinBlock = new JoinBlock<string, int>(new GroupingDataflowBlockOptions { Greedy = true });


            var terminalBlock = new ActionBlock<Tuple<string, int>>(i => actual.Add($"Person : {i.Item1} Id is: {i.Item2.ToString()}"));

            namesBufferBlock.LinkTo(joinBlock.Target1, new DataflowLinkOptions { PropagateCompletion = true });
            idBufferBlock.LinkTo(joinBlock.Target2, new DataflowLinkOptions { PropagateCompletion = true });

            joinBlock.LinkTo(terminalBlock, new DataflowLinkOptions { PropagateCompletion = true });



            for (int i = 1; i <= 5; i++)
            {
                await namesBufferBlock.SendAsync($"Person{i}");
            }

            for (int i = 1; i <= 6; i++)
            {
                await idBufferBlock.SendAsync(i);
            }
            var s1Countbefore = namesBufferBlock.Count;
            var s2Countbefore = idBufferBlock.Count;
            var batchCountbefore = joinBlock.OutputCount;
            //var joininput = joinBlock
            //joinBlock.Complete();

            await Task.Delay(10);

            var s1CountAmoung = namesBufferBlock.Count;
            var s2CountAmoung = idBufferBlock.Count;
            var batchCountAmoung = joinBlock.OutputCount;


            await terminalBlock.Completion.ConfigureAwait(false);
            var s1CountAfter = namesBufferBlock.Count;
            var s2CountAfter = idBufferBlock.Count;
            var batchCountAfter = joinBlock.OutputCount;
            foreach (var item in actual)
            {
                output.WriteLine(item);
            }

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], actual[i]);
            }

        }

        [Fact]
        public async Task JoinBlock_Greedy_false_Test()
        {
            var actual = new List<string>();
            var expected = new List<string>()
            {
                "Person : Person1 Id is: 1",
            };

            var namesBufferBlock = new BufferBlock<string>();
            var idBufferBlock = new BufferBlock<int>();

            var joinBlock = new JoinBlock<string, int>(new GroupingDataflowBlockOptions { Greedy = false });

            var terminalBlock = new ActionBlock<Tuple<string, int>>(i => actual.Add($"Person : {i.Item1} Id is: {i.Item2.ToString()}"));
            // var anotherTerminalBlock = new ActionBlock<string>(i => output.WriteLine($"Message '{i}' consumed by another terminal block"));

            namesBufferBlock.LinkTo(joinBlock.Target1, new DataflowLinkOptions { PropagateCompletion = true });
            idBufferBlock.LinkTo(joinBlock.Target2, new DataflowLinkOptions { PropagateCompletion = true });
            joinBlock.LinkTo(terminalBlock, new DataflowLinkOptions { PropagateCompletion = true });



            //await namesBufferBlock.SendAsync($"Person1").ConfigureAwait(false);
            //await namesBufferBlock.SendAsync($"Person2").ConfigureAwait(false);
            await idBufferBlock.SendAsync(1).ConfigureAwait(false);

            for (int i = 1; i <= 3; i++)
            {
                await namesBufferBlock.SendAsync($"Person{i}").ConfigureAwait(false);
            }

            await Task.Delay(10);

            //namesBufferBlock.LinkTo(anotherTerminalBlock, new DataflowLinkOptions { PropagateCompletion = true });

            namesBufferBlock.Complete();


            await idBufferBlock.SendAsync(2).ConfigureAwait(false);
            idBufferBlock.Complete();

            //  await Task.WhenAll(terminalBlock.Completion, anotherTerminalBlock.Completion).ConfigureAwait(false);
            await terminalBlock.Completion.ConfigureAwait(false);

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], actual[i]);
            }

        }

        [Fact]
        public async Task Simple_BathedJoinedBlock_Test()
        {
            var actual = new List<string>();
            var expected = new List<string>()
            {
                "Person : Person1 Id is: 1",
                "Person : Person2 Id is: 2",
                "Person : Person3 Id is: 3",
                "Person : Person4 Id is: 4",
                "Person : Person5 Id is: 5"
            };

            var namesBufferBlock = new BufferBlock<string>();
            var idBufferBlock = new BufferBlock<int>();

            var joinBlock = new BatchedJoinBlock<string, int>(2);


            var terminalBlock = new ActionBlock<Tuple<IList<string>, IList<int>>>((i) =>
            {
                foreach (var item in i.Item1)
                {
                    var index = i.Item1.IndexOf(item);
                    actual.Add($"Person : {item} Id is: {i.Item2[index]}");
                }
            });
            namesBufferBlock.LinkTo(joinBlock.Target1, new DataflowLinkOptions { PropagateCompletion = true });
            idBufferBlock.LinkTo(joinBlock.Target2, new DataflowLinkOptions { PropagateCompletion = true });
            joinBlock.LinkTo(terminalBlock, new DataflowLinkOptions { PropagateCompletion = true });

            for (int i = 1; i <= 5; i++)
            {
                await idBufferBlock.SendAsync(i).ConfigureAwait(false);
                await namesBufferBlock.SendAsync($"Person{i}").ConfigureAwait(false);
            }
            await Task.Delay(1000);




            //namesBufferBlock.Complete();

            //idBufferBlock.Complete();

            joinBlock.Complete();

            await Task.Delay(100);
            await terminalBlock.Completion.ConfigureAwait(false);

            foreach (var item in actual)
            {
                output.WriteLine(item);
            }

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], actual[i]);
            }
        }

        [Fact]
        public async Task BatchBlock_with_sources_bigger_than_batch_size_and_greedy_false_Test1()
        {
            var actualList = new List<string>();

            var source1 = new BufferBlock<string>();
            var source2 = new BufferBlock<string>();
            var source3 = new BufferBlock<string>();
            var source4 = new BufferBlock<string>();

            var batchBlock = new BatchBlock<string>(3, new GroupingDataflowBlockOptions { Greedy = false });

            var terminalBlock = new ActionBlock<IEnumerable<string>>(i => actualList.AddRange(i));

            source1.LinkTo(batchBlock, new DataflowLinkOptions { PropagateCompletion = true });
            source2.LinkTo(batchBlock, new DataflowLinkOptions { PropagateCompletion = true });
            source3.LinkTo(batchBlock, new DataflowLinkOptions { PropagateCompletion = true });
            source4.LinkTo(batchBlock, new DataflowLinkOptions { PropagateCompletion = true });

            batchBlock.LinkTo(terminalBlock, new DataflowLinkOptions { PropagateCompletion = true });

            var t1 = Task.Factory.StartNew(async () =>
            {
                //await Task.Delay(1);
                for (int i = 1; i <= 1; i++)
                {
                    await source1.SendAsync($"source1 - item {i}").ConfigureAwait(false);
                }
            });
            var t2 = Task.Factory.StartNew(async () =>
            {
                for (int i = 1; i <= 5000; i++)
                {
                    await source2.SendAsync($"source2 - item {i}").ConfigureAwait(false);
                }
            });
            var t3 = Task.Factory.StartNew(async () =>
            {
                for (int i = 1; i <= 5000; i++)
                {
                    await source3.SendAsync($"source3 - item {i}").ConfigureAwait(false);

                }
            });
            var t4 = Task.Factory.StartNew(async () =>
            {
                for (int i = 1; i <= 5000; i++)
                {
                    await source4.SendAsync($"source4 - item {i}").ConfigureAwait(false);

                }
            });


            await Task.WhenAll(t1.Result, t2.Result, t3.Result, t4.Result).ContinueWith(async delegate
            {
                await Task.Delay(20);
                batchBlock.Complete();
            }).ConfigureAwait(false);


            await terminalBlock.Completion.ConfigureAwait(false);

            Assert.Equal(15000, actualList.Count);
        }
        [Fact]
        public async Task jointest()
        {
            var b1 = new BufferBlock<int>();
            var b2 = new BufferBlock<int>();

            var jb = new JoinBlock<int, int>(new GroupingDataflowBlockOptions { Greedy = false });

            var ab = new ActionBlock<Tuple<int, int>>(i => output.WriteLine($"{i.Item1} - {i.Item2}"));

            var ab1 = new ActionBlock<int>(i => output.WriteLine($" *{i}"));

            b1.LinkTo(jb.Target1, new DataflowLinkOptions { PropagateCompletion = true });
            b2.LinkTo(jb.Target2, new DataflowLinkOptions { PropagateCompletion = true });
            jb.LinkTo(ab, new DataflowLinkOptions { PropagateCompletion = true });
            b2.LinkTo(ab1, new DataflowLinkOptions { PropagateCompletion = true }, m => m % 2 == 0);

            for (int i = 0; i < 5; i++)
            {
                b1.Post(i);
                b2.Post(i);
            }
            b2.Post(5);
            b2.Post(6);

            var c1 = b1.Count;
            var c2 = b2.Count;

            b1.Complete();
            b2.Complete();
            await Task.WhenAll(ab.Completion, ab1.Completion);

            c1 = b1.Count;
            c2 = b2.Count;
        }

        [Fact]
        public void BatchJoin_Test()
        {
            var b1 = new BufferBlock<string>();
            var b2 = new BufferBlock<int>();

            var bj = new BatchedJoinBlock<string, int>(2);

            var ab = new ActionBlock<Tuple<IList<string>, IList<int>>>(i =>
            {
                foreach (var item in i.Item1)
                {
                    output.WriteLine(item);
                }
                foreach (var item in i.Item2)
                {
                    output.WriteLine(item.ToString());
                }
                output.WriteLine("-------------");
            });

            b1.LinkTo(bj.Target1, new DataflowLinkOptions { PropagateCompletion = true });
            b2.LinkTo(bj.Target2, new DataflowLinkOptions { PropagateCompletion = true });
            bj.LinkTo(ab, new DataflowLinkOptions { PropagateCompletion = true });

            for (int i = 1; i <= 5; i++)
            {
                b1.Post(i.ToString() + '*');
                b2.Post(i);
            }

            // b2.Post(60);

            b1.Complete();
            b2.Complete();

            ab.Completion.Wait();



        }
    }


}

