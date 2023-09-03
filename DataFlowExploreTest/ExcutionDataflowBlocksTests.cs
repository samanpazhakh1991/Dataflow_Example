using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Xunit;
using Xunit.Abstractions;

namespace DataFlowExploreTest
{
    public class ExcutionDataflowBlocksTests
    {
        readonly ITestOutputHelper output;

        public ExcutionDataflowBlocksTests(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public async Task Simple_ActionBlock_Test()
        {
            var actualList = new List<int>();
            var expectedList = new List<int> { 2, 4, 6, 8, 10 };
            var addToList = new ActionBlock<int>(i =>
            {
                actualList.Add(2 * i);
            });

            for (int i = 1; i < 6; i++)
            {
                await addToList.SendAsync(i).ConfigureAwait(false);
            }
            addToList.Complete();
            await addToList.Completion.ConfigureAwait(false);

            Assert.Equal(expectedList.Count, actualList.Count);
            for (int i = 0; i < 5; i++)
            {
                Assert.Equal(expectedList[i], actualList[i]);
            }
        }
        [Fact]
        public async Task Simple_TransformBlock_With_PropagateCompletion_false_Test()
        {
            var actualList = new List<int>();
            var expectedList = new List<int> { 2, 4, 6, 8, 10 };
            var claculatorBlock = new TransformBlock<int, int>(i => 2 * i);

            var addToListBlock = new ActionBlock<int>(i => actualList.Add(i));

            claculatorBlock.LinkTo(addToListBlock, new DataflowLinkOptions { PropagateCompletion = false });

            for (int i = 1; i < 6; i++)
            {
                await claculatorBlock.SendAsync(i).ConfigureAwait(false);
            }
            claculatorBlock.Complete();
            var task = addToListBlock.Completion;
            var timeOut = Task.Delay(1000);
            var t = await Task.WhenAny(task, timeOut).ConfigureAwait(false);

            Assert.Equal(expectedList.Count, actualList.Count);
            for (int i = 0; i < 5; i++)
            {
                Assert.Equal(expectedList[i], actualList[i]);
            }
            Assert.Equal(timeOut, t);
        }
        [Fact]
        public async Task Simple_TransformBlock_With_PropagateCompletion_true_Test()
        {
            var actualList = new List<int>();
            var expectedList = new List<int> { 2, 4, 6, 8, 10 };
            var claculatorBlock = new TransformBlock<int, int>(i => 2 * i);

            var addToListBlock = new ActionBlock<int>(i => actualList.Add(i));

            claculatorBlock.LinkTo(addToListBlock, new DataflowLinkOptions { PropagateCompletion = true });

            for (int i = 1; i < 6; i++)
            {
                await claculatorBlock.SendAsync(i).ConfigureAwait(false);
            }
            claculatorBlock.Complete();
            var task = addToListBlock.Completion;
            var timeOut = Task.Delay(1000);
            var t = await Task.WhenAny(task, timeOut).ConfigureAwait(false);

            Assert.Equal(expectedList.Count, actualList.Count);
            for (int i = 0; i < 5; i++)
            {
                Assert.Equal(expectedList[i], actualList[i]);
            }
            Assert.Equal(task, t);
        }
        [Fact]
        public async Task Simple_TransformManyBlock_Test()
        {

            var actualList = new List<int>();
            var expectedList = new List<int> { 1, 1, 2, 1, 2, 3, 1, 2, 3, 4, 1, 2, 3, 4, 5 };

            var reverseNumbersBlock = new TransformManyBlock<int, int>(getRange);

            var calculatorBlock = new ActionBlock<int>(i =>
            {
                actualList.Add(i);
            });

            reverseNumbersBlock.LinkTo(calculatorBlock, new DataflowLinkOptions { PropagateCompletion = true });

            for (int i = 1; i < 6; i++)
            {
                await reverseNumbersBlock.SendAsync(i).ConfigureAwait(false);
            }

            reverseNumbersBlock.Complete();

            await calculatorBlock.Completion.ConfigureAwait(false);

            Assert.Equal(expectedList.Count(), actualList.Count());
            for (int i = 0; i < expectedList.Count; i++)
            {
                Assert.Equal(expectedList[i], actualList[i]);
            }

            IEnumerable<int> getRange(int num)
            {
                return Enumerable.Range(1, num);
            }
        }
    }
}