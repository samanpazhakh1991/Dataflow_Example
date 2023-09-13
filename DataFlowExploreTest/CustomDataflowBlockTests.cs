using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace DataFlowExploreTest
{
    public class CustomDataflowBlockTests
    {
        [Fact]
        public async Task Assemble_Custom_Dataflow_Block_Test()
        {
            List<List<int>> numbers = new();

            var customBlock = new CustomBlock<int>(3);

            var actionBlock = new ActionBlock<IEnumerable<int>>(i => numbers.Add(i.ToList()));

            customBlock.LinkTo(actionBlock, new DataflowLinkOptions { PropagateCompletion = true });

            for (int i = 1; i <= 5; i++)
            {
                await customBlock.SendAsync(i).ConfigureAwait(false);
            }

            customBlock.Complete();

            await actionBlock.Completion.ConfigureAwait(false);

            Assert.Equal(3, numbers.Count);
        }


    }
}
