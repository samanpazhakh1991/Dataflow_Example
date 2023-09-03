using System.Threading.Tasks.Dataflow;

namespace Dataflow_Example.Buffering_Blocks
{
    public static class BufferBlockExample
    {
        public static void Do()
        {
            // Create a BufferBlock<int> object.
            var bufferBlock = new BufferBlock<int>();

            // Post several messages to the block.
            for (int i = 0; i < 3; i++)
            {
                bufferBlock.Post(i);
            }

            // Receive the messages back from the block.
            for (int i = 0; i < 3; i++)
            {
                Console.WriteLine(bufferBlock.Receive());
            }

            /* Output:
               0
               1
               2
             */
        }
    }
}
