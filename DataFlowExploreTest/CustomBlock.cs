using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace DataFlowExploreTest
{
    public class CustomBlock<T> : IPropagatorBlock<T, T[]>
    {
        private readonly IPropagatorBlock<T, T[]> dataflow;

        public CustomBlock(int windowSize) 
        {
            var queue = new Queue<T>();

            // The source part of the propagator holds arrays of size windowSize
            // and propagates data out to any connected targets.
            var source = new BufferBlock<T[]>();

            // The target part receives data and adds them to the queue.
            var target = new ActionBlock<T>(item =>
            {
                // Add the item to the queue.
                queue.Enqueue(item);
                // Remove the oldest item when the queue size exceeds the window size.
                if (queue.Count > windowSize)
                    queue.Dequeue();
                // Post the data in the queue to the source block when the queue size
                // equals the window size.
                if (queue.Count == windowSize)
                    source.Post(queue.ToArray());
            });

            // When the target is set to the completed state, propagate out any
            // remaining data and set the source to the completed state.
            target.Completion.ContinueWith(delegate
            {
                if (queue.Count > 0 && queue.Count < windowSize)
                    source.Post(queue.ToArray());
                source.Complete();
            });

            // Return a IPropagatorBlock<T, T[]> object that encapsulates the
            // target and source blocks.
            dataflow = DataflowBlock.Encapsulate(target, source);
        }
        public Task Completion => dataflow.Completion;

        public void Complete()
        {
            dataflow.Complete();
        }

        public T[]? ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<T[]> target, out bool messageConsumed)
        {
            return dataflow.ConsumeMessage(messageHeader, target, out messageConsumed);
        }

        public void Fault(Exception exception)
        {
            dataflow.Fault(exception);
        }

        public IDisposable LinkTo(ITargetBlock<T[]> target, DataflowLinkOptions linkOptions)
        {
            return dataflow.LinkTo(target, linkOptions);
        }

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, T messageValue, ISourceBlock<T>? source, bool consumeToAccept)
        {
            return dataflow.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
        }

        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<T[]> target)
        {
            dataflow.ReleaseReservation(messageHeader, target);
        }

        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<T[]> target)
        {
            return dataflow.ReserveMessage(messageHeader, target);
        }
    }
}
