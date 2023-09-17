// See https://aka.ms/new-console-template for more information;
using System.Threading.Tasks.Dataflow;

public static class DataflowLinkExtensions
{
    public static ISourceBlock<TTarget> Link<TSource, TTarget>(
        this ISourceBlock<TSource> source,
        IPropagatorBlock<TSource, TTarget> target, DataflowLinkOptions? dataflowLinkOptions = null, Predicate<TSource>? predicate = null)
    {
        if (dataflowLinkOptions != null && predicate == null)
        {
            source.LinkTo(target, dataflowLinkOptions);
        }
        if (predicate != null && dataflowLinkOptions == null)
        {
            source.LinkTo(target, predicate);
        }
        if (predicate != null && dataflowLinkOptions != null)
        {
            source.LinkTo(target, dataflowLinkOptions, predicate);
        }
        if (predicate == null && dataflowLinkOptions == null)
        {
            source.LinkTo(target);
        }

        return target;
    }

    public static void Link<TSource>(
        this ISourceBlock<TSource> source, ITargetBlock<TSource> target, DataflowLinkOptions? dataflowLinkOptions = null, Predicate<TSource>? predicate = null)
    {
        if (dataflowLinkOptions != null && predicate == null)
        {
            source.LinkTo(target, dataflowLinkOptions);
        }
        if (predicate != null && dataflowLinkOptions == null)
        {
            source.LinkTo(target, predicate);
        }
        if (predicate != null && dataflowLinkOptions != null)
        {
            source.LinkTo(target, dataflowLinkOptions, predicate);
        }
        if (predicate == null && dataflowLinkOptions == null)
        {
            source.LinkTo(target);
        }
    }
}

