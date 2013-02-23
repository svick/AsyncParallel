using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Nito.AsyncEx;

namespace AsyncEx
{
    /// <summary>
    /// Provides support for async loops.
    /// </summary>
    public static class AsyncParallel
    {
        private static readonly ParallelOptions DefaultOptions = new ParallelOptions();

        /// <summary>
        /// Executes a <c>foreach</c> loop in which async iterations may run in parallel,
        /// with unbounded parallelism.
        /// </summary>
        /// <typeparam name="T">The type of data in the source.</typeparam>
        /// <param name="source">The enumerable data source.</param>
        /// <param name="body">The asynchronous delegate that is invoked for each item in the source collection.</param>
        /// <returns>A <see cref="Task"/> that represents the completion of the loop.</returns>
        public static Task ForEach<T>(IEnumerable<T> source, Func<T, Task> body)
        {
            return ForEach(source, null, body);
        }

        /// <summary>
        /// Executes a <c>foreach</c> loop in which async iterations may run in parallel,
        /// with set maximum degree of parallelism.
        /// </summary>
        /// <typeparam name="T">The type of data in the source.</typeparam>
        /// <param name="source">The enumerable data source.</param>
        /// <param name="maxDegreeOfParallelism">The maximum degree of parallelism.</param>
        /// <param name="body">The asynchronous delegate that is invoked for each item in the source collection.</param>
        /// <returns>A <see cref="Task"/> that represents the completion of the loop.</returns>
        public static Task ForEach<T>(IEnumerable<T> source, int maxDegreeOfParallelism, Func<T, Task> body)
        {
            return ForEach(
                Partitioner.Create(source), new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism },
                body);
        }

        /// <summary>
        /// Executes a <c>foreach</c> loop in which async iterations may run in parallel,
        /// with set maximum degree of parallelism and other options.
        /// </summary>
        /// <typeparam name="T">The type of data in the source.</typeparam>
        /// <param name="source">The enumerable data source.</param>
        /// <param name="options">Options that configure the loop.</param>
        /// <param name="body">The asynchronous delegate that is invoked for each item in the source collection.</param>
        /// <returns>A <see cref="Task"/> that represents the completion of the loop.</returns>
        public static Task ForEach<T>(IEnumerable<T> source, ParallelOptions options, Func<T, Task> body)
        {
            return ForEach(Partitioner.Create(source), options, body);
        }

        /// <summary>
        /// Executes a <c>foreach</c> loop on a partitioner in which async iterations may run in parallel,
        /// with set maximum degree of parallelism and other options.
        /// </summary>
        /// <typeparam name="T">The type of data in the source.</typeparam>
        /// <param name="source">The partitioner that contains the data.</param>
        /// <param name="options">Options that configure the loop.</param>
        /// <param name="body">The asynchronous delegate that is invoked for each item in the source collection.</param>
        /// <returns>A <see cref="Task"/> that represents the completion of the loop.</returns>
        /// <remarks>The <paramref name="source"/> partitioner has to support dynamic partitions.</remarks>
        public static Task ForEach<T>(Partitioner<T> source, ParallelOptions options, Func<T, Task> body)
        {
            if (!source.SupportsDynamicPartitions)
                throw new ArgumentException("The partitioner has to support dynamic partitions.", "source");

            if (options == null)
                options = DefaultOptions;

            var partitions = source.GetDynamicPartitions();
            var ev = new AsyncCountdownEvent(1);
            var cts = CancellationTokenSource.CreateLinkedTokenSource(options.CancellationToken);

            var data = new ForEachAsyncData<T>(partitions, options.TaskScheduler, ev, body, cts);

            StartWork(data, options.MaxDegreeOfParallelism);

            var tcs = new TaskCompletionSource();

            ev.WaitAsync().ContinueWith(
                _ =>
                {
                    if (data.Exceptions.Any())
                        tcs.SetException(data.Exceptions);
                    else if (options.CancellationToken.IsCancellationRequested)
                        tcs.SetCanceled();
                    else
                        tcs.SetResult();
                });

            return tcs.Task;
        }

        /// <summary>
        /// Holds all information required to execute the loop.
        /// </summary>
        private class ForEachAsyncData<T>
        {
            public IEnumerable<T> Partitions { get; private set; }
            public TaskScheduler Scheduler { get; private set; }
            public AsyncCountdownEvent Countdown { get; private set; }
            public Func<T, Task> Body { get; private set; }
            public CancellationTokenSource CancellationTokenSource { get; private set; }
            public ConcurrentQueue<Exception> Exceptions { get; private set; }
            public volatile bool EndPhase;

            public ForEachAsyncData(
                IEnumerable<T> partitions, TaskScheduler scheduler, AsyncCountdownEvent countdown, Func<T, Task> body,
                CancellationTokenSource cancellationTokenSource)
            {
                Partitions = partitions;
                Scheduler = scheduler;
                Countdown = countdown;
                Body = body;
                CancellationTokenSource = cancellationTokenSource;

                Exceptions = new ConcurrentQueue<Exception>();
            }
        }

        /// <summary>
        /// Starts a new <see cref="Task"/> to execute iterations of the loop.
        /// </summary>
        private static void StartWork<T>(ForEachAsyncData<T> data, int currentParallelism)
        {
            // the cancellation token cannot be used here, because it would mean
            // countdown would never reach zero
            Task.Factory.StartNew(
                () => DoWork(data, currentParallelism - 1), CancellationToken.None,
                TaskCreationOptions.None, data.Scheduler);
        }

        /// <summary>
        /// Executes iterations of the loop.
        /// </summary>
        private static async void DoWork<T>(ForEachAsyncData<T> data, int remainingParallelism)
        {
            try
            {
                if (data.CancellationTokenSource.Token.IsCancellationRequested)
                    return;

                if (remainingParallelism != 0 && !data.EndPhase)
                {
                    data.Countdown.AddCount();
                    StartWork(data, remainingParallelism);
                }

                foreach (var item in data.Partitions)
                {
                    if (data.CancellationTokenSource.IsCancellationRequested)
                        return;

                    await data.Body(item);
                }

                // marks the start of the end phase, so that no new Tasks are started from this point
                data.EndPhase = true;
            }
            catch (Exception ex)
            {
                // break other Tasks
                data.CancellationTokenSource.Cancel();
                data.Exceptions.Enqueue(ex);
            }
            finally
            {
                data.Countdown.Signal();
            }
        }
    }
}