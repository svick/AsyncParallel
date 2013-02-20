using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Nito.AsyncEx;

namespace AsyncEx
{
    public static class AsyncParallel
    {
        private static readonly ParallelOptions DefaultOptions = new ParallelOptions();

        public static Task ForEach<T>(IEnumerable<T> source, Func<T, Task> body)
        {
            return ForEach(source, DefaultOptions, body);
        }

        public static Task ForEach<T>(IEnumerable<T> source, ParallelOptions options, Func<T, Task> body)
        {
            return ForEach(Partitioner.Create(source), options, body);
        }

        public static Task ForEach<T>(Partitioner<T> source, ParallelOptions options, Func<T, Task> body)
        {
            if (options == null)
                options = DefaultOptions;

            var partitions = source.GetDynamicPartitions();
            var ev = new AsyncCountdownEvent(1);
            var cts = CancellationTokenSource.CreateLinkedTokenSource(options.CancellationToken);

            var data = new ForEachAsyncData<T>(
                partitions, options.MaxDegreeOfParallelism, options.TaskScheduler, ev, body, cts);

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

        private class ForEachAsyncData<T>
        {
            public IEnumerable<T> Partitions { get; private set; }
            public int MaxDegreeOfParallelism { get; private set; }
            public TaskScheduler Scheduler { get; private set; }
            public AsyncCountdownEvent Countdown { get; private set; }
            public Func<T, Task> Body { get; private set; }
            public CancellationTokenSource CancellationTokenSource { get; private set; }
            public ConcurrentQueue<Exception> Exceptions { get; private set; }
            public volatile bool EndPhase;

            public ForEachAsyncData(
                IEnumerable<T> partitions, int maxDegreeOfParallelism, TaskScheduler scheduler,
                AsyncCountdownEvent countdown, Func<T, Task> body, CancellationTokenSource cancellationTokenSource)
            {
                Partitions = partitions;
                MaxDegreeOfParallelism = maxDegreeOfParallelism;
                Scheduler = scheduler;
                Countdown = countdown;
                Body = body;
                CancellationTokenSource = cancellationTokenSource;

                Exceptions = new ConcurrentQueue<Exception>();
            }
        }

        private static void StartWork<T>(ForEachAsyncData<T> data, int currentParallelism)
        {
            // the cancellation token cannot be used here, because it would mean
            // countdown would never reach zero
            Task.Factory.StartNew(
                () => DoWork(data, currentParallelism - 1), CancellationToken.None,
                TaskCreationOptions.None, data.Scheduler);
        }

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