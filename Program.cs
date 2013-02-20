using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Nito.AsyncEx;

// http://blogs.msdn.com/b/pfxteam/archive/2012/03/05/10278165.aspx
// http://stackoverflow.com/questions/14673728/run-async-method-8-times-in-parallel
// http://stackoverflow.com/questions/14889988/how-can-i-use-where-with-an-async-predicate/14895226#14895226


namespace ConsoleApplication1
{
    static class Program
    {
        static void Main(string[] args)
        {
            var items = Enumerable.Range(0, 100).ToArray();

            var scheduler = TaskScheduler.Default;

            /*ForEachAsync(
                items, 8, async i =>
                {
                    for (int j = 0; j < 20000000; j++)
                    {}
                }).Wait();*/
        }

        private static readonly ParallelOptions DefaultOptions = new ParallelOptions();

        public static Task ForEachAsync<T>(
            IEnumerable<T> source, Func<T, Task> body)
        {
            return ForEachAsync(source, DefaultOptions, body);
        }

        public static Task ForEachAsync<T>(
            IEnumerable<T> source, ParallelOptions options, Func<T, Task> body)
        {
            if (options == null)
                options = DefaultOptions;

            var partitioner = Partitioner.Create(source);
            var ev = new AsyncCountdownEvent(1);
            var cts = CancellationTokenSource.CreateLinkedTokenSource(options.CancellationToken);

            var data = new ForEachAsyncData<T>(
                partitioner, options.MaxDegreeOfParallelism, options.TaskScheduler, ev, body, cts);

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
            public OrderablePartitioner<T> Partitioner { get; private set; }
            public int MaxDegreeOfParallelism { get; private set; }
            public TaskScheduler Scheduler { get; private set; }
            public AsyncCountdownEvent Countdown { get; private set; }
            public Func<T, Task> Body { get; private set; }
            public CancellationTokenSource CancellationTokenSource { get; private set; }
            public ConcurrentQueue<Exception> Exceptions { get; private set; }

            public ForEachAsyncData(
                OrderablePartitioner<T> partitioner, int maxDegreeOfParallelism, TaskScheduler scheduler,
                AsyncCountdownEvent countdown, Func<T, Task> body, CancellationTokenSource cancellationTokenSource)
            {
                Partitioner = partitioner;
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
            Task.Factory.StartNew(
                () => DoWork(data, currentParallelism - 1), data.CancellationTokenSource.Token,
                TaskCreationOptions.None, data.Scheduler);
        }

        private static async void DoWork<T>(ForEachAsyncData<T> data, int remainingParallelism)
        {
            // this is not the first Task
            if (remainingParallelism != data.MaxDegreeOfParallelism - 1)
            {
                if (!data.Countdown.TryAddCount())
                {
                    // all other Tasks have finished, which means the work is done
                    return;
                }
            }

            try
            {
                if (data.CancellationTokenSource.Token.IsCancellationRequested)
                    return;

                if (remainingParallelism != 0)
                    StartWork(data, remainingParallelism);

                var partition = data.Partitioner.GetDynamicPartitions();
                foreach (var item in partition)
                {
                    if (data.CancellationTokenSource.IsCancellationRequested)
                        return;

                    await data.Body(item);
                }

                // all work is done cancel any Task waiting to start
                data.CancellationTokenSource.Cancel();
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