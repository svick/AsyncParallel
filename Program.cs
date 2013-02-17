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
            IEnumerable<T> source,
            ParallelOptions options, Func<T, Task> body)
        {
            if (options == null)
                options = DefaultOptions;

            var partitioner = Partitioner.Create(source);

            var ev = new AsyncCountdownEvent(1);

            StartWork(partitioner, options, options.MaxDegreeOfParallelism, ev, body);

            return ev.WaitAsync();
        }

        private static void StartWork<T>(OrderablePartitioner<T> partitioner, ParallelOptions options, int currentParallelism, AsyncCountdownEvent countdown, Func<T, Task> body)
        {
            Task.Factory.StartNew(
                () => DoWork(partitioner, options, currentParallelism - 1, countdown, body), options.CancellationToken,
                TaskCreationOptions.None, options.TaskScheduler);
        }

        private static async void DoWork<T>(
            OrderablePartitioner<T> partitioner, ParallelOptions options, int remainingParallelism, AsyncCountdownEvent countdown, Func<T, Task> body)
        {
            if (remainingParallelism != 0)
            {
                countdown.AddCount();
                StartWork(partitioner, options, remainingParallelism, countdown, body);
            }

            var partition = partitioner.GetDynamicPartitions();
            foreach (var item in partition)
            {
                await body(item);
                // TODO: cancellation, exceptions
            }
        }
    }
}
