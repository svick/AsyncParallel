using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nito.AsyncEx;

namespace AsyncEx.Tests
{
    [TestClass]
    public class AsyncParallelTests
    {
        [TestMethod]
        public async Task ProcessesEverythingTest()
        {
            var source = Enumerable.Range(1, 100);

            int sum = 0;

            await AsyncParallel.ForEach(
                source, async i =>
                {
                    await Task.Yield();
                    Interlocked.Add(ref sum, i);
                });

            Assert.AreEqual(5050, sum);
        }

        [TestMethod]
        public async Task SerialSyncExceptionTest()
        {
            var source = new[] { true, false };

            int ran = 0;

            var exception = new Exception();
            try
            {
                await AsyncParallel.ForEach(
                    source, new ParallelOptions { MaxDegreeOfParallelism = 1 },
                    async b =>
                    {
                        if (b)
                            throw exception;

                        Interlocked.Increment(ref ran);
                    });
                Assert.Fail();
            }
            catch (Exception e)
            {
                Assert.AreEqual(exception, e);
            }
            finally
            {
                Assert.AreEqual(0, ran);
            }
        }

        [TestMethod]
        public async Task SerialAsyncExceptionTest()
        {
            var source = new[] { true, false };

            int ran = 0;

            var exception = new Exception();
            try
            {
                await AsyncParallel.ForEach(
                    source, new ParallelOptions { MaxDegreeOfParallelism = 1 },
                    async b =>
                    {
                        await Task.Yield();
                        if (b)
                            throw exception;

                        Interlocked.Increment(ref ran);
                    });
                Assert.Fail();
            }
            catch (Exception e)
            {
                Assert.AreEqual(exception, e);
            }
            finally
            {
                Assert.AreEqual(0, ran);
            }
        }

        [TestMethod]
        public async Task ParallelExceptionsTest()
        {
            var source = Enumerable.Repeat(true, 20).ToArray();

            var barrier = new AsyncBarrier(source.Length);

            var task = AsyncParallel.ForEach(
                source, async b =>
                {
                    await barrier.SignalAndWaitAsync();
                    throw new Exception();
                });

            await task.ContinueWith(t => { Assert.AreEqual(source.Length, t.Exception.InnerExceptions.Count); });
        }

        [TestMethod]
        [ExpectedException(typeof(TaskCanceledException))]
        public async Task PrecancelledTest()
        {
            var source = new[] { true };

            var cts = new CancellationTokenSource();
            cts.Cancel();

            await AsyncParallel.ForEach(
                source, new ParallelOptions { CancellationToken = cts.Token }, async b => Assert.Fail());
        }

        [TestMethod]
        [ExpectedException(typeof(TaskCanceledException))]
        public async Task CancelledWhileProcessingTest()
        {
            var source = new[] { true, false };

            var cts = new CancellationTokenSource();

            await AsyncParallel.ForEach(
                source, new ParallelOptions { CancellationToken = cts.Token, MaxDegreeOfParallelism = 1 },
                async b =>
                {
                    if (b)
                        cts.Cancel();
                    else
                        Assert.Fail();
                });
        }

        [TestMethod]
        [ExpectedException(typeof(TaskCanceledException))]
        public async Task CancelledAfterProcessingTest()
        {
            var source = new[] { true };

            var cts = new CancellationTokenSource();

            await AsyncParallel.ForEach(
                source, new ParallelOptions { CancellationToken = cts.Token },
                async b => cts.Cancel());
        }
    }
}