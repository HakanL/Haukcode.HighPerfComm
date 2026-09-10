using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Haukcode.HighPerfComm.Tests
{
    /// <summary>
    /// The send and receive pipelines against an in-memory client: no sockets, packets are
    /// bytes handed to <see cref="FakeClient"/>.
    /// </summary>
    public class ClientPipelineTests
    {
        private sealed class TestSendData : SendData
        {
        }

        private sealed class Parsed
        {
            public byte[] Bytes = Array.Empty<byte>();
            public double TimestampMS;
            public IPEndPoint Source = null!;
        }

        private sealed class FakeClient : Client<TestSendData, Parsed>
        {
            private static readonly IPEndPoint source = new IPEndPoint(IPAddress.Parse("10.0.0.2"), 5000);
            private static readonly IPAddress destination = IPAddress.Parse("10.0.0.1");

            private readonly BlockingCollection<byte[]> inbound = new();
            private readonly CancellationTokenSource receiveCts = new();

            public FakeClient(Func<Parsed, Task>? channelWriter, int senderCount = 1, Action? complete = null)
                : base(packetSize: 64, channelWriter, complete, senderCount)
            {
            }

            public ConcurrentQueue<(int Shard, byte[] Payload)> Sent { get; } = new();

            public ManualResetEventSlim SendGate { get; } = new(true);

            public void Listen() => StartReceive();

            public void Inject(byte[] packet) => this.inbound.Add(packet);

            public ValueTask Queue(byte[] payload, bool important, int shardKey) =>
                QueuePacket(payload.Length, important, () => RentSendData() ?? new TestSendData(), m => { payload.CopyTo(m); return payload.Length; }, shardKey);

            public ValueTask QueueBarrier(byte[] payload, int shardKey) =>
                QueueBarrierPacket(payload.Length, () => RentSendData() ?? new TestSendData(), m => { payload.CopyTo(m); return payload.Length; }, shardKey);

            protected override int ReceiveData(Memory<byte> memory, out IPEndPoint? remoteEndPoint, out IPAddress? destinationAddress)
            {
                var packet = this.inbound.Take(this.receiveCts.Token);
                packet.CopyTo(memory);
                remoteEndPoint = source;
                destinationAddress = destination;

                return packet.Length;
            }

            protected override int SendPacket(TestSendData sendData, ReadOnlyMemory<byte> payload, int senderIndex)
            {
                SendGate.Wait();
                Sent.Enqueue((senderIndex, payload.ToArray()));

                return payload.Length;
            }

            protected override void InitializeReceiveSocket()
            {
            }

            protected override void DisposeReceiveSocket()
            {
                this.receiveCts.Cancel();
            }

            protected override Parsed? TryParseObject(ReadOnlyMemory<byte> buffer, double timestampMS, IPEndPoint sourceIP, IPAddress destinationIP)
            {
                if (buffer.Length == 0 || buffer.Span[0] == 0xFF)
                    // "Unparseable"
                    return null;

                return new Parsed { Bytes = buffer.ToArray(), TimestampMS = timestampMS, Source = sourceIP };
            }
        }

        private static bool WaitFor(Func<bool> condition, int timeoutMS = 5_000)
        {
            var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMS);
            while (DateTime.UtcNow < deadline)
            {
                if (condition())
                    return true;
                Thread.Sleep(5);
            }

            return condition();
        }

        [Fact]
        public async Task QueuedPackets_GoOutInOrder_PerShard()
        {
            using var client = new FakeClient(channelWriter: null, senderCount: 2);

            for (int i = 0; i < 200; i++)
                await client.Queue(new[] { (byte)(i & 1), (byte)i }, important: false, shardKey: i & 1);

            Assert.True(WaitFor(() => client.Sent.Count == 200), $"Only {client.Sent.Count} sent");

            foreach (var shard in new[] { 0, 1 })
            {
                var order = client.Sent.Where(x => x.Shard == shard).Select(x => x.Payload[1]).ToList();
                Assert.Equal(100, order.Count);
                Assert.Equal(order.OrderBy(x => x).ToList(), order);
                Assert.All(client.Sent.Where(x => x.Shard == shard), x => Assert.Equal(shard, x.Payload[0]));
            }

            var stats = client.GetSendStatistics(reset: false);
            Assert.Equal(200, stats.TotalPackets);
            Assert.Equal(0, stats.DroppedPackets);
            Assert.Equal(0, stats.FullQueue);
            Assert.Equal(0, stats.QueueLength);
        }

        [Fact]
        public async Task StalledSender_DropsUnimportantPastTheBound_KeepsImportant()
        {
            using var client = new FakeClient(channelWriter: null);
            client.SendGate.Reset();

            // The sender is blocked in SendPacket holding one item; the queue fills behind it.
            for (int i = 0; i < 10_500; i++)
                await client.Queue(new byte[] { 1 }, important: false, shardKey: 0);

            await client.Queue(new byte[] { 2 }, important: true, shardKey: 0);

            var stats = client.GetSendStatistics(reset: false);
            Assert.True(stats.FullQueue > 0, "unimportant packets past the bound must be counted as FullQueue");
            Assert.True(stats.QueueLength <= 10_002, $"queue length {stats.QueueLength}");

            client.SendGate.Set();

            Assert.True(WaitFor(() => client.Sent.Any(x => x.Payload[0] == 2), 10_000), "the important packet must still go out");
        }

        [Fact]
        public async Task ReceivedPackets_AreParsedAndHandedOnInOrder_OnTheReceiveThread()
        {
            var received = new ConcurrentQueue<(Parsed Packet, int ThreadId)>();
            using var client = new FakeClient(p =>
            {
                received.Enqueue((p, Environment.CurrentManagedThreadId));

                return Task.CompletedTask;
            });
            client.Listen();

            for (int i = 0; i < 100; i++)
                client.Inject(new[] { (byte)1, (byte)i });

            client.Inject(new byte[] { 0xFF, 7 });

            Assert.True(WaitFor(() => received.Count == 100), $"Only {received.Count} handed on");

            var list = received.ToList();
            Assert.Equal(Enumerable.Range(0, 100).Select(x => (byte)x), list.Select(x => x.Packet.Bytes[1]));
            Assert.Single(list.Select(x => x.ThreadId).Distinct());
            Assert.All(list, x => Assert.Equal(2, x.Packet.Bytes.Length));
            Assert.True(list.Zip(list.Skip(1), (a, b) => b.Packet.TimestampMS >= a.Packet.TimestampMS).All(x => x));

            // The receive thread itself, not the pool
            Assert.DoesNotContain(list, x => Thread.CurrentThread.ManagedThreadId == x.ThreadId);
        }

        [Fact]
        public async Task SlowConsumer_HoldsTheReceiveThread_InsteadOfQueueingInMemory()
        {
            var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            int handed = 0;
            using var client = new FakeClient(p =>
            {
                if (Interlocked.Increment(ref handed) == 1)
                    return release.Task;

                return Task.CompletedTask;
            });
            client.Listen();

            for (int i = 0; i < 10; i++)
                client.Inject(new byte[] { 1, (byte)i });

            Assert.True(WaitFor(() => Volatile.Read(ref handed) == 1));
            await Task.Delay(200);
            Assert.Equal(1, Volatile.Read(ref handed));

            release.SetResult(true);
            Assert.True(WaitFor(() => Volatile.Read(ref handed) == 10), $"Only {handed} handed on after release");
        }

        [Fact]
        public void Dispose_CompletesTheChannelWriter_AndStopsTheSenders()
        {
            bool completed = false;
            var client = new FakeClient(_ => Task.CompletedTask, senderCount: 3, complete: () => completed = true);
            client.Listen();

            client.Dispose();

            Assert.True(WaitFor(() => completed), "channelWriterComplete must run when the receive loop ends");
        }
    }
}
