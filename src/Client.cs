using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using HdrHistogram;

namespace Haukcode.HighPerfComm
{
    public abstract class Client<TSendData, TPacketType> : IDisposable where TSendData : SendData
    {
        private readonly CancellationTokenSource senderCTS = new CancellationTokenSource();
        private CancellationTokenSource? receiverCTS;
        private readonly HdrHistogram.Recorder sendRecorder;
        private readonly HdrHistogram.Recorder ageRecorder;

        // The interval histograms handed out by GetSendStatistics, recycled into the recorders on
        // the next call. Without a recycle target every call allocated two fresh histograms, each
        // carrying a bucket array of hundreds of KB (values up to a minute in Stopwatch ticks at
        // three significant digits) -- large-object churn once a second per client, and gen2
        // pressure on a box that is already short of GC headroom.
        private HistogramBase? sendIntervalHistogram;
        private HistogramBase? ageIntervalHistogram;

        // One queue + one dedicated thread + (in the derived client) one socket per sender shard.
        // A single sender thread is CPU-bound at ~20 us per packet on an RPi4 -- the cost is the
        // kernel's per-packet UDP/IP/multicast work, not syscall entry, so batching (sendmmsg)
        // only buys ~35% while sharding across cores scales ~2.4x on a 4-core box. Callers pick a
        // shard key (the universe id for DMX protocols); every packet with the same key lands on
        // the same thread and socket, which is what keeps per-universe sequence numbers ordered.
        private readonly SendQueue[] sendQueues;
        private readonly Thread[] sendThreads;
        private readonly int senderCount;

        private readonly int receiveBufferSize;
        private readonly Func<TPacketType, Task>? channelWriter;
        private readonly Action? channelWriterComplete;
        private int queueItemCounter;
        private int droppedPackets;
        private int fullQueue;
        private long totalPackets;
        protected readonly ISubject<Exception> errorSubject;
        private readonly ISubject<KernelClockStep> kernelClockStepSubject = new Subject<KernelClockStep>();
        private Thread? receiveThread;
        private readonly Stopwatch receiveClock = new Stopwatch();

        // Maps kernel CLOCK_REALTIME stamps onto receiveClock, re-anchoring if NTP steps
        private readonly KernelTimestampMapper kernelTimestampMapper = new KernelTimestampMapper();

        // Fixed-size pool sized to the largest packet this client handles; see FixedSizeMemoryPool
        // for why MemoryPool<byte>.Shared misses under queue depth. Initialized in the constructor
        // once the buffer size is known.
        private readonly MemoryPool<byte> memoryPool;

        // Pool of spent TSendData objects so the hot send path doesn't allocate a new one per
        // packet (tens of thousands per second at the throughput ceiling). Multi-producer now that
        // several sender threads return objects after transmit; the (single) queue-writer thread
        // rents via RentSendData. ConcurrentQueue handles that. Bounded so a client whose factory
        // doesn't rent (leaves the pool filling) can't grow it without limit.
        private readonly ConcurrentQueue<TSendData> sendDataPool = new();
        private const int SendDataPoolCap = 2048;

        // Unimportant packets past this many queued per shard are discarded (FullQueue);
        // important ones always queue.
        private const int SendQueueBound = 10_000;

        private long lastSuccessfulSendTimestamp = Stopwatch.GetTimestamp();
        private long firstSendFailureTimestamp;
        private long lastErrorEmitTimestamp;
        private const double SendFaultThresholdMS = 3_000;
        private const double ErrorEmitThrottleMS = 5_000;

        /// <summary>
        /// One sender shard's queue. A lock-free queue with a "consumer is asleep" flag instead
        /// of a bounded Channel: the channel took a Monitor on every TryWrite and every TryRead,
        /// and with the scheduler writing ~36,000 packets/s while three sender threads read,
        /// that lock was contended on both sides (measured at 17 % of the scheduler thread's
        /// time and ~5 % of each sender's on a CM4 at 600 universes / 60 Hz). Here the steady
        /// state costs one interlocked enqueue/dequeue and no kernel wait; the event is only
        /// touched when the sender has actually run dry.
        /// </summary>
        private sealed class SendQueue
        {
            private readonly ConcurrentQueue<TSendData> items = new();
            private readonly ManualResetEventSlim signal = new(false);
            private int count;
            private int consumerWaiting;

            public int Count => Volatile.Read(ref this.count);

            public void Enqueue(TSendData item)
            {
                Interlocked.Increment(ref this.count);
                this.items.Enqueue(item);

                // Dekker-style handshake with the consumer: it publishes consumerWaiting and then
                // re-checks the queue; we publish the item and then check the flag. A full fence
                // on both sides keeps the two from missing each other on weakly ordered CPUs.
                Interlocked.MemoryBarrier();

                if (Volatile.Read(ref this.consumerWaiting) != 0)
                    this.signal.Set();
            }

            public bool TryDequeue(out TSendData item)
            {
                if (this.items.TryDequeue(out item!))
                {
                    Interlocked.Decrement(ref this.count);

                    return true;
                }

                return false;
            }

            /// <summary>
            /// Dequeue, sleeping until an item arrives or the token is cancelled. Returns false only
            /// on cancellation.
            /// </summary>
            public bool Take(out TSendData item, CancellationToken cancellationToken)
            {
                while (true)
                {
                    if (TryDequeue(out item))
                        return true;

                    Interlocked.Exchange(ref this.consumerWaiting, 1);

                    if (TryDequeue(out item))
                    {
                        Volatile.Write(ref this.consumerWaiting, 0);

                        return true;
                    }

                    try
                    {
                        this.signal.Wait(cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        Volatile.Write(ref this.consumerWaiting, 0);

                        return false;
                    }

                    this.signal.Reset();
                    Volatile.Write(ref this.consumerWaiting, 0);
                }
            }

            /// <summary>Wake a sleeping consumer without an item (shutdown).</summary>
            public void Wake()
            {
                this.signal.Set();
            }

            public void Dispose()
            {
                this.signal.Dispose();
            }
        }

        /// <param name="senderCount">
        /// Number of sender shards (thread + queue + socket each). Defaults to 1, which is the
        /// original single-threaded behavior. Raise it to spread the kernel's per-packet send cost
        /// across cores when one sender thread saturates.
        /// </param>
        /// <param name="channelWriter">
        /// Receives every parsed packet, on the receive thread, synchronously: the packet (and any
        /// buffer slice it references) is only valid until the call returns, because the next
        /// datagram is read into the same buffer. Copy what must be kept. A task that has not
        /// completed when the call returns is waited for on the receive thread, which is the
        /// backpressure: a stalled consumer leaves datagrams in the kernel socket buffer.
        /// </param>
        public Client(int packetSize, Func<TPacketType, Task>? channelWriter, Action? channelWriterComplete, int senderCount = 1)
        {
            if (senderCount < 1)
                throw new ArgumentOutOfRangeException(nameof(senderCount));

            this.receiveBufferSize = packetSize;
            this.senderCount = senderCount;
            this.channelWriter = channelWriter;
            this.channelWriterComplete = channelWriterComplete;
            this.sendQueues = new SendQueue[senderCount];
            this.sendThreads = new Thread[senderCount];

            // Sized so every send-queue slot can hold a pooled buffer at once (plus slack); memory
            // is only retained if that in-flight depth is actually reached.
            this.memoryPool = new FixedSizeMemoryPool(packetSize, maxPooled: senderCount * SendQueueBound + 4_096);

            for (int i = 0; i < senderCount; i++)
            {
                this.sendQueues[i] = new SendQueue();
            }

            // WithThreadSafeWrites is required: the sender shards record concurrently, and the
            // Recorder's phaser only guards the snapshot swap — writers enter the active
            // histogram in parallel, so it must be the concurrent variant. Without it the
            // counts silently corrupt and the interval snapshot's enumerator can throw
            // ArgumentOutOfRangeException (TotalCount no longer matches the bucket sums).
            this.sendRecorder = HistogramFactory
                .With64BitBucketSize()                  //LongConcurrentHistogram
                .WithValuesFrom(1)                      //Default value
                .WithValuesUpTo(TimeStamp.Minutes(1))   //Default value
                .WithPrecisionOf(3)                     //Default value
                .WithThreadSafeWrites()
                .WithThreadSafeReads()                  //returns a Recorder
                .Create();

            this.ageRecorder = HistogramFactory
                .With64BitBucketSize()                  //LongConcurrentHistogram
                .WithValuesFrom(1)                      //Default value
                .WithValuesUpTo(TimeStamp.Minutes(1))   //Default value
                .WithPrecisionOf(3)                     //Default value
                .WithThreadSafeWrites()
                .WithThreadSafeReads()                  //returns a Recorder
                .Create();

            this.errorSubject = new Subject<Exception>();

            // Run each send loop on its own dedicated thread with blocking sends, mirroring
            // the receive loop: an async send loop depends on the shared thread pool for
            // socket completions and queue wakeups, so pool starvation delayed queued
            // packets past the age cutoff and dropped them. A blocking send is serviced
            // directly by the kernel.
            for (int i = 0; i < senderCount; i++)
            {
                int senderIndex = i;

                this.sendThreads[i] = new Thread(() => Sender(senderIndex))
                {
                    Name = senderCount == 1 ? $"{GetType().Name} sender" : $"{GetType().Name} sender {senderIndex}",
                    IsBackground = true,
                    Priority = ThreadPriority.AboveNormal
                };

                this.sendThreads[i].Start();
            }
        }

        /// <summary>
        /// Blocking receive of a single packet into <paramref name="memory"/>. Called on the
        /// dedicated receive thread; must block until a packet arrives or the receive socket
        /// is closed (throw on close/shutdown). A synchronous read is woken directly by the
        /// kernel, so packet-arrival timestamping never depends on the shared thread pool —
        /// async socket completions are dispatched via the thread pool and get delayed when
        /// the pool is saturated, which corrupted recorded timestamps (gap-then-burst).
        /// </summary>
        protected abstract int ReceiveData(Memory<byte> memory, out IPEndPoint? remoteEndPoint, out IPAddress? destinationAddress);

        /// <summary>
        /// Kernel arrival timestamp in nanoseconds (on a platform-specific clock; only deltas
        /// are used) of the packet just returned by ReceiveData, or 0 when unavailable. Set by
        /// implementations that use kernel receive timestamping (see
        /// ReceiveTimestamping.TryCreate); cleared by the receive loop
        /// before every ReceiveData call. When present it replaces the user-space timestamp,
        /// so packets that waited in the socket buffer keep their true arrival times.
        /// </summary>
        protected long KernelReceiveTimestampNS { get; set; }

        /// <summary>
        /// Blocking send of a single packet. Called on the dedicated send thread (and from
        /// SendImmediateAsync on caller threads); must block until the packet is handed to
        /// the kernel. A synchronous send keeps outgoing packet pacing independent of the
        /// shared thread pool, which async socket completions are dispatched through.
        /// </summary>
        /// <param name="senderIndex">
        /// Which sender shard is calling, 0..SenderCount-1. Implementations with more than one
        /// shard must send on that shard's own socket: one socket driven by several threads
        /// serializes on the kernel's socket lock and throws the scaling away.
        /// </param>
        protected abstract int SendPacket(TSendData sendData, ReadOnlyMemory<byte> payload, int senderIndex);

        /// <summary>
        /// Number of sender shards. Derived clients need one socket per shard.
        /// </summary>
        protected int SenderCount => this.senderCount;

        protected abstract void InitializeReceiveSocket();

        protected abstract void DisposeReceiveSocket();

        /// <summary>
        /// Parse one received datagram. Called on the receive thread; <paramref name="buffer"/> is
        /// the receive buffer itself and is overwritten by the next datagram, so the returned
        /// object may reference it only for as long as the channel writer holds it (see the
        /// constructor). Return null to drop the packet.
        /// </summary>
#if NETSTANDARD2_1
        protected abstract TPacketType TryParseObject(ReadOnlyMemory<byte> buffer, double timestampMS, IPEndPoint sourceIP, IPAddress destinationIP);
#else
        protected abstract TPacketType? TryParseObject(ReadOnlyMemory<byte> buffer, double timestampMS, IPEndPoint sourceIP, IPAddress destinationIP);
#endif

        public bool IsOperational => !this.senderCTS.IsCancellationRequested && !HasSustainedSendFailure;

        // True once sends have been failing continuously for longer than the
        // threshold (e.g. the NIC we bound to went away after a network change).
        // Resets as soon as a single send succeeds.
        private bool HasSustainedSendFailure
            => this.firstSendFailureTimestamp != 0
                && ElapsedMs(this.firstSendFailureTimestamp) >= SendFaultThresholdMS;

        private static double ElapsedMs(long startTimestamp)
            => (Stopwatch.GetTimestamp() - startTimestamp) * 1000.0 / Stopwatch.Frequency;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.senderCTS.Cancel();

                foreach (var queue in this.sendQueues)
                    queue.Wake();

                StopReceive();

                foreach (var thread in this.sendThreads)
                    thread.Join(5_000);

                foreach (var queue in this.sendQueues)
                {
                    while (queue.TryDequeue(out var sendData))
                        sendData.Data?.Dispose();

                    queue.Dispose();
                }
            }
        }

        public IObservable<Exception> OnError => this.errorSubject.AsObservable();

        /// <summary>
        /// Fired when a kernel CLOCK_REALTIME step is absorbed so receive timestamps stay
        /// monotonic. Silent to callers of the packet pipeline; subscribe to log it.
        /// </summary>
        public IObservable<KernelClockStep> OnKernelClockStep => this.kernelClockStepSubject.AsObservable();

        /// <summary>
        /// Send statistics since the previous call. The histograms in the result are only valid
        /// until the next call, which recycles them; copy (or <c>Add</c> into a cumulative
        /// histogram) anything that must be kept.
        /// </summary>
        public SendStatistics GetSendStatistics(bool reset)
        {
            var sendStatsCopy = this.sendIntervalHistogram == null ? this.sendRecorder.GetIntervalHistogram() : this.sendRecorder.GetIntervalHistogram(this.sendIntervalHistogram);
            var ageStatsCopy = this.ageIntervalHistogram == null ? this.ageRecorder.GetIntervalHistogram() : this.ageRecorder.GetIntervalHistogram(this.ageIntervalHistogram);
            this.sendIntervalHistogram = sendStatsCopy;
            this.ageIntervalHistogram = ageStatsCopy;

            var sendStatistics = new SendStatistics
            {
                DroppedPackets = this.droppedPackets,
                QueueLength = this.queueItemCounter,
                FullQueue = this.fullQueue,
                TotalPackets = this.totalPackets,
                SendStats = sendStatsCopy,
                AgeStats = ageStatsCopy
            };

            if (reset)
            {
                // Reset. Interlocked because several sender threads increment these.
                Interlocked.Exchange(ref this.droppedPackets, 0);
                Interlocked.Exchange(ref this.fullQueue, 0);
                Interlocked.Exchange(ref this.totalPackets, 0);
            }

            return sendStatistics;
        }

        public ReceiveStatistics GetReceiveStatistics()
        {
            // Packets are parsed and handed on by the receive thread itself, so nothing is ever
            // queued between the socket and the channel writer.
            return new ReceiveStatistics
            {
                ObjectsInQueue1 = 0
            };
        }

        private void Sender(int senderIndex)
        {
            var queue = this.sendQueues[senderIndex];
            var token = this.senderCTS.Token;

            while (!token.IsCancellationRequested)
            {
                if (!queue.Take(out var sendData, token))
                    break;

                {
                    Interlocked.Decrement(ref this.queueItemCounter);

                    try
                    {
                        if (sendData.BarrierSignal != null)
                        {
                            // Barrier marker: this shard has now drained everything queued ahead of
                            // the barrier. Signal and send nothing (there is no payload).
                            var signal = sendData.BarrierSignal;
                            sendData.BarrierSignal = null;
                            signal.Signal();

                            continue;
                        }

                        if (!sendData.Important)
                        {
                            // Ignore the important packets when recording age, not relevant
                            this.ageRecorder.RecordValue(sendData.AgeTicks);

                            if (sendData.AgeMS > 200)
                            {
                                // Old, discard
                                Interlocked.Increment(ref this.droppedPackets);
                                continue;
                            }
                        }

                        if (sendData.BarrierWait != null)
                        {
                            // Ordering barrier: hold this packet until every other shard has
                            // reached its marker, so it cannot overtake DMX still queued
                            // elsewhere. Bounded, so a wedged shard can't stall output forever.
                            sendData.BarrierWait.Wait(BarrierTimeoutMS);
                            sendData.BarrierWait = null;
                        }

                        long startTimestamp = Stopwatch.GetTimestamp();

                        // Send packet on this shard's socket
                        SendPacket(sendData, sendData.Data.Memory[..sendData.DataLength], senderIndex);

                        if (!sendData.Important)
                        {
                            // Ignore recording important packets since we may have a burst of a lot of them (blackouts for example)
                            long elapsedTicks = Stopwatch.GetTimestamp() - startTimestamp;
                            this.sendRecorder.RecordValue(elapsedTicks);
                        }

                        Interlocked.Increment(ref this.totalPackets);

                        // Successful send clears any pending fault state.
                        Interlocked.Exchange(ref this.lastSuccessfulSendTimestamp, Stopwatch.GetTimestamp());
                        Interlocked.Exchange(ref this.firstSendFailureTimestamp, 0);
                    }
                    catch (Exception ex)
                    {
                        if (ex is OperationCanceledException)
                            continue;

                        if (ex is System.Net.Sockets.SocketException)
                        {
                            // CompareExchange, not a read-then-write: with several sender threads
                            // failing at once (a NIC flap takes them all down together) only the
                            // first should stamp the fault, or the fault window keeps restarting
                            // and HasSustainedSendFailure never trips.
                            Interlocked.CompareExchange(ref this.firstSendFailureTimestamp, Stopwatch.GetTimestamp(), 0);

                            // Throttle notifications so a persistent failure (e.g. the bound
                            // NIC went away after a network change) doesn't spam the log once
                            // per packet. The first failure is reported immediately.
                            if (ElapsedMs(Interlocked.Read(ref this.lastErrorEmitTimestamp)) >= ErrorEmitThrottleMS)
                            {
                                Interlocked.Exchange(ref this.lastErrorEmitTimestamp, Stopwatch.GetTimestamp());

                                this.errorSubject.OnNext(ex);
                            }

                            // Transient send failure (e.g. errno 101 Network unreachable during a NIC flap).
                            // Don't kill the sender — back off briefly and keep draining the queue so we recover when routing returns.
                            this.senderCTS.Token.WaitHandle.WaitOne(100);
                        }
                        else
                        {
                            this.errorSubject.OnNext(ex);
                        }
                    }
                    finally
                    {
                        // Dispose the buffer and return the send-data object to the pool for reuse.
                        ReturnSendData(sendData);
                    }
                }
            }
        }

        public double ReceiveClock => this.receiveClock.Elapsed.TotalMilliseconds;

        /// <summary>
        /// Number of kernel-clock steps (NTP) absorbed since the last <see cref="StartReceive"/>.
        /// Linux/macOS software timestamps are CLOCK_REALTIME and can jump; each jump is
        /// dropped so the receive timeline stays continuous on the monotonic side.
        /// </summary>
        public int KernelClockSteps => this.kernelTimestampMapper.Steps;

        /// <summary>
        /// Number of out-of-order kernel receive timestamps clamped since the last
        /// <see cref="StartReceive"/>. Multi-queue NICs stamp packets that then get dequeued
        /// in the other order; the sub-millisecond reversal is held flat rather than treated
        /// as a clock step. A nonzero value is normal — watch the rate, not the count.
        /// </summary>
        public int KernelTimestampReorders => this.kernelTimestampMapper.Reorders;

        protected void StartReceive()
        {
            if (this.receiverCTS != null)
                throw new Exception("Already running");

            InitializeReceiveSocket();

            this.receiverCTS = new CancellationTokenSource();

            // Restart the clocks before the receive thread can dequeue a packet, otherwise
            // the first stamp can land against a stale mapper state from a previous session.
            this.receiveClock.Restart();
            this.kernelTimestampMapper.Reset();

            // Run the receive loop on its own dedicated thread with blocking socket reads.
            // The kernel wakes the thread directly on packet arrival, so the packet-arrival
            // timestamp capture never depends on the shared thread pool. With the previous
            // pool-scheduled async loop, a saturated pool (even from unrelated code in the
            // process) left received packets in the kernel buffer for a second or more and
            // they were then drained in a burst with near-identical timestamps.
            this.receiveThread = new Thread(Receiver)
            {
                Name = $"{GetType().Name} receiver",
                IsBackground = true,
                Priority = ThreadPriority.AboveNormal
            };
            this.receiveThread.Start();
        }

        private void StopReceive()
        {
            this.receiverCTS?.Cancel();

            // Close the socket first — that unblocks the receive thread's blocking read
            // so it can observe the cancellation and exit.
            DisposeReceiveSocket();

            this.receiveThread?.Join(5_000);

            this.receiverCTS?.Dispose();

            this.receiveThread = null;
            this.receiverCTS = null;
        }

        /// <summary>
        /// Rent a spent send-data object from the pool, or null when the pool is empty (the
        /// caller's factory then allocates a fresh one). Every field must be reconfigured before
        /// use — a returned object is cleared of its buffer only. Called from the single
        /// queue-writer thread.
        /// </summary>
        protected TSendData? RentSendData()
        {
            return this.sendDataPool.TryDequeue(out var sendData) ? sendData : null;
        }

        private void ReturnSendData(TSendData sendData)
        {
            sendData.Data?.Dispose();
            sendData.Data = null!;

            // Never let barrier state leak into the next packet that rents this object.
            sendData.BarrierSignal = null;
            sendData.BarrierWait = null;

            // Bounded so a client whose factory never rents (its pool only ever fills) can't grow
            // the pool without limit.
            if (this.sendDataPool.Count < SendDataPoolCap)
                this.sendDataPool.Enqueue(sendData);
        }

        private void DiscardQueuedPackets()
        {
            // Clear every queue, not just this shard's — the client as a whole is down.
            foreach (var q in this.sendQueues)
            {
                while (q.TryDequeue(out var sendData))
                {
                    Interlocked.Decrement(ref this.queueItemCounter);
                    sendData.Data?.Dispose();
                }
            }
        }

        /// <param name="shardKey">
        /// Selects the sender shard. Packets sharing a key are guaranteed to go out on the same
        /// thread and socket, in order — pass the universe id so a universe's sequence numbers
        /// stay monotonic. Any stable key works; it is reduced modulo the shard count.
        /// </param>
        protected ValueTask QueuePacket(int allocatePacketLength, bool important, Func<TSendData> sendDataFactory, Func<Memory<byte>, int> packetWriter, int shardKey = 0)
        {
            var queue = this.sendQueues[ShardFor(shardKey)];

            if (!IsOperational)
            {
                DiscardQueuedPackets();

                return default;
            }

            var memory = this.memoryPool.Rent(allocatePacketLength);

            var newSendData = sendDataFactory();

            newSendData.Data = memory;
            newSendData.Important = important;

            int packetLength = packetWriter(memory.Memory);

            newSendData.DataLength = packetLength;

            newSendData.StartAgeStopwatch();

            if (important || queue.Count < SendQueueBound)
            {
                Interlocked.Increment(ref this.queueItemCounter);
                queue.Enqueue(newSendData);
            }
            else
            {
                // Discard, indicate queue full. Dispose the rented buffer and return the
                // send-data object to the pool (it never reached the sender's finally).
                Interlocked.Increment(ref this.fullQueue);
                ReturnSendData(newSendData);
            }

            return default;
        }

        /// <summary>
        /// Queue a packet that must not overtake anything already queued on any shard — E1.31 sync
        /// and ArtSync, which have to follow the DMX frames they synchronize.
        ///
        /// With one shard the queue gives that ordering for free. With several, the packet would
        /// otherwise be transmitted as soon as its own shard reached it, while a slower shard still
        /// had DMX for the same frame pending — silently breaking synchronization. So push a marker
        /// onto every other shard and have the packet's own sender wait until all of them are
        /// reached. Only sender threads block; the caller queues and moves on, exactly as before.
        /// </summary>
        protected ValueTask QueueBarrierPacket(int allocatePacketLength, Func<TSendData> sendDataFactory, Func<Memory<byte>, int> packetWriter, int shardKey = 0)
        {
            if (this.senderCount == 1)
            {
                // Single shard: FIFO already guarantees it follows everything queued before it.
                return QueuePacket(allocatePacketLength, important: true, sendDataFactory, packetWriter, shardKey);
            }

            if (!IsOperational)
                return default;

            int targetShard = ShardFor(shardKey);
            var countdown = new CountdownEvent(this.senderCount - 1);

            for (int i = 0; i < this.senderCount; i++)
            {
                if (i == targetShard)
                    continue;

                var marker = sendDataFactory();
                marker.Data = null!;
                marker.DataLength = 0;
                marker.Important = true;
                marker.BarrierSignal = countdown;
                marker.StartAgeStopwatch();

                Interlocked.Increment(ref this.queueItemCounter);
                this.sendQueues[i].Enqueue(marker);
            }

            var memory = this.memoryPool.Rent(allocatePacketLength);

            var newSendData = sendDataFactory();

            newSendData.Data = memory;
            newSendData.Important = true;
            newSendData.DataLength = packetWriter(memory.Memory);
            newSendData.BarrierWait = countdown;
            newSendData.StartAgeStopwatch();

            Interlocked.Increment(ref this.queueItemCounter);
            this.sendQueues[targetShard].Enqueue(newSendData);

            return default;
        }

        // A wedged shard must not be able to stall output indefinitely; the sync goes out late
        // rather than never.
        private const int BarrierTimeoutMS = 200;

        /// <summary>
        /// Map a shard key onto a sender index. Non-negative and stable, so a given universe
        /// always lands on the same thread/socket.
        /// </summary>
        private int ShardFor(int shardKey)
        {
            if (this.senderCount == 1)
                return 0;

            return (int)((uint)shardKey % (uint)this.senderCount);
        }

        protected ValueTask SendImmediateAsync(int allocatePacketLength, bool important, Func<TSendData> sendDataFactory, Func<Memory<byte>, int> packetWriter)
        {
            if (!IsOperational)
                return default;

            var memory = this.memoryPool.Rent(allocatePacketLength);

            try
            {
                var sendData = sendDataFactory();

                sendData.Data = memory;
                sendData.Important = important;

                int packetLength = packetWriter(memory.Memory);

                sendData.DataLength = packetLength;

                // Sent inline on the caller's thread, bypassing the shard queues entirely, so it
                // uses shard 0's socket. UDP sockets are safe to send from any thread; this path
                // is for one-off/immediate packets, not the sustained stream, so it does not
                // contend meaningfully with the sender threads.
                SendPacket(sendData, memory.Memory[..packetLength], 0);

                Interlocked.Increment(ref this.totalPackets);
            }
            finally
            {
                memory.Dispose();
            }

            return default;
        }

        /// <summary>
        /// The receive loop: one blocking read, stamp, parse and hand-off per datagram, all on
        /// this thread. Earlier versions copied each datagram into a System.IO.Pipelines pipe for
        /// a separate parser task; at 36,000 packets/s that cost a Monitor acquisition per packet
        /// on each side of the pipe (22 % of this thread's time went to lock contention on a CM4),
        /// the parser hopped between thread-pool workers after every await, and every packet paid
        /// a Task allocation. Parsing here needs none of that, and the kernel receive timestamp
        /// keeps arrival times exact even when the consumer holds this thread up.
        /// </summary>
        private void Receiver()
        {
            var buffer = new byte[this.receiveBufferSize];
            var memory = new Memory<byte>(buffer);

            while (!this.receiverCTS!.IsCancellationRequested)
            {
                try
                {
                    KernelReceiveTimestampNS = 0;

                    int receivedBytes = ReceiveData(memory, out IPEndPoint? remoteEndPoint, out IPAddress? destinationAddress);

                    // Capture the timestamp first so it's as accurate as possible. Kernel
                    // stamps (CLOCK_REALTIME on Linux/macOS) are mapped onto receiveClock;
                    // NTP steps are absorbed so the output timeline stays monotonic.
                    long timestampTicks;
                    long kernelNS = KernelReceiveTimestampNS;
                    if (kernelNS != 0)
                    {
                        var mapped = this.kernelTimestampMapper.Map(kernelNS, this.receiveClock.ElapsedTicks);
                        timestampTicks = mapped.TimestampTicks;

                        if (mapped.Stepped)
                        {
                            try
                            {
                                this.kernelClockStepSubject.OnNext(new KernelClockStep(
                                    mapped.Forward, mapped.KernelDeltaNS, mapped.MonotonicDeltaNS,
                                    this.kernelTimestampMapper.Steps));
                            }
                            catch
                            {
                            }
                        }
                    }
                    else
                    {
                        timestampTicks = this.receiveClock.ElapsedTicks;
                    }

                    if (remoteEndPoint == null || destinationAddress == null ||
                        remoteEndPoint.AddressFamily != AddressFamily.InterNetwork ||
                        destinationAddress.AddressFamily != AddressFamily.InterNetwork)
                    {
                        // Missing or not IPv4
                        continue;
                    }

                    if (receivedBytes > 0 && this.channelWriter != null)
                    {
                        double timestampMS = (double)timestampTicks / Stopwatch.Frequency * 1000;

                        DispatchPacket(memory[..receivedBytes], timestampMS, remoteEndPoint, destinationAddress);
                    }
                }
                catch (Exception ex)
                {
                    if (this.receiverCTS.IsCancellationRequested)
                        break;

                    if (!(ex is OperationCanceledException))
                    {
                        this.errorSubject.OnNext(ex);
                    }

                    if (ex is System.Net.Sockets.SocketException)
                    {
                        // Transient receive failure during a NIC flap — back off briefly and keep listening.
                        Thread.Sleep(100);
                    }
                }
            }

            this.channelWriterComplete?.Invoke();
        }

        private void DispatchPacket(ReadOnlyMemory<byte> data, double timestampMS, IPEndPoint sourceIP, IPAddress destinationIP)
        {
            try
            {
                var parsedObject = TryParseObject(data, timestampMS, sourceIP, destinationIP);
                if (parsedObject != null)
                {
                    var task = this.channelWriter!(parsedObject);

                    // Normally already completed (the consumer copies and returns). If it is not,
                    // wait here: the buffer is about to be reused, and holding the receive thread
                    // is the backpressure that keeps a stalled consumer from growing memory.
                    if (!task.IsCompleted)
                        task.GetAwaiter().GetResult();
                }
            }
            catch (Exception ex)
            {
                this.errorSubject.OnNext(ex);
            }
        }
    }
}
