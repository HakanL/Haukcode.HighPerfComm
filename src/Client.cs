using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Channels;
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

        // One queue + one dedicated thread + (in the derived client) one socket per sender shard.
        // A single sender thread is CPU-bound at ~20 us per packet on an RPi4 -- the cost is the
        // kernel's per-packet UDP/IP/multicast work, not syscall entry, so batching (sendmmsg)
        // only buys ~35% while sharding across cores scales ~2.4x on a 4-core box. Callers pick a
        // shard key (the universe id for DMX protocols); every packet with the same key lands on
        // the same thread and socket, which is what keeps per-universe sequence numbers ordered.
        private readonly Channel<TSendData>[] sendQueues;
        private readonly Thread[] sendThreads;
        private readonly int senderCount;

        private readonly int receiveBufferSize;
        private int queueItemCounter;
        private int droppedPackets;
        private int fullQueue;
        private long totalPackets;
        protected readonly ISubject<Exception> errorSubject;
        private Thread? receiveThread;
        private Task? parserTask;
        private readonly Stopwatch receiveClock = new Stopwatch();
        private readonly MemoryPool<byte> memoryPool = MemoryPool<byte>.Shared;

        // Pool of spent TSendData objects so the hot send path doesn't allocate a new one per
        // packet (tens of thousands per second at the throughput ceiling). Multi-producer now that
        // several sender threads return objects after transmit; the (single) queue-writer thread
        // rents via RentSendData. ConcurrentQueue handles that. Bounded so a client whose factory
        // doesn't rent (leaves the pool filling) can't grow it without limit.
        private readonly ConcurrentQueue<TSendData> sendDataPool = new();
        private const int SendDataPoolCap = 2048;
        private long objectsFromPipeline;
        private long objectsIntoChannel;
        private Pipe? receivePipeline;

        // Per-source/destination IP caches so the parse thread (the single-reader
        // ParseFromPipeAsync consumer) doesn't allocate an IPEndPoint + two IPAddress objects
        // on every received packet. Capped so a flood of spoofed source addresses can't grow
        // them without bound. Only touched from GetSocketData on the parse thread.
        private const int IpCacheCap = 512;
        private readonly Dictionary<uint, IPAddress> ipAddressCache = new();
        private readonly Dictionary<(uint Address, int Port), IPEndPoint> sourceEndPointCache = new();

        private long lastSuccessfulSendTimestamp = Stopwatch.GetTimestamp();
        private long firstSendFailureTimestamp;
        private long lastErrorEmitTimestamp;
        private const double SendFaultThresholdMS = 3_000;
        private const double ErrorEmitThrottleMS = 5_000;

        /// <param name="senderCount">
        /// Number of sender shards (thread + queue + socket each). Defaults to 1, which is the
        /// original single-threaded behavior. Raise it to spread the kernel's per-packet send cost
        /// across cores when one sender thread saturates.
        /// </param>
        public Client(int packetSize, Func<TPacketType, Task>? channelWriter, Action? channelWriterComplete, int senderCount = 1)
        {
            if (senderCount < 1)
                throw new ArgumentOutOfRangeException(nameof(senderCount));

            this.receiveBufferSize = packetSize + HeaderDataSize;
            this.senderCount = senderCount;
            this.sendQueues = new Channel<TSendData>[senderCount];
            this.sendThreads = new Thread[senderCount];

            for (int i = 0; i < senderCount; i++)
            {
                // Each shard keeps the original bound, so total capacity scales with the shard
                // count and one busy universe range can't starve another.
                this.sendQueues[i] = Channel.CreateBounded<TSendData>(new BoundedChannelOptions(10_000)
                {
                    SingleReader = true,
                    SingleWriter = true,
                    AllowSynchronousContinuations = true,
                    FullMode = BoundedChannelFullMode.Wait
                });
            }

            this.sendRecorder = HistogramFactory
                .With64BitBucketSize()                  //LongHistogram
                .WithValuesFrom(1)                      //Default value
                .WithValuesUpTo(TimeStamp.Minutes(1))   //Default value
                .WithPrecisionOf(3)                     //Default value
                .WithThreadSafeReads()                  //returns a Recorder that wraps the LongConcurrentHistogram
                .Create();

            this.ageRecorder = HistogramFactory
                .With64BitBucketSize()                  //LongHistogram
                .WithValuesFrom(1)                      //Default value
                .WithValuesUpTo(TimeStamp.Minutes(1))   //Default value
                .WithPrecisionOf(3)                     //Default value
                .WithThreadSafeReads()                  //returns a Recorder that wraps the LongConcurrentHistogram
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

            this.receivePipeline = new Pipe(new PipeOptions(pauseWriterThreshold: 10_000_000));

            if (channelWriter != null)
            {
                this.parserTask = Task.Factory.StartNew(async () =>
                {
                    // Parse and then call the transfomer to get our internal deconstructed data
                    await ParseFromPipeAsync(this.receivePipeline.Reader, channelWriter, CancellationToken.None);

                    channelWriterComplete?.Invoke();
                }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();
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
                    queue.Writer.Complete();

                StopReceive();

                foreach (var thread in this.sendThreads)
                    thread.Join(5_000);
            }
        }

        public IObservable<Exception> OnError => this.errorSubject.AsObservable();

        public SendStatistics GetSendStatistics(bool reset)
        {
            var sendStatsCopy = this.sendRecorder.GetIntervalHistogram();
            var ageStatsCopy = this.ageRecorder.GetIntervalHistogram();

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
            long delta1 = Interlocked.Read(ref this.objectsFromPipeline) - Interlocked.Read(ref this.objectsIntoChannel);

            var receiveStatistics = new ReceiveStatistics
            {
                ObjectsInQueue1 = (int)delta1
            };

            return receiveStatistics;
        }

        private void Sender(int senderIndex)
        {
            var reader = this.sendQueues[senderIndex].Reader;

            while (!this.senderCTS.IsCancellationRequested)
            {
                if (!reader.TryRead(out var sendData))
                {
                    // Block until data is available or the channel is completed. The waiter
                    // is completed inline by the writer (AllowSynchronousContinuations), so
                    // waking up doesn't depend on the thread pool.
                    if (!reader.WaitToReadAsync().AsTask().GetAwaiter().GetResult())
                        break;

                    continue;
                }

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

        protected void StartReceive()
        {
            if (this.receiverCTS != null)
                throw new Exception("Already running");

            InitializeReceiveSocket();

            this.objectsFromPipeline = 0;
            this.objectsIntoChannel = 0;

            this.receiverCTS = new CancellationTokenSource();

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

            this.receiveClock.Restart();
        }

        private void StopReceive()
        {
            this.receiverCTS?.Cancel();

            // Close the socket first — that unblocks the receive thread's blocking read
            // so it can observe the cancellation and exit.
            DisposeReceiveSocket();

            this.receiveThread?.Join(5_000);

            if (this.parserTask?.IsCanceled == false)
                this.parserTask?.Wait(5_000);
            this.parserTask?.Dispose();

            this.receiverCTS?.Dispose();

            this.receiveThread = null;
            this.receiverCTS = null;
            this.parserTask = null;

            this.receivePipeline = null;
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

        /// <param name="shardKey">
        /// Selects the sender shard. Packets sharing a key are guaranteed to go out on the same
        /// thread and socket, in order — pass the universe id so a universe's sequence numbers
        /// stay monotonic. Any stable key works; it is reduced modulo the shard count.
        /// </param>
        protected async ValueTask QueuePacket(int allocatePacketLength, bool important, Func<TSendData> sendDataFactory, Func<Memory<byte>, int> packetWriter, int shardKey = 0)
        {
            var queue = this.sendQueues[ShardFor(shardKey)];

            if (!IsOperational)
            {
                // Clear every queue, not just this shard's — the client as a whole is down.
                foreach (var q in this.sendQueues)
                {
                    while (q.Reader.TryRead(out var sendData))
                    {
                        sendData.Data?.Dispose();
                    }
                }

                return;
            }

            var memory = this.memoryPool.Rent(allocatePacketLength);

            var newSendData = sendDataFactory();

            newSendData.Data = memory;
            newSendData.Important = important;

            int packetLength = packetWriter(memory.Memory);

            newSendData.DataLength = packetLength;

            newSendData.StartAgeStopwatch();

            if (queue.Writer.TryWrite(newSendData))
            {
                Interlocked.Increment(ref this.queueItemCounter);
            }
            else
            {
                if (important)
                {
                    await queue.Writer.WriteAsync(newSendData);

                    Interlocked.Increment(ref this.queueItemCounter);
                }
                else
                {
                    // Discard, indicate queue full. Dispose the rented buffer and return the
                    // send-data object to the pool (it never reached the sender's finally).
                    Interlocked.Increment(ref this.fullQueue);
                    ReturnSendData(newSendData);
                }
            }
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
        protected async ValueTask QueueBarrierPacket(int allocatePacketLength, Func<TSendData> sendDataFactory, Func<Memory<byte>, int> packetWriter, int shardKey = 0)
        {
            if (this.senderCount == 1)
            {
                // Single shard: FIFO already guarantees it follows everything queued before it.
                await QueuePacket(allocatePacketLength, important: true, sendDataFactory, packetWriter, shardKey);

                return;
            }

            if (!IsOperational)
                return;

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

                await this.sendQueues[i].Writer.WriteAsync(marker);

                Interlocked.Increment(ref this.queueItemCounter);
            }

            var memory = this.memoryPool.Rent(allocatePacketLength);

            var newSendData = sendDataFactory();

            newSendData.Data = memory;
            newSendData.Important = true;
            newSendData.DataLength = packetWriter(memory.Memory);
            newSendData.BarrierWait = countdown;
            newSendData.StartAgeStopwatch();

            await this.sendQueues[targetShard].Writer.WriteAsync(newSendData);

            Interlocked.Increment(ref this.queueItemCounter);
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

        protected const int HeaderDataSize = 24;

        private void WriteSocketDataToBuffer(int receivedBytes, long timestampTicks, IPEndPoint remoteEndPoint, IPAddress destAddress, Span<byte> buffer)
        {
            // Write the packet size
            BinaryPrimitives.WriteInt32LittleEndian(buffer, receivedBytes);

            int writePos = 4;
            // Write timestamp
            BinaryPrimitives.WriteInt64LittleEndian(buffer[writePos..], timestampTicks);
            writePos += 8;

            writePos += WriteIpAddress(buffer[writePos..], remoteEndPoint.Address);

            // Socket port
            BinaryPrimitives.WriteInt32LittleEndian(buffer[writePos..], remoteEndPoint.Port);
            writePos += 4;

            writePos += WriteIpAddress(buffer[writePos..], destAddress);

#if DEBUG
            if (writePos > HeaderDataSize)
                throw new ArgumentOutOfRangeException("Invalid data");
#endif
        }

        private int WriteIpAddress(Span<byte> buffer, IPAddress input)
        {
            byte[] sourceAddr = input.GetAddressBytes();
            sourceAddr.CopyTo(buffer);

            return sourceAddr.Length;
        }

        public void GetSocketData(ReadOnlySpan<byte> buffer, out int packetSize, out double timestampMS, out IPEndPoint source, out IPAddress destination)
        {
            packetSize = BinaryPrimitives.ReadInt32LittleEndian(buffer);

            Debug.Assert(packetSize > 0 && packetSize < this.receiveBufferSize);

            long timestampTicks = BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(4));
            timestampMS = (double)timestampTicks / Stopwatch.Frequency * 1000;

            var sourceAddressBytes = buffer.Slice(12, 4);
            uint sourceKey = BinaryPrimitives.ReadUInt32LittleEndian(sourceAddressBytes);
            int sourcePort = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(16, 4));

            if (!this.sourceEndPointCache.TryGetValue((sourceKey, sourcePort), out var sourceEndPoint))
            {
                if (this.sourceEndPointCache.Count >= IpCacheCap)
                    this.sourceEndPointCache.Clear();

                sourceEndPoint = new IPEndPoint(GetCachedIpAddress(sourceKey, sourceAddressBytes), sourcePort);
                this.sourceEndPointCache[(sourceKey, sourcePort)] = sourceEndPoint;
            }

            source = sourceEndPoint;

            var destinationAddressBytes = buffer.Slice(20, 4);
            destination = GetCachedIpAddress(BinaryPrimitives.ReadUInt32LittleEndian(destinationAddressBytes), destinationAddressBytes);
        }

        private IPAddress GetCachedIpAddress(uint key, ReadOnlySpan<byte> addressBytes)
        {
            if (!this.ipAddressCache.TryGetValue(key, out var address))
            {
                if (this.ipAddressCache.Count >= IpCacheCap)
                    this.ipAddressCache.Clear();

                address = new IPAddress(addressBytes);
                this.ipAddressCache[key] = address;
            }

            return address;
        }

        private void Receiver()
        {
            var writer = this.receivePipeline!.Writer;

            while (!this.receiverCTS!.IsCancellationRequested)
            {
                try
                {
                    Memory<byte> memory = writer.GetMemory(this.receiveBufferSize);

                    int receivedBytes = ReceiveData(memory[HeaderDataSize..], out IPEndPoint? remoteEndPoint, out IPAddress? destinationAddress);

                    // Capture the timestamp first so it's as accurate as possible
                    long timestampTicks = this.receiveClock.ElapsedTicks;

                    if (remoteEndPoint == null || destinationAddress == null ||
                        remoteEndPoint.AddressFamily != AddressFamily.InterNetwork ||
                        destinationAddress.AddressFamily != AddressFamily.InterNetwork)
                    {
                        // Missing or not IPv4
                        continue;
                    }

                    if (receivedBytes > 0)
                    {
                        WriteSocketDataToBuffer(receivedBytes, timestampTicks, remoteEndPoint, destinationAddress, memory.Span);

                        // Commit data to the pipe
                        writer.Advance(receivedBytes + HeaderDataSize);

                        Interlocked.Increment(ref this.objectsFromPipeline);

                        // Below the pause threshold this completes synchronously; if the parser
                        // is far behind we block this thread, which is the desired backpressure.
                        ValueTask<FlushResult> flushTask = writer.FlushAsync();
                        FlushResult flushResult = flushTask.IsCompletedSuccessfully ? flushTask.Result : flushTask.AsTask().GetAwaiter().GetResult();

                        if (flushResult.IsCompleted)
                            break;
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

            // Signal that writing is complete
            writer.Complete();
        }

        private async Task ParseFromPipeAsync(
            PipeReader reader,
            Func<TPacketType, Task> channelWriter,
            CancellationToken cancellationToken)
        {
            async Task processData(ReadOnlyMemory<byte> data, double timestampMS, IPEndPoint sourceIP, IPAddress destinationIP)
            {
                try
                {
                    var parsedObject = TryParseObject(data, timestampMS, sourceIP, destinationIP);
                    if (parsedObject != null)
                    {
                        Interlocked.Increment(ref this.objectsIntoChannel);

                        await channelWriter(parsedObject);
                    }
                }
                catch (Exception ex)
                {
                    this.errorSubject.OnNext(ex);
                }
            }

            async Task<int> processBuffer(ReadOnlySequence<byte> buffer, int packetSize, double timestampMS, IPEndPoint sourceIP, IPAddress destinationIP)
            {
                if (buffer.Length >= HeaderDataSize + packetSize)
                {
                    var packetSequence = buffer.Slice(HeaderDataSize, packetSize);

                    if (packetSequence.IsSingleSegment)
                    {
                        await processData(packetSequence.First, timestampMS, sourceIP, destinationIP);
                    }
                    else
                    {
                        var copyBuf = this.memoryPool.Rent((int)packetSequence.Length);
                        packetSequence.CopyTo(copyBuf.Memory.Span);

                        await processData(copyBuf.Memory, timestampMS, sourceIP, destinationIP);

                        copyBuf.Dispose();
                    }

                    return HeaderDataSize + packetSize;
                }

                return 0;
            }

            while (true)
            {
                ReadResult result = await reader.ReadAsync(cancellationToken);
                ReadOnlySequence<byte> buffer = result.Buffer;

                while (buffer.Length >= HeaderDataSize)
                {
                    // We have enough data to read the packet size and details
                    ReadOnlySequence<byte> header = buffer.Slice(0, HeaderDataSize);

                    int advanceBytes;
                    if (header.IsSingleSegment)
                    {
                        GetSocketData(header.First.Span, out int packetSize, out double timestampMS, out IPEndPoint sourceIP, out IPAddress destinationIP);

                        advanceBytes = await processBuffer(buffer, packetSize, timestampMS, sourceIP, destinationIP);
                    }
                    else
                    {
                        // Unlikely that we'll have multiple segments for 24 bytes, but could happen
                        var copyBuf = this.memoryPool.Rent((int)header.Length);
                        header.CopyTo(copyBuf.Memory.Span);

                        GetSocketData(copyBuf.Memory.Span, out int packetSize, out double timestampMS, out IPEndPoint sourceIP, out IPAddress destinationIP);

                        advanceBytes = await processBuffer(buffer, packetSize, timestampMS, sourceIP, destinationIP);

                        copyBuf.Dispose();
                    }

                    if (advanceBytes == 0)
                        // We don't have enough data yet
                        break;

                    buffer = buffer.Slice(advanceBytes); // Advance buffer
                }

                // Indicate consumed bytes
                reader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted)
                    break;
            }

            await reader.CompleteAsync();
        }
    }
}
