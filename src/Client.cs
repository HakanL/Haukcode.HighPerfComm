using System;
using System.Buffers;
using System.Buffers.Binary;
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
        private readonly Channel<TSendData> sendQueue;
        private readonly int receiveBufferSize;
        private int queueItemCounter;
        private int droppedPackets;
        private int fullQueue;
        private long totalPackets;
        protected readonly ISubject<Exception> errorSubject;
        private Thread? receiveThread;
        private Task? parserTask;
        private readonly Task sendTask;
        private readonly Stopwatch receiveClock = new Stopwatch();
        private readonly MemoryPool<byte> memoryPool = MemoryPool<byte>.Shared;
        private long objectsFromPipeline;
        private long objectsIntoChannel;
        private Pipe? receivePipeline;
        private long lastSuccessfulSendTimestamp = Stopwatch.GetTimestamp();
        private long firstSendFailureTimestamp;
        private long lastErrorEmitTimestamp;
        private const double SendFaultThresholdMS = 3_000;
        private const double ErrorEmitThrottleMS = 5_000;

        public Client(int packetSize, Func<TPacketType, Task>? channelWriter, Action? channelWriterComplete)
        {
            this.receiveBufferSize = packetSize + HeaderDataSize;
            this.sendQueue = Channel.CreateBounded<TSendData>(new BoundedChannelOptions(10_000)
            {
                SingleReader = true,
                SingleWriter = true,
                AllowSynchronousContinuations = true,
                FullMode = BoundedChannelFullMode.Wait
            });

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

            this.sendTask = Task.Factory.StartNew(Sender, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();

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

        protected abstract ValueTask<int> SendPacketAsync(TSendData sendData, ReadOnlyMemory<byte> payload);

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

                this.sendQueue.Writer.Complete();

                StopReceive();

                if (this.sendTask?.IsCanceled == false)
                    this.sendTask?.Wait(5_000);
                this.sendTask?.Dispose();
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
                // Reset
                this.droppedPackets = 0;
                this.fullQueue = 0;
                this.totalPackets = 0;
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

        private async Task Sender()
        {
            while (!this.senderCTS.IsCancellationRequested)
            {
                await foreach (var sendData in this.sendQueue.Reader.ReadAllAsync())
                {
                    Interlocked.Decrement(ref this.queueItemCounter);

                    try
                    {
                        if (!sendData.Important)
                        {
                            // Ignore the important packets when recording age, not relevant
                            this.ageRecorder.RecordValue(sendData.AgeTicks);

                            if (sendData.AgeMS > 200)
                            {
                                // Old, discard
                                this.droppedPackets++;
                                //Console.WriteLine($"Age {sendData.Enqueued.Elapsed.TotalMilliseconds:N2}   queue length = {this.sendQueue.Count}   Dropped = {this.droppedPackets}");
                                continue;
                            }
                        }

                        long startTimestamp = Stopwatch.GetTimestamp();

                        // Send packet
                        await SendPacketAsync(sendData, sendData.Data.Memory[..sendData.DataLength]);

                        if (!sendData.Important)
                        {
                            // Ignore recording important packets since we may have a burst of a lot of them (blackouts for example)
                            long elapsedTicks = Stopwatch.GetTimestamp() - startTimestamp;
                            this.sendRecorder.RecordValue(elapsedTicks);
                        }

                        this.totalPackets++;

                        // Successful send clears any pending fault state.
                        this.lastSuccessfulSendTimestamp = Stopwatch.GetTimestamp();
                        this.firstSendFailureTimestamp = 0;
                    }
                    catch (Exception ex)
                    {
                        if (ex is OperationCanceledException)
                            continue;

                        if (ex is System.Net.Sockets.SocketException)
                        {
                            if (this.firstSendFailureTimestamp == 0)
                                this.firstSendFailureTimestamp = Stopwatch.GetTimestamp();

                            // Throttle notifications so a persistent failure (e.g. the bound
                            // NIC went away after a network change) doesn't spam the log once
                            // per packet. The first failure is reported immediately.
                            if (ElapsedMs(this.lastErrorEmitTimestamp) >= ErrorEmitThrottleMS)
                            {
                                this.lastErrorEmitTimestamp = Stopwatch.GetTimestamp();

                                this.errorSubject.OnNext(ex);
                            }

                            // Transient send failure (e.g. errno 101 Network unreachable during a NIC flap).
                            // Don't kill the sender — back off briefly and keep draining the queue so we recover when routing returns.
                            try
                            {
                                await Task.Delay(100, this.senderCTS.Token);
                            }
                            catch (OperationCanceledException)
                            {
                            }
                        }
                        else
                        {
                            this.errorSubject.OnNext(ex);
                        }
                    }
                    finally
                    {
                        // Return to pool
                        sendData.Data?.Dispose();
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

        protected async ValueTask QueuePacket(int allocatePacketLength, bool important, Func<TSendData> sendDataFactory, Func<Memory<byte>, int> packetWriter)
        {
            if (!IsOperational)
            {
                // Clear queue
                while (this.sendQueue.Reader.TryRead(out var sendData))
                {
                    sendData.Data?.Dispose();
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

            if (this.sendQueue.Writer.TryWrite(newSendData))
            {
                Interlocked.Increment(ref this.queueItemCounter);
            }
            else
            {
                if (important)
                {
                    await this.sendQueue.Writer.WriteAsync(newSendData);

                    Interlocked.Increment(ref this.queueItemCounter);
                }
                else
                {
                    // Discard, indicate queue full
                    this.fullQueue++;
                }
            }
        }

        protected async ValueTask SendImmediateAsync(int allocatePacketLength, bool important, Func<TSendData> sendDataFactory, Func<Memory<byte>, int> packetWriter)
        {
            if (!IsOperational)
                return;

            var memory = this.memoryPool.Rent(allocatePacketLength);

            try
            {
                var sendData = sendDataFactory();

                sendData.Data = memory;
                sendData.Important = important;

                int packetLength = packetWriter(memory.Memory);

                sendData.DataLength = packetLength;

                await SendPacketAsync(sendData, memory.Memory[..packetLength]);

                this.totalPackets++;
            }
            finally
            {
                memory.Dispose();
            }
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

            source = new IPEndPoint(new IPAddress(buffer.Slice(12, 4)), BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(16, 4)));
            destination = new IPAddress(buffer.Slice(20, 4));
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
