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
using System.Threading.Channels;
using HdrHistogram;

namespace Haukcode.HighPerfComm;

public abstract class Client<TSendData, TPacketType> : IDisposable where TSendData : SendData
{
    private readonly CancellationTokenSource senderCTS = new();
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
    private Task? receiveTask;
    private Task? parserTask;
    private readonly Task sendTask;
    private readonly Stopwatch receiveClock = new();
    private readonly MemoryPool<byte> memoryPool = MemoryPool<byte>.Shared;
    private long objectsFromPipeline;
    private long objectsIntoChannel;
    private Pipe? receivePipeline;

    public Client(int packetSize)
    {
        this.receiveBufferSize = packetSize + HeaderDataSize;
        this.sendQueue = Channel.CreateBounded<TSendData>(new BoundedChannelOptions(10000)
        {
            SingleReader = true,
            SingleWriter = true,
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
    }

    protected abstract ValueTask<(int ReceivedBytes, SocketReceiveMessageFromResult Result)> ReceiveData(Memory<byte> memory, CancellationToken cancelToken);

    protected abstract ValueTask<int> SendPacketAsync(TSendData sendData, ReadOnlyMemory<byte> payload);

    protected abstract void InitializeReceiveSocket();

    protected abstract void DisposeReceiveSocket();

    protected abstract TPacketType? TryParseObject(ReadOnlyMemory<byte> buffer, double timestampMS, IPEndPoint sourceIP, IPAddress destinationIP);

    public bool IsOperational => !this.senderCTS.IsCancellationRequested;

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
                        this.ageRecorder.RecordValue(sendData.Enqueued.ElapsedTicks);
                    }

                    if (sendData.AgeMS > 200 && !sendData.Important)
                    {
                        // Old, discard
                        this.droppedPackets++;
                        //Console.WriteLine($"Age {sendData.Enqueued.Elapsed.TotalMilliseconds:N2}   queue length = {this.sendQueue.Count}   Dropped = {this.droppedPackets}");
                        continue;
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
                }
                catch (Exception ex)
                {
                    if (ex is OperationCanceledException)
                        continue;

                    this.errorSubject.OnNext(ex);

                    if (ex is System.Net.Sockets.SocketException)
                    {
                        // Network unreachable
                        this.senderCTS.Cancel();
                        break;
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

    private void StartReceive()
    {
        if (this.receiverCTS != null)
            throw new Exception("Already running");

        InitializeReceiveSocket();

        this.objectsFromPipeline = 0;
        this.objectsIntoChannel = 0;

        this.receiverCTS = new CancellationTokenSource();

        this.receiveTask = Task.Factory.StartNew(Receiver, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();

        this.receiveClock.Restart();
    }

    public void StopReceive()
    {
        this.receiverCTS?.Cancel();

        if (this.receiveTask?.IsCanceled == false)
            this.receiveTask?.Wait(5_000);
        this.receiveTask?.Dispose();

        if (this.parserTask?.IsCanceled == false)
            this.parserTask?.Wait(5_000);
        this.parserTask?.Dispose();

        this.receiverCTS?.Dispose();

        this.receiveTask = null;
        this.receiverCTS = null;
        this.parserTask = null;

        this.receivePipeline = null;

        DisposeReceiveSocket();
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

    private async Task Receiver()
    {
        var writer = this.receivePipeline!.Writer;

        while (!this.receiverCTS!.IsCancellationRequested)
        {
            try
            {
                Memory<byte> memory = writer.GetMemory(this.receiveBufferSize);

                var result = await ReceiveData(memory[HeaderDataSize..], this.receiverCTS.Token);

                // Capture the timestamp first so it's as accurate as possible
                long timestampTicks = this.receiveClock.ElapsedTicks;

                if (result.Result.RemoteEndPoint is not IPEndPoint remoteEndPoint ||
                    remoteEndPoint.AddressFamily != AddressFamily.InterNetwork ||
                    result.Result.PacketInformation.Address.AddressFamily != AddressFamily.InterNetwork)
                {
                    // Missing or not IPv4
                    continue;
                }

                int receivedBytes = result.ReceivedBytes;
                if (receivedBytes > 0)
                {
                    WriteSocketDataToBuffer(receivedBytes, timestampTicks, remoteEndPoint, result.Result.PacketInformation.Address, memory.Span);

                    // Commit data to the pipe
                    writer.Advance(receivedBytes + HeaderDataSize);

                    Interlocked.Increment(ref this.objectsFromPipeline);

                    FlushResult flushResult = await writer.FlushAsync(this.receiverCTS.Token);

                    if (flushResult.IsCompleted)
                        break;
                }
            }
            catch (Exception ex)
            {
                if (ex is not OperationCanceledException)
                {
                    this.errorSubject.OnNext(ex);
                }

                if (ex is System.Net.Sockets.SocketException)
                {
                    // Network unreachable
                    this.receiverCTS.Cancel();
                    break;
                }
            }
        }

        // Signal that writing is complete
        await writer.CompleteAsync();
    }

    public void StartRecordPipeline(Func<TPacketType, Task> channelWriter, Action writeComplete)
    {
        this.receivePipeline = new Pipe(new PipeOptions(pauseWriterThreshold: 10_000_000));

        // Start the pipeline components (we leave it running once it's started)
        StartReceive();

        this.parserTask = Task.Factory.StartNew(async () =>
        {
            await ParseFromPipeAsync(this.receivePipeline.Reader, channelWriter, CancellationToken.None);

            writeComplete();
        }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
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
