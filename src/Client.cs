using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Sockets;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;
using HdrHistogram;

namespace Haukcode.HighPerfComm;

public abstract class Client<T, TResult> : IDisposable where T : SendData
{
    private readonly CancellationTokenSource shutdownCTS = new();
    private readonly HdrHistogram.Recorder sendRecorder;
    private readonly HdrHistogram.Recorder ageRecorder;
    private readonly Channel<T> sendQueue;
    private int queueItemCounter;
    private int droppedPackets;
    private int fullQueue;
    private long totalPackets;
    protected readonly ISubject<Exception> errorSubject;
    private Task? receiveTask1;
    private Task? receiveTask2;
    private readonly Task sendTask;
    private readonly Stopwatch receiveClock = new();
    private readonly MemoryPool<byte> memoryPool = MemoryPool<byte>.Shared;
    private readonly Func<T> sendDataFactory;
    private readonly Memory<byte> receiveBufferMem1;
    private readonly Memory<byte> receiveBufferMem2;

    public Client(Func<T> sendDataFactory, int receiveBufferSize = 20480)
    {
        this.sendDataFactory = sendDataFactory;
        this.sendQueue = Channel.CreateBounded<T>(new BoundedChannelOptions(10000)
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

        var receiveBuffer = GC.AllocateArray<byte>(length: receiveBufferSize, pinned: true);
        this.receiveBufferMem1 = receiveBuffer.AsMemory();

        receiveBuffer = GC.AllocateArray<byte>(length: receiveBufferSize, pinned: true);
        this.receiveBufferMem2 = receiveBuffer.AsMemory();

        this.sendTask = Task.Factory.StartNew(Sender, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();
    }

    public bool IsOperational => !this.shutdownCTS.IsCancellationRequested;

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            this.shutdownCTS.Cancel();

            this.sendQueue.Writer.Complete();

            if (this.receiveTask1?.IsCanceled == false)
                this.receiveTask1?.Wait(5_000);
            this.receiveTask1?.Dispose();

            if (this.receiveTask2 != null)
            {
                if (this.receiveTask2?.IsCanceled == false)
                    this.receiveTask2?.Wait(5_000);
                this.receiveTask2?.Dispose();
            }

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
            //DestinationCount = this.usedDestinations.Count,
            FullQueue = this.fullQueue,
            TotalPackets = this.totalPackets,
            SendStats = sendStatsCopy,
            AgeStats = ageStatsCopy
        };

        if (reset)
        {
            // Reset
            this.droppedPackets = 0;
            //this.usedDestinations.Clear();
            this.fullQueue = 0;
            this.totalPackets = 0;
        }

        return sendStatistics;
    }

    protected abstract ValueTask<int> SendPacketAsync(T sendData, ReadOnlyMemory<byte> payload);

    private async Task Sender()
    {
        while (!this.shutdownCTS.IsCancellationRequested)
        {
            await foreach (var sendData in this.sendQueue.Reader.ReadAllAsync())
            {
                Interlocked.Decrement(ref this.queueItemCounter);

                try
                {
                    this.ageRecorder.RecordValue(sendData.Enqueued.ElapsedTicks);

                    if (sendData.AgeMS > 100 && !sendData.Important)
                    {
                        // Old, discard
                        this.droppedPackets++;
                        //Console.WriteLine($"Age {sendData.Enqueued.Elapsed.TotalMilliseconds:N2}   queue length = {this.sendQueue.Count}   Dropped = {this.droppedPackets}");
                        continue;
                    }

                    long startTimestamp = Stopwatch.GetTimestamp();

                    // Send packet
                    await SendPacketAsync(sendData, sendData.Data.Memory[..sendData.DataLength]);

                    long elapsedTicks = Stopwatch.GetTimestamp() - startTimestamp;
                    this.sendRecorder.RecordValue(elapsedTicks);

                    this.totalPackets++;
                }
                catch (Exception ex)
                {
                    if (ex is OperationCanceledException)
                        continue;

                    //Console.WriteLine($"Exception in Sender handler: {ex.Message}");
                    this.errorSubject.OnNext(ex);

                    if (ex is System.Net.Sockets.SocketException)
                    {
                        // Network unreachable
                        this.shutdownCTS.Cancel();
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

    public void StartReceive()
    {
        this.receiveTask1 ??= Task.Factory.StartNew(() => Receiver(false), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();

        if (SupportsTwoReceivers)
            this.receiveTask2 ??= Task.Factory.StartNew(() => Receiver(true), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();

        this.receiveClock.Restart();
    }

    protected async ValueTask QueuePacket(int allocatePacketLength, bool important, Func<T, Memory<byte>, int> packetWriter)
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

        var newSendData = this.sendDataFactory();

        newSendData.Data = memory;
        newSendData.Important = important;

        int packetLength = packetWriter(newSendData, memory.Memory);

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

    protected abstract ValueTask<(int ReceivedBytes, TResult Result)> ReceiveData1(Memory<byte> memory, CancellationToken cancelToken);

    protected abstract ValueTask<(int ReceivedBytes, TResult Result)> ReceiveData2(Memory<byte> memory, CancellationToken cancelToken);

    protected abstract bool SupportsTwoReceivers { get; }

    protected abstract void ParseReceiveData(ReadOnlyMemory<byte> memory, TResult result, double timestampMS);

    private async Task Receiver(bool receiver2)
    {
        while (!this.shutdownCTS.IsCancellationRequested)
        {
            try
            {
                (int ReceivedBytes, TResult Result) result;
                Memory<byte> receiveBufferMem;

                if (receiver2)
                {
                    result = await ReceiveData2(this.receiveBufferMem1, this.shutdownCTS.Token);
                    receiveBufferMem = this.receiveBufferMem1;
                }
                else
                {
                    result = await ReceiveData1(this.receiveBufferMem2, this.shutdownCTS.Token);
                    receiveBufferMem = this.receiveBufferMem2;
                }

                // Capture the timestamp first so it's as accurate as possible
                double timestampMS = this.receiveClock.Elapsed.TotalMilliseconds;

                if (result.ReceivedBytes > 0)
                {
                    var readBuffer = receiveBufferMem[..result.ReceivedBytes];

                    ParseReceiveData(readBuffer, result.Result, timestampMS);
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
                    this.shutdownCTS.Cancel();
                    break;
                }
            }
        }
    }
}
