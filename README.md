# Haukcode.HighPerfComm [![NuGet Version](http://img.shields.io/nuget/v/Haukcode.HighPerfComm.svg?style=flat)](https://www.nuget.org/packages/Haukcode.HighPerfComm/)

A high-performance .NET library for packet-based network communication with built-in memory pooling, pipelining, and performance monitoring.

## Features

- **High Performance**: Built on modern .NET primitives including `System.IO.Pipelines`, `System.Threading.Channels`, and `System.Buffers` for maximum throughput and minimal allocations
- **Memory Efficient**: Uses `MemoryPool<byte>` for buffer management to reduce GC pressure
- **Asynchronous**: Fully async/await pattern with cancellation support
- **Statistics & Monitoring**: Built-in HDR histograms for tracking send/receive performance and queue metrics
- **Flexible**: Abstract base class design allowing custom protocol implementations
- **Packet Age Management**: Automatic dropping of stale packets based on configurable age thresholds
- **Back-pressure Handling**: Configurable bounded channels with overflow protection
- **Multi-Target**: Supports .NET Standard 2.1, .NET 8.0, and .NET 9.0

## Installation

Install via NuGet Package Manager:

```bash
dotnet add package Haukcode.HighPerfComm
```

Or via Package Manager Console:

```powershell
Install-Package Haukcode.HighPerfComm
```

## Quick Start

The library provides an abstract `Client<TSendData, TPacketType>` class that you extend to implement your specific packet protocol:

```csharp
using Haukcode.HighPerfComm;
using System.Net;
using System.Net.Sockets;

// Define your packet type
public class MyPacket
{
    public byte[] Data { get; set; }
    public IPEndPoint Source { get; set; }
    public double Timestamp { get; set; }
}

// Define your send data (if you need custom fields beyond the base)
public class MySendData : SendData
{
    // Add custom fields if needed
}

// Implement the abstract client
public class MyUdpClient : Client<MySendData, MyPacket>
{
    private UdpClient udpClient;
    private readonly IPEndPoint localEndPoint;
    
    public MyUdpClient(IPEndPoint localEndPoint, int packetSize, 
                       Func<MyPacket, Task> packetHandler, 
                       Action onComplete) 
        : base(packetSize, packetHandler, onComplete)
    {
        this.localEndPoint = localEndPoint;
    }

    protected override void InitializeReceiveSocket()
    {
        udpClient = new UdpClient(localEndPoint);
        udpClient.Client.ReceiveBufferSize = 1024 * 1024; // 1MB buffer
        
        // Start receiving
        StartReceive();
    }

    protected override void DisposeReceiveSocket()
    {
        udpClient?.Dispose();
    }

    protected override async ValueTask<(int ReceivedBytes, SocketReceiveMessageFromResult Result)> 
        ReceiveData(Memory<byte> memory, CancellationToken cancelToken)
    {
        var result = await udpClient.Client.ReceiveMessageFromAsync(
            memory, 
            SocketFlags.None, 
            new IPEndPoint(IPAddress.Any, 0), 
            cancelToken);
        
        return (result.ReceivedBytes, result);
    }

    protected override async ValueTask<int> SendPacketAsync(MySendData sendData, 
                                                             ReadOnlyMemory<byte> payload)
    {
        // Implement your sending logic
        return await udpClient.Client.SendAsync(payload, SocketFlags.None);
    }

    protected override MyPacket? TryParseObject(ReadOnlyMemory<byte> buffer, 
                                                double timestampMS, 
                                                IPEndPoint sourceIP, 
                                                IPAddress destinationIP)
    {
        // Parse your packet format
        return new MyPacket
        {
            Data = buffer.ToArray(),
            Source = sourceIP,
            Timestamp = timestampMS
        };
    }

    // Helper method to send packets
    public async Task SendAsync(byte[] data, bool important = false)
    {
        await QueuePacket(
            allocatePacketLength: data.Length,
            important: important,
            sendDataFactory: () => new MySendData(),
            packetWriter: (buffer) =>
            {
                data.CopyTo(buffer);
                return data.Length;
            });
    }
}
```

## Usage Example

```csharp
// Create a handler for received packets
async Task PacketHandler(MyPacket packet)
{
    Console.WriteLine($"Received {packet.Data.Length} bytes from {packet.Source}");
    // Process your packet
}

void OnComplete()
{
    Console.WriteLine("Receiver completed");
}

// Create and initialize the client
var localEndPoint = new IPEndPoint(IPAddress.Any, 5000);
using var client = new MyUdpClient(localEndPoint, 1500, PacketHandler, OnComplete);

// Subscribe to errors
client.OnError.Subscribe(ex => 
{
    Console.WriteLine($"Error: {ex.Message}");
});

// Send packets
await client.SendAsync(new byte[] { 1, 2, 3, 4, 5 });
await client.SendAsync(new byte[] { 6, 7, 8, 9, 10 }, important: true);

// Get statistics
var sendStats = client.GetSendStatistics(reset: false);
Console.WriteLine($"Total packets sent: {sendStats.TotalPackets}");
Console.WriteLine($"Dropped packets: {sendStats.DroppedPackets}");
Console.WriteLine($"Queue length: {sendStats.QueueLength}");

var receiveStats = client.GetReceiveStatistics();
Console.WriteLine($"Objects in queue: {receiveStats.ObjectsInQueue1}");
```

## Architecture

### Core Components

1. **Client Base Class**: The abstract `Client<TSendData, TPacketType>` manages the lifecycle of send and receive operations
2. **Send Pipeline**: Uses a bounded `Channel<TSendData>` with a dedicated sender task for non-blocking packet transmission
3. **Receive Pipeline**: Uses `System.IO.Pipelines.Pipe` for efficient buffer management during packet reception
4. **Parser Task**: Processes received data from the pipeline and transforms it into strongly-typed packet objects

### Data Flow

```
Send Path:
QueuePacket() → Channel → Sender Task → SendPacketAsync() → Network

Receive Path:
Network → ReceiveData() → Pipe → Parser Task → TryParseObject() → channelWriter
```

## Advanced Features

### Packet Prioritization

The library supports important vs. non-important packets:

```csharp
// Important packets are always sent, even if the queue is full
await client.SendAsync(criticalData, important: true);

// Non-important packets can be dropped if they're too old (>200ms by default)
await client.SendAsync(regularData, important: false);
```

### Statistics and Monitoring

The library provides detailed statistics using HDR histograms:

```csharp
var stats = client.GetSendStatistics(reset: true);

// Access HDR histogram data
if (stats.SendStats != null)
{
    var p50 = stats.SendStats.GetValueAtPercentile(50);
    var p99 = stats.SendStats.GetValueAtPercentile(99);
    var max = stats.SendStats.GetMaxValue();
    
    Console.WriteLine($"Send latency - P50: {p50}, P99: {p99}, Max: {max}");
}

// Age statistics show how long packets waited in queue
if (stats.AgeStats != null)
{
    var avgAge = stats.AgeStats.GetMean();
    Console.WriteLine($"Average packet age: {avgAge} ticks");
}
```

### Error Handling

Subscribe to the error stream for centralized error handling:

```csharp
client.OnError.Subscribe(
    onNext: ex => 
    {
        if (ex is SocketException socketEx)
        {
            Console.WriteLine($"Network error: {socketEx.SocketErrorCode}");
        }
        else
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
    },
    onError: ex => Console.WriteLine($"Fatal error: {ex.Message}"),
    onCompleted: () => Console.WriteLine("Error stream completed")
);
```

### Memory Management

The library automatically manages memory using `MemoryPool<byte>`:

- Buffers are rented from the pool when queueing packets
- Buffers are returned to the pool after sending
- No need to manually manage buffer lifecycle
- Reduces GC pressure for high-throughput scenarios

### Performance Tuning

Key parameters you can adjust:

```csharp
// In the Client constructor:
// - packetSize: Maximum packet size in bytes (buffer allocation size)
// - Queue capacity: Default is 10,000 items (see Channel.CreateBounded in Client.cs)
// - Receive buffer: Set in InitializeReceiveSocket() (e.g., udpClient.Client.ReceiveBufferSize)
// - Pipeline buffer: Default pause threshold is 10MB (see PipeOptions in Client.cs)
```

## API Reference

### Client<TSendData, TPacketType>

Abstract base class for implementing packet-based communication.

#### Constructor

```csharp
protected Client(
    int packetSize,                      // Maximum packet size in bytes
    Func<TPacketType, Task>? channelWriter,  // Callback for received packets
    Action? channelWriterComplete        // Callback when receiver completes
)
```

#### Abstract Methods (Must Implement)

```csharp
// Initialize the receive socket/connection
protected abstract void InitializeReceiveSocket();

// Clean up the receive socket/connection
protected abstract void DisposeReceiveSocket();

// Receive raw data from the network
protected abstract ValueTask<(int ReceivedBytes, SocketReceiveMessageFromResult Result)> 
    ReceiveData(Memory<byte> memory, CancellationToken cancelToken);

// Send a packet to the network
protected abstract ValueTask<int> SendPacketAsync(
    TSendData sendData, 
    ReadOnlyMemory<byte> payload);

// Parse received bytes into your packet type
protected abstract TPacketType? TryParseObject(
    ReadOnlyMemory<byte> buffer, 
    double timestampMS, 
    IPEndPoint sourceIP, 
    IPAddress destinationIP);
```

#### Protected Methods

```csharp
// Queue a packet for sending
protected async ValueTask QueuePacket(
    int allocatePacketLength,            // Buffer size to allocate
    bool important,                      // Whether packet is important
    Func<TSendData> sendDataFactory,     // Factory to create send data
    Func<Memory<byte>, int> packetWriter // Writer that fills buffer and returns length
);

// Start receiving packets
protected void StartReceive();
```

#### Public Properties

```csharp
bool IsOperational { get; }              // Whether the client is operational
IObservable<Exception> OnError { get; }  // Error notification stream
double ReceiveClock { get; }             // Elapsed receive time in milliseconds
```

#### Public Methods

```csharp
// Get send statistics (optionally reset counters)
SendStatistics GetSendStatistics(bool reset);

// Get receive statistics
ReceiveStatistics GetReceiveStatistics();

// Dispose the client
void Dispose();
```

### SendStatistics

```csharp
public class SendStatistics
{
    public int DroppedPackets { get; set; }     // Packets dropped due to age
    public int QueueLength { get; set; }        // Current queue length
    public int FullQueue { get; set; }          // Times queue was full
    public long TotalPackets { get; set; }      // Total packets sent
    public HistogramBase? SendStats { get; set; } // Send latency histogram
    public HistogramBase? AgeStats { get; set; }  // Packet age histogram
}
```

### ReceiveStatistics

```csharp
public class ReceiveStatistics
{
    public int ObjectsInQueue1 { get; set; }    // Objects waiting in pipeline
    public int ObjectsInQueue2 { get; set; }    // Reserved for future use
}
```

### SendData

Base class for send data. Extend if you need custom metadata:

```csharp
public class SendData
{
    public IMemoryOwner<byte> Data { get; set; }  // Buffer from memory pool
    public int DataLength { get; set; }           // Actual data length
    public bool Important { get; set; }           // Importance flag
    public long AgeTicks { get; }                 // Age in ticks
    public double AgeMS { get; }                  // Age in milliseconds
}
```

## Performance Characteristics

- **Zero-copy operations** where possible using `Memory<T>` and `Span<T>`
- **Lock-free** send queue using `System.Threading.Channels`
- **Minimal allocations** through buffer pooling
- **Back-pressure support** with bounded channels
- **Automatic packet dropping** for aged packets (>200ms by default)
- **Concurrent send/receive** with dedicated tasks

Typical performance (on modern hardware):
- **Throughput**: 100,000+ packets/second
- **Latency**: Sub-millisecond for local operations
- **Memory**: Minimal GC pressure due to pooling

## Thread Safety

- The send queue is thread-safe and can be called from multiple threads
- Receive operations run on a dedicated long-running task
- Statistics can be read from any thread
- Error notifications are thread-safe via Rx observables

## Requirements

- .NET Standard 2.1, .NET 8.0, or .NET 9.0
- Dependencies:
  - HdrHistogram (for performance statistics)
  - System.IO.Pipelines (for efficient I/O)
  - System.Threading.Channels (for async queuing)
  - System.Reactive (for error notifications)

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests on [GitHub](https://github.com/HakanL/Haukcode.HighPerfComm).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Credits

Created by Hakan Lindestaf
