# Haukcode.HighPerfComm

High-performance .NET library for packet-based network communication with built-in memory pooling, pipelining, and performance monitoring.

## Features

✅ High-performance async packet processing  
✅ Memory-efficient with buffer pooling  
✅ Built-in HDR histogram statistics  
✅ Automatic packet age management  
✅ Back-pressure handling  
✅ Supports .NET Standard 2.1, .NET 8.0, .NET 9.0

## Quick Start

```csharp
using Haukcode.HighPerfComm;

// 1. Extend the Client base class
public class MyUdpClient : Client<MySendData, MyPacket>
{
    public MyUdpClient(int packetSize, Func<MyPacket, Task> handler) 
        : base(packetSize, handler, null) { }
    
    // Implement abstract methods for your protocol
    protected override void InitializeReceiveSocket() { /* ... */ }
    protected override void DisposeReceiveSocket() { /* ... */ }
    protected override ValueTask<(int, SocketReceiveMessageFromResult)> 
        ReceiveData(Memory<byte> memory, CancellationToken ct) { /* ... */ }
    protected override ValueTask<int> SendPacketAsync(
        MySendData data, ReadOnlyMemory<byte> payload) { /* ... */ }
    protected override MyPacket? TryParseObject(
        ReadOnlyMemory<byte> buffer, double timestamp, 
        IPEndPoint source, IPAddress dest) { /* ... */ }
}

// 2. Use your client
using var client = new MyUdpClient(1500, async packet => 
{
    Console.WriteLine($"Received: {packet}");
});

// 3. Send packets
await client.QueuePacket(
    allocatePacketLength: dataLength,
    important: false,
    sendDataFactory: () => new MySendData(),
    packetWriter: buffer => { /* fill buffer */ return length; }
);

// 4. Monitor performance
var stats = client.GetSendStatistics(reset: false);
Console.WriteLine($"Total: {stats.TotalPackets}, Dropped: {stats.DroppedPackets}");
```

## Key Benefits

- **Zero-copy operations** using `Memory<T>` and `Span<T>`
- **Lock-free send queue** via `System.Threading.Channels`
- **Minimal GC pressure** through `MemoryPool<byte>`
- **Automatic stale packet dropping** (>200ms age threshold)
- **100,000+ packets/second** throughput on modern hardware

## Documentation

For comprehensive documentation, examples, and API reference, visit:  
👉 **[GitHub Repository](https://github.com/HakanL/Haukcode.HighPerfComm)**

## License

MIT License - see [LICENSE](https://github.com/HakanL/Haukcode.HighPerfComm/blob/main/LICENSE)
