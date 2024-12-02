using System.Buffers;
using System.Diagnostics;

namespace Haukcode.HighPerfComm;

public class SendData
{
    public IMemoryOwner<byte> Data { get; set; } = null!;

    public int DataLength { get; set; }

    public Stopwatch Enqueued { get; set; }

    public bool Important { get; set; }

    public double AgeMS => Enqueued.Elapsed.TotalMilliseconds;

    public SendData()
    {
        Enqueued = Stopwatch.StartNew();
    }
}
