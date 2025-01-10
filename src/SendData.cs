using System.Buffers;
using System.Diagnostics;

namespace Haukcode.HighPerfComm
{
    public class SendData
    {
        private readonly Stopwatch ageStopwatch = new Stopwatch();

        public IMemoryOwner<byte> Data { get; set; } = null!;

        public int DataLength { get; set; }

        public bool Important { get; set; }

        public long AgeTicks => this.ageStopwatch.ElapsedTicks;

        public double AgeMS => this.ageStopwatch.Elapsed.TotalMilliseconds;

        public void StartAgeStopwatch()
        {
            this.ageStopwatch.Restart();
        }
    }
}
