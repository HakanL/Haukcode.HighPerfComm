using System.Buffers;
using System.Diagnostics;
using System.Threading;

namespace Haukcode.HighPerfComm
{
    public class SendData
    {
        private readonly Stopwatch ageStopwatch = new Stopwatch();

        public IMemoryOwner<byte> Data { get; set; } = null!;

        public int DataLength { get; set; }

        public bool Important { get; set; }

        /// <summary>
        /// Set on the marker items an ordering barrier pushes onto every other shard. Carries no
        /// payload: the sender signals it on dequeue and transmits nothing. Reaching the marker is
        /// proof that shard has drained everything queued before the barrier.
        /// </summary>
        public CountdownEvent? BarrierSignal { get; set; }

        /// <summary>
        /// Set on a barrier's real packet. Its sender blocks until every other shard has reached
        /// its marker, then transmits — so the packet cannot overtake anything queued before it on
        /// another shard. Used for E1.31 sync and ArtSync, which must follow the DMX they
        /// synchronize.
        /// </summary>
        public CountdownEvent? BarrierWait { get; set; }

        public long AgeTicks => this.ageStopwatch.ElapsedTicks;

        public double AgeMS => this.ageStopwatch.Elapsed.TotalMilliseconds;

        public void StartAgeStopwatch()
        {
            this.ageStopwatch.Restart();
        }
    }
}
