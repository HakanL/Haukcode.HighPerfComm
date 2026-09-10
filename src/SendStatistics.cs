using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using HdrHistogram;

namespace Haukcode.HighPerfComm
{
    public class SendStatistics
    {
        public int DroppedPackets { get; set; }

        public int QueueLength { get; set; }

        public int FullQueue { get; set; }

        public long TotalPackets { get; set; }

        public HistogramBase? SendStats { get; set; }

        public HistogramBase? AgeStats { get; set; }

        /// <summary>
        /// Sum of the send durations (Stopwatch ticks) of the packets <see cref="SendStats"/>
        /// covers, over the same interval: the total time the sender threads spent in the
        /// socket. Equal to summing every bucket of the histogram, without the walk.
        /// </summary>
        public long TotalSendTicks { get; set; }
    }
}
