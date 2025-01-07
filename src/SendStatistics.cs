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
    }
}
