using System.Diagnostics;

namespace Haukcode.HighPerfComm
{
    /// <summary>
    /// Maps kernel receive timestamps (CLOCK_REALTIME on Linux/macOS, so NTP can step them)
    /// onto a monotonic Stopwatch timeline. Within a segment the kernel deltas are used
    /// verbatim, which keeps the "packet that waited 400 ms still carries its true arrival
    /// time" property. When the kernel clock steps, the mapper re-anchors and continues the
    /// output timeline by however much Stopwatch advanced, so a recording does not inherit
    /// the discontinuity.
    ///
    /// Detection is deliberately asymmetric:
    /// <list type="bullet">
    /// <item>
    /// Forward step: the kernel advanced more than Stopwatch by <see cref="StepThresholdNS"/>
    /// since the previous packet. Queue delay cannot produce this (it makes Stopwatch run
    /// ahead of the kernel, not behind). A burst drain also cannot: each queued packet's
    /// kernel delta is one frame period, well under the threshold.
    /// </item>
    /// <item>
    /// Backward step: the mapped tick would go backwards. Queue delay never does that
    /// because kernel arrival times still increase; an NTP step-back does.
    /// </item>
    /// </list>
    /// NTP slew (adjtime) is not a step and is left alone: per-packet divergence stays
    /// far below the threshold. Not thread-safe; one instance per receive loop.
    /// </summary>
    internal sealed class KernelTimestampMapper
    {
        /// <summary>
        /// Divergence larger than this between a kernel delta and the matching Stopwatch
        /// delta is treated as a clock step. 250 ms is several DMX frame periods (16.7–25 ms)
        /// and well above typical scheduling jitter, but far below a Pi's first NTP correction
        /// (often seconds to hours, with no battery-backed RTC).
        /// </summary>
        public const long StepThresholdNS = 250_000_000;

        private readonly double ticksPerNanosecond;
        private readonly double nanosecondsPerTick;
        private long baseNS;
        private long baseTicks;
        private long lastKernelNS;
        private long lastOutputTicks;
        private long lastMonotonicTicks;
        private bool anchored;
        private int steps;

        /// <param name="stopwatchFrequency">
        /// Ticks per second of the monotonic clock. 0 (the default) uses
        /// <see cref="Stopwatch.Frequency"/>; tests pass a round number so the math is exact.
        /// </param>
        public KernelTimestampMapper(long stopwatchFrequency = 0)
        {
            long frequency = stopwatchFrequency > 0 ? stopwatchFrequency : Stopwatch.Frequency;
            this.ticksPerNanosecond = frequency / 1_000_000_000.0;
            this.nanosecondsPerTick = 1_000_000_000.0 / frequency;
        }

        /// <summary>
        /// Number of kernel-clock steps absorbed since the last <see cref="Reset"/>.
        /// </summary>
        public int Steps => this.steps;

        public void Reset()
        {
            this.anchored = false;
            this.baseNS = 0;
            this.baseTicks = 0;
            this.lastKernelNS = 0;
            this.lastOutputTicks = 0;
            this.lastMonotonicTicks = 0;
            this.steps = 0;
        }

        /// <summary>
        /// Convert a kernel arrival timestamp to Stopwatch ticks on the monotonic timeline.
        /// <paramref name="monotonicTicks"/> is <c>receiveClock.ElapsedTicks</c> at the moment
        /// user space dequeued the packet (processing time, not arrival).
        /// </summary>
        public KernelTimestampMapResult Map(long kernelNS, long monotonicTicks)
        {
            if (!this.anchored)
            {
                this.baseNS = kernelNS;
                this.baseTicks = monotonicTicks;
                this.lastKernelNS = kernelNS;
                this.lastOutputTicks = monotonicTicks;
                this.lastMonotonicTicks = monotonicTicks;
                this.anchored = true;

                return new KernelTimestampMapResult(monotonicTicks);
            }

            long mappedTicks = this.baseTicks + (long)((kernelNS - this.baseNS) * this.ticksPerNanosecond);

            long kernelDeltaNS = kernelNS - this.lastKernelNS;
            long monotonicDeltaNS = (long)((monotonicTicks - this.lastMonotonicTicks) * this.nanosecondsPerTick);

            bool forwardStep = kernelDeltaNS - monotonicDeltaNS > StepThresholdNS;
            bool backwardStep = mappedTicks < this.lastOutputTicks;

            if (forwardStep || backwardStep)
            {
                this.steps++;

                // Keep the output timeline continuous on the monotonic side: advance by
                // however much Stopwatch moved since the previous packet, drop the kernel jump.
                long outputTicks = this.lastOutputTicks + (monotonicTicks - this.lastMonotonicTicks);

                this.baseNS = kernelNS;
                this.baseTicks = outputTicks;
                this.lastKernelNS = kernelNS;
                this.lastOutputTicks = outputTicks;
                this.lastMonotonicTicks = monotonicTicks;

                return new KernelTimestampMapResult(outputTicks, stepped: true, forward: forwardStep,
                    kernelDeltaNS, monotonicDeltaNS);
            }

            this.lastKernelNS = kernelNS;
            this.lastOutputTicks = mappedTicks;
            this.lastMonotonicTicks = monotonicTicks;

            return new KernelTimestampMapResult(mappedTicks);
        }
    }

    internal readonly struct KernelTimestampMapResult
    {
        public KernelTimestampMapResult(long timestampTicks, bool stepped = false, bool forward = false,
            long kernelDeltaNS = 0, long monotonicDeltaNS = 0)
        {
            TimestampTicks = timestampTicks;
            Stepped = stepped;
            Forward = forward;
            KernelDeltaNS = kernelDeltaNS;
            MonotonicDeltaNS = monotonicDeltaNS;
        }

        public long TimestampTicks { get; }

        public bool Stepped { get; }

        public bool Forward { get; }

        public long KernelDeltaNS { get; }

        public long MonotonicDeltaNS { get; }
    }
}
