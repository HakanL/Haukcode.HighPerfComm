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
    /// Forward step: the kernel advanced more than Stopwatch by <see cref="ForwardStepThresholdNS"/>
    /// since the previous packet. Queue delay cannot produce this (it makes Stopwatch run
    /// ahead of the kernel, not behind). A burst drain *can*, though — not per packet, but
    /// once at the end, when the socket buffer overflowed and the replayed backlog runs out:
    /// that jump is real traffic loss and must reach the caller, which is why the threshold
    /// sits above any gap a stalled loop can open rather than just above frame jitter.
    /// </item>
    /// <item>
    /// Backward step: the mapped tick would go backwards by more than
    /// <see cref="ReorderToleranceNS"/>, or by less than that for longer than a reorder can
    /// last (see <see cref="MaxConsecutiveReorders"/>). Queue delay never moves the mapped
    /// tick backwards because kernel arrival times still increase; an NTP step-back does.
    /// </item>
    /// </list>
    /// A backward move *within* the tolerance is packet reordering, not a clock step: the
    /// stamp is applied when the driver hands the datagram to the stack, and with multiple
    /// receive queues (RSS) or several adapters feeding one socket, two packets stamped
    /// microseconds apart can be dequeued in the other order. Those are clamped to the
    /// previous output tick — the timeline stays monotonic — and counted as
    /// <see cref="Reorders"/> rather than reported as steps. Windows makes this obvious:
    /// its kernel stamps are QPC, the same monotonic clock Stopwatch reads, so a genuine
    /// backward step cannot happen there at all.
    ///
    /// NTP slew (adjtime) is not a step and is left alone: per-packet divergence stays
    /// far below the threshold. Not thread-safe; one instance per receive loop.
    /// </summary>
    internal sealed class KernelTimestampMapper
    {
        /// <summary>
        /// Divergence larger than this between a kernel delta and the matching Stopwatch
        /// delta is treated as a forward clock step.
        ///
        /// This has to clear the largest gap a loaded receive loop can produce, not just
        /// scheduling jitter. When the loop stalls long enough to overflow the socket
        /// buffer, the drain replays the buffered (old) packets first, so the jump lands at
        /// the END of the burst and measures stall duration minus buffer depth — hundreds of
        /// milliseconds under load. An earlier 250 ms threshold swallowed those: routing 600
        /// universes at 60 Hz on a CM4 logged 86 "forward step absorbed" warnings in 15
        /// minutes (and none at 40 Hz, which no clock event would explain). Absorbing them is
        /// actively harmful — it erases the gap from the timeline, so a recording looks
        /// continuous across traffic that was actually lost and the stream-gap detector
        /// downstream never sees it.
        ///
        /// 2 s stays far below a Pi's first NTP correction (seconds to hours, with no
        /// battery-backed RTC) while letting load-induced gaps through to be reported.
        /// </summary>
        public const long ForwardStepThresholdNS = 2_000_000_000;

        /// <summary>
        /// A mapped tick that lands this far behind the previous one or less is treated as
        /// packet reordering and clamped, not as a clock step. Measured reordering between
        /// receive queues is sub-millisecond; 10 ms leaves an order of magnitude of headroom
        /// while still being far below any NTP step-back worth absorbing (and a step smaller
        /// than this is absorbed by the clamp anyway — only the bookkeeping differs).
        /// </summary>
        public const long ReorderToleranceNS = 10_000_000;

        /// <summary>
        /// Packet-count bound on a hold. Deliberately generous: a run of reordered packets
        /// is not one or two, it is however many the NIC indicates in a batch. With RSS the
        /// destination groups of a large sACN stream hash across receive queues, each queue
        /// is drained by its own DPC on its own core, and interrupt moderation makes those
        /// batches big — a measured Windows box (Intel I210, 2 RSS queues, *SoftwareTimestamp
        /// RxAll) produced runs well past 8, which is what made an earlier cap of that size
        /// report thousands of false steps.
        /// </summary>
        public const int MaxConsecutiveReorders = 32;

        /// <summary>
        /// Wall-clock bound on the same hold. Queue skew resolves in well under a
        /// millisecond (measured: 99 % of holds under 0.5 ms, none over 2 ms that were not
        /// genuine), while a real sub-tolerance step back holds until the clock catches up.
        /// </summary>
        public const long ReorderHoldLimitNS = 2_000_000;

        private readonly double ticksPerNanosecond;
        private readonly double nanosecondsPerTick;
        private readonly long reorderToleranceTicks;
        private readonly long reorderHoldLimitTicks;
        private long baseNS;
        private long baseTicks;
        private long lastKernelNS;
        private long lastOutputTicks;
        private long lastMonotonicTicks;
        private bool anchored;
        private int steps;
        private int reorders;
        private int consecutiveReorders;

        /// <param name="stopwatchFrequency">
        /// Ticks per second of the monotonic clock. 0 (the default) uses
        /// <see cref="Stopwatch.Frequency"/>; tests pass a round number so the math is exact.
        /// </param>
        public KernelTimestampMapper(long stopwatchFrequency = 0)
        {
            long frequency = stopwatchFrequency > 0 ? stopwatchFrequency : Stopwatch.Frequency;
            this.ticksPerNanosecond = frequency / 1_000_000_000.0;
            this.nanosecondsPerTick = 1_000_000_000.0 / frequency;
            this.reorderToleranceTicks = (long)(ReorderToleranceNS * this.ticksPerNanosecond);
            this.reorderHoldLimitTicks = (long)(ReorderHoldLimitNS * this.ticksPerNanosecond);
        }

        /// <summary>
        /// Number of kernel-clock steps absorbed since the last <see cref="Reset"/>.
        /// </summary>
        public int Steps => this.steps;

        /// <summary>
        /// Number of out-of-order kernel timestamps clamped since the last <see cref="Reset"/>.
        /// Normal on a busy multi-queue NIC; only interesting as a rate.
        /// </summary>
        public int Reorders => this.reorders;

        public void Reset()
        {
            this.anchored = false;
            this.baseNS = 0;
            this.baseTicks = 0;
            this.lastKernelNS = 0;
            this.lastOutputTicks = 0;
            this.lastMonotonicTicks = 0;
            this.steps = 0;
            this.reorders = 0;
            this.consecutiveReorders = 0;
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

            long backwardTicks = this.lastOutputTicks - mappedTicks;

            bool forwardStep = kernelDeltaNS - monotonicDeltaNS > ForwardStepThresholdNS;
            bool backwardStep = backwardTicks > this.reorderToleranceTicks;

            if (!forwardStep && !backwardStep && backwardTicks > 0)
            {
                // Out-of-order arrival stamps, not a clock step. Hold the timeline where it
                // is and leave the anchor alone so the next in-order packet maps normally;
                // re-anchoring here would throw away kernel precision on every reorder.
                //
                // Magnitude alone cannot tell the two apart, so persistence decides: queue
                // skew resolves as soon as the lagging queue is drained, while a genuine
                // sub-tolerance clock step leaves EVERY later packet behind. Holding through
                // one of those would tie the whole recording to one timestamp until the clock
                // caught up — at 36k packets/s an 8 ms step-back is ~290 tied frames.
                //
                // Both bounds must be exceeded, because each one alone misreads a different
                // stream. A fast stream reorders in bulk but briefly, so the count says step
                // and the clock says no. A slow stream reorders one packet across a whole
                // frame period, so the clock says step and the count says no — and there a
                // real step never opens a hold at all, since one frame gap already outruns
                // it. Only a hold that is both long and deep is the real thing.
                this.consecutiveReorders++;

                if (this.consecutiveReorders <= MaxConsecutiveReorders ||
                    monotonicTicks - this.lastMonotonicTicks <= this.reorderHoldLimitTicks)
                {
                    this.reorders++;

                    return new KernelTimestampMapResult(this.lastOutputTicks);
                }

                backwardStep = true;
            }

            this.consecutiveReorders = 0;

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
