namespace Haukcode.HighPerfComm
{
    /// <summary>
    /// A kernel CLOCK_REALTIME step that was absorbed so the receive timeline stays monotonic.
    /// Raised from the receive thread; handlers must be cheap and non-blocking.
    /// </summary>
    public sealed class KernelClockStep
    {
        public KernelClockStep(bool forward, long kernelDeltaNS, long monotonicDeltaNS, int absorbedCount)
        {
            Forward = forward;
            KernelDeltaMS = kernelDeltaNS / 1_000_000.0;
            MonotonicDeltaMS = monotonicDeltaNS / 1_000_000.0;
            AbsorbedCount = absorbedCount;
        }

        /// <summary>
        /// True when the kernel clock jumped ahead of Stopwatch; false when it went backwards.
        /// </summary>
        public bool Forward { get; }

        public double KernelDeltaMS { get; }

        public double MonotonicDeltaMS { get; }

        /// <summary>
        /// Number of steps absorbed since the last receive start, including this one.
        /// </summary>
        public int AbsorbedCount { get; }
    }
}
