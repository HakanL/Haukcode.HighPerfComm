using Xunit;

namespace Haukcode.HighPerfComm.Tests
{
    /// <summary>
    /// Frequency is 1e9 so 1 tick = 1 ns and millisecond values convert with a multiply.
    /// Kernel timestamps start at 1 second of CLOCK_REALTIME so they look like real stamps.
    /// </summary>
    public class KernelTimestampMapperTests
    {
        private const long Frequency = 1_000_000_000;
        private const long KernelOriginNS = 1_000_000_000;

        private static KernelTimestampMapper CreateMapper() => new KernelTimestampMapper(Frequency);

        private static long Ms(double milliseconds) => (long)(milliseconds * 1_000_000.0);

        [Fact]
        public void FirstPacket_ReturnsMonotonicTicks()
        {
            var mapper = CreateMapper();

            long output = mapper.Map(KernelOriginNS, monotonicTicks: 0).TimestampTicks;

            Assert.Equal(0, output);
            Assert.Equal(0, mapper.Steps);
        }

        [Fact]
        public void SteadyFrames_FollowKernelDeltas()
        {
            var mapper = CreateMapper();
            mapper.Map(KernelOriginNS, 0);

            long output = mapper.Map(KernelOriginNS + Ms(25), Ms(25)).TimestampTicks;

            Assert.Equal(Ms(25), output);
            Assert.Equal(0, mapper.Steps);
        }

        [Fact]
        public void QueueDelay_PreservesKernelArrivalTime()
        {
            var mapper = CreateMapper();
            mapper.Map(KernelOriginNS, 0);

            // Packet arrived 25 ms after the first but waited 400 ms in the socket buffer.
            long output = mapper.Map(KernelOriginNS + Ms(25), Ms(400)).TimestampTicks;

            Assert.Equal(Ms(25), output);
            Assert.Equal(0, mapper.Steps);
        }

        [Fact]
        public void QueueDrainBurst_IsNotAStep()
        {
            var mapper = CreateMapper();
            mapper.Map(KernelOriginNS, 0);

            // 400 ms of 25 ms frames sit in the socket buffer, then drain at ~0.1 ms each.
            double monotonicMS = 400;
            for (int i = 1; i <= 16; i++)
            {
                monotonicMS += 0.1;
                long output = mapper.Map(KernelOriginNS + Ms(25 * i), Ms(monotonicMS)).TimestampTicks;

                Assert.Equal(Ms(25 * i), output);
            }

            Assert.Equal(0, mapper.Steps);
        }

        [Fact]
        public void ReceiveGapAfterBufferOverflow_IsNotAbsorbed()
        {
            var mapper = CreateMapper();
            mapper.Map(KernelOriginNS, 0);

            // The shape logged while routing 600 universes at 60 Hz on a CM4: the loop
            // stalled, the socket buffer overflowed, and after the backlog drained the next
            // packet was 400 ms newer in kernel time with no monotonic time elapsed. That is
            // lost traffic, not a clock step — absorbing it would erase the hole from the
            // recording and hide it from the stream-gap detector downstream.
            var result = mapper.Map(KernelOriginNS + Ms(400), Ms(0.05));

            Assert.False(result.Stepped);
            Assert.Equal(0, mapper.Steps);
            Assert.Equal(Ms(400), result.TimestampTicks);
        }

        [Fact]
        public void ForwardNtpStep_DoesNotJumpOutput()
        {
            var mapper = CreateMapper();
            mapper.Map(KernelOriginNS, 0);

            // 25 ms of real time, 5.025 s of CLOCK_REALTIME (5 s NTP step forward).
            var result = mapper.Map(KernelOriginNS + Ms(5025), Ms(25));

            Assert.Equal(Ms(25), result.TimestampTicks);
            Assert.True(result.Stepped);
            Assert.True(result.Forward);
            Assert.Equal(1, mapper.Steps);
        }

        [Fact]
        public void AfterForwardStep_KernelPrecisionResumes()
        {
            var mapper = CreateMapper();
            mapper.Map(KernelOriginNS, 0);
            mapper.Map(KernelOriginNS + Ms(5025), Ms(25));

            long output = mapper.Map(KernelOriginNS + Ms(5050), Ms(50)).TimestampTicks;

            Assert.Equal(Ms(50), output);
            Assert.Equal(1, mapper.Steps);
        }

        [Fact]
        public void BackwardNtpStep_DoesNotReverseOutput()
        {
            var mapper = CreateMapper();
            mapper.Map(KernelOriginNS, 0);
            mapper.Map(KernelOriginNS + Ms(25), Ms(25));

            // Clock stepped back 2 s; kernel stamp is earlier than the previous packet.
            var result = mapper.Map(KernelOriginNS + Ms(25) - Ms(2000) + Ms(25), Ms(50));

            Assert.Equal(Ms(50), result.TimestampTicks);
            Assert.True(result.Stepped);
            Assert.False(result.Forward);
            Assert.Equal(1, mapper.Steps);
        }

        [Fact]
        public void SmallBackwardStep_IsStillAbsorbed()
        {
            var mapper = CreateMapper();
            mapper.Map(KernelOriginNS, 0);
            mapper.Map(KernelOriginNS + Ms(25), Ms(25));

            // 100 ms step back with a 25 ms frame: kernel went backwards by 75 ms, under
            // the reorder tolerance, but the mapped tick reverses far enough to re-anchor.
            long output = mapper.Map(KernelOriginNS + Ms(25) - Ms(100) + Ms(25), Ms(50)).TimestampTicks;

            Assert.Equal(Ms(50), output);
            Assert.Equal(1, mapper.Steps);
        }

        [Fact]
        public void ReorderedPacket_IsClampedAndNotAStep()
        {
            var mapper = CreateMapper();
            mapper.Map(KernelOriginNS, 0);
            mapper.Map(KernelOriginNS + Ms(25), Ms(25));

            // Two packets stamped 0.2 ms apart on different receive queues, dequeued in the
            // other order. This is not a clock step and must not be reported as one.
            var result = mapper.Map(KernelOriginNS + Ms(24.8), Ms(25.1));

            Assert.Equal(Ms(25), result.TimestampTicks);
            Assert.False(result.Stepped);
            Assert.Equal(0, mapper.Steps);
            Assert.Equal(1, mapper.Reorders);
        }

        [Fact]
        public void AfterReorder_KernelPrecisionIsPreserved()
        {
            var mapper = CreateMapper();
            mapper.Map(KernelOriginNS, 0);
            mapper.Map(KernelOriginNS + Ms(25), Ms(25));
            mapper.Map(KernelOriginNS + Ms(24.8), Ms(25.1));

            // The anchor was left alone, so the next in-order packet still maps by its
            // kernel delta rather than by however long the receive loop took.
            long output = mapper.Map(KernelOriginNS + Ms(50), Ms(70)).TimestampTicks;

            Assert.Equal(Ms(50), output);
            Assert.Equal(0, mapper.Steps);
        }

        [Fact]
        public void RepeatedIsolatedReorders_NeverAccumulateIntoAStep()
        {
            var mapper = CreateMapper();
            long kernelNS = KernelOriginNS;
            long monotonicNS = 0;
            mapper.Map(kernelNS, monotonicNS);

            // Two receive queues swapping adjacent packets over and over: each hold is one
            // packet long, so the run counter must reset and never reach the step path.
            for (int i = 0; i < 500; i++)
            {
                kernelNS += Ms(0.05);
                monotonicNS += Ms(0.05);
                mapper.Map(kernelNS + Ms(0.02), monotonicNS);
                mapper.Map(kernelNS, monotonicNS + Ms(0.01));
            }

            Assert.Equal(0, mapper.Steps);
            Assert.Equal(500, mapper.Reorders);
        }

        [Fact]
        public void SubToleranceBackwardStep_HoldsTheTimelineBriefly_ThenResumes()
        {
            var mapper = CreateMapper();

            // 600 universes at 60 Hz: 36k packets/s, one every ~28 us. A CLOCK_REALTIME
            // step back of 8 ms is under the reorder tolerance, so it is clamped rather
            // than re-anchored — the timeline must not stay pinned.
            const double PacketIntervalMS = 1000.0 / 36000;
            long kernelNS = KernelOriginNS;
            long monotonicNS = 0;
            mapper.Map(kernelNS, monotonicNS);

            for (int i = 0; i < 100; i++)
            {
                kernelNS += Ms(PacketIntervalMS);
                monotonicNS += Ms(PacketIntervalMS);
                mapper.Map(kernelNS, monotonicNS);
            }

            long outputBeforeStep = mapper.Map(kernelNS, monotonicNS).TimestampTicks;
            kernelNS -= Ms(8);

            int clamped = 0;
            long lastOutput = outputBeforeStep;
            for (int i = 0; i < 2000; i++)
            {
                kernelNS += Ms(PacketIntervalMS);
                monotonicNS += Ms(PacketIntervalMS);
                long output = mapper.Map(kernelNS, monotonicNS).TimestampTicks;

                Assert.True(output >= lastOutput, "the output timeline must never go backwards");

                if (output == lastOutput)
                    clamped++;

                lastOutput = output;
            }

            // The hold must not last until the clock catches up — that would tie ~290
            // frames to one timestamp. It is reclassified as the step it is after a
            // handful of packets, and the recording keeps moving.
            Assert.InRange(clamped, 1, KernelTimestampMapper.MaxConsecutiveReorders);
            Assert.Equal(1, mapper.Steps);
            Assert.True(lastOutput > outputBeforeStep + Ms(40), "timeline resumed after the hold");
        }

        [Fact]
        public void JitterUnderThreshold_IsNotAStep()
        {
            var mapper = CreateMapper();
            mapper.Map(KernelOriginNS, 0);

            var result = mapper.Map(KernelOriginNS + Ms(50), Ms(25));

            Assert.Equal(Ms(50), result.TimestampTicks);
            Assert.False(result.Stepped);
            Assert.Equal(0, mapper.Steps);
        }

        [Fact]
        public void Reset_ClearsAnchorAndSteps()
        {
            var mapper = CreateMapper();
            mapper.Map(KernelOriginNS, 0);
            mapper.Map(KernelOriginNS + Ms(5025), Ms(25));
            Assert.Equal(1, mapper.Steps);

            mapper.Reset();

            long output = mapper.Map(KernelOriginNS + Ms(9000), Ms(100)).TimestampTicks;

            Assert.Equal(Ms(100), output);
            Assert.Equal(0, mapper.Steps);
            Assert.Equal(0, mapper.Reorders);
        }
    }
}
