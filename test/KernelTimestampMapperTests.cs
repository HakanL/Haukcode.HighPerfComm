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
        public void RssQueueBatch_IsClampedWholeAndNeverReportedAsAStep()
        {
            var mapper = CreateMapper();

            // The measured Windows shape: an Intel I210 with 2 RSS queues and software
            // timestamping, where a large sACN stream's destination groups hash across both
            // queues. One queue's moderated batch is indicated after the other's, so a long
            // run of packets carries stamps behind the high-water mark. Measured at 600
            // universes / 60 Hz: holds of 2.0-2.8 ms, stamps 0.1-1.8 ms behind, runs far
            // longer than 40. Not one of these may be reported as a clock step.
            long kernelNS = KernelOriginNS;
            mapper.Map(kernelNS, 0);
            mapper.Map(kernelNS + Ms(1.8), Ms(0.01));

            for (int i = 0; i < 120; i++)
            {
                mapper.Map(kernelNS + Ms(0.1 + i * 0.014), Ms(0.02 + i * 0.023));
            }

            Assert.Equal(0, mapper.Steps);
            Assert.Equal(120, mapper.Reorders);
        }

        [Fact]
        public void SlowStreamReorder_OutlivingTheTolerance_IsReclassified()
        {
            var mapper = CreateMapper();

            // A handful of universes at 40 Hz. An isolated swap holds across a whole 25 ms
            // frame, which outlives the tolerance, so this one IS reclassified — and that is
            // correct: on a stream this slow the swap is indistinguishable from the clock
            // having moved, and re-anchoring costs nothing because the next packet maps by
            // its own kernel delta either way.
            long kernelNS = KernelOriginNS;
            mapper.Map(kernelNS, 0);
            mapper.Map(kernelNS + Ms(25), Ms(25));
            var result = mapper.Map(kernelNS + Ms(24.9), Ms(50));

            Assert.True(result.Stepped);
            Assert.Equal(1, mapper.Steps);
            Assert.Equal(Ms(50), result.TimestampTicks);
        }

        [Fact]
        public void SubToleranceBackwardStep_IsAbsorbedByTheClampWithoutAStep()
        {
            var mapper = CreateMapper();

            // 600 universes at 60 Hz: 36k packets/s, one every ~28 us. A CLOCK_REALTIME
            // step back of 8 ms is under the reorder tolerance, and the clamp simply
            // absorbs it: the kernel clock undoes the deficit within its own size, so the
            // hold ends on its own without ever being called a step. The tie is bounded by
            // the tolerance — under 10 ms of stream time, less than one frame period at
            // 40-60 Hz — so no universe loses more than about a single frame's timestamp.
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

            Assert.InRange(clamped, 250, 320);
            Assert.Equal(0, mapper.Steps);
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
