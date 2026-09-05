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
        public void ForwardNtpStep_DoesNotJumpOutput()
        {
            var mapper = CreateMapper();
            mapper.Map(KernelOriginNS, 0);

            // 25 ms of real time, 2.025 s of CLOCK_REALTIME (2 s NTP step forward).
            var result = mapper.Map(KernelOriginNS + Ms(2025), Ms(25));

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
            mapper.Map(KernelOriginNS + Ms(2025), Ms(25));

            long output = mapper.Map(KernelOriginNS + Ms(2050), Ms(50)).TimestampTicks;

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
            // StepThresholdNS, but the mapped tick would reverse so it must still re-anchor.
            long output = mapper.Map(KernelOriginNS + Ms(25) - Ms(100) + Ms(25), Ms(50)).TimestampTicks;

            Assert.Equal(Ms(50), output);
            Assert.Equal(1, mapper.Steps);
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
            mapper.Map(KernelOriginNS + Ms(2025), Ms(25));
            Assert.Equal(1, mapper.Steps);

            mapper.Reset();

            long output = mapper.Map(KernelOriginNS + Ms(5000), Ms(100)).TimestampTicks;

            Assert.Equal(Ms(100), output);
            Assert.Equal(0, mapper.Steps);
        }
    }
}
