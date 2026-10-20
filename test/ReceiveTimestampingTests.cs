using System;
using System.Net;
using System.Net.Sockets;
using Xunit;

namespace Haukcode.HighPerfComm.Tests
{
    /// <summary>
    /// The platform receivers against a real loopback socket.
    /// </summary>
    public class ReceiveTimestampingTests
    {
        [Fact]
        public void OversizeDatagram_IsDropped_AndTheNextOneIsReceived()
        {
            using var receiveSocket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            receiveSocket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            receiveSocket.ReceiveTimeout = 2_000;

            var receiver = ReceiveTimestamping.TryCreate(receiveSocket);

            // Always available on Linux (CI); Windows needs a recent build, macOS SO_TIMESTAMP
            if (OperatingSystem.IsLinux())
                Assert.NotNull(receiver);
            if (receiver == null)
                return;

            using var sendSocket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            sendSocket.SendTo(new byte[2_000], receiveSocket.LocalEndPoint!);
            sendSocket.SendTo(new byte[] { 1, 2, 3 }, receiveSocket.LocalEndPoint!);

            var buffer = new ArraySegment<byte>(new byte[1_144]);

            // The oversize one comes back as 0 bytes (dropped), without throwing
            Assert.Equal(0, receiver!.Receive(buffer, out _, out _, out _));

            Assert.Equal(3, receiver.Receive(buffer, out var source, out _, out _));
            Assert.Equal(IPAddress.Loopback, source!.Address);
            Assert.Equal(new byte[] { 1, 2, 3 }, buffer.AsSpan(0, 3).ToArray());
        }
    }
}
