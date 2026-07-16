using System;
using System.Net;
using System.Net.Sockets;

namespace Haukcode.HighPerfComm
{
    /// <summary>
    /// A receive path that returns the kernel's arrival timestamp with every datagram, so
    /// time spent waiting in the socket buffer (process descheduled, GC pause, busy receive
    /// loop) does not distort receive timestamps. Implementations exist per platform; use
    /// <see cref="ReceiveTimestamping.TryCreate"/> to get the right one for the current OS.
    /// </summary>
    public interface IReceiveTimestamping
    {
        /// <summary>
        /// Blocking receive. Returns the received byte count; kernelTimestampNS is the
        /// arrival time in nanoseconds on a platform-specific clock (CLOCK_REALTIME on
        /// Linux, QPC on Windows, the wall clock on macOS), or 0 when the kernel did not
        /// attach one. Only deltas between timestamps from the same instance are
        /// meaningful. Throws SocketException on socket errors, matching
        /// Socket.ReceiveMessageFrom.
        /// </summary>
        int Receive(ArraySegment<byte> buffer, out IPEndPoint? remoteEndPoint, out IPAddress? destinationAddress, out long kernelTimestampNS);

        /// <summary>
        /// True once at least one received packet actually carried a kernel timestamp.
        /// On Linux and macOS a successful setsockopt means stamps flow, so this turns
        /// true on the first packet. On Windows the socket option alone is not enough —
        /// the adapter's miniport driver must have software timestamping enabled (the
        /// *SoftwareTimestamp INF keyword, off by default on most drivers) or every
        /// SO_TIMESTAMP control message carries zero; this property is the reliable,
        /// after-the-fact signal of which case applies (the driver capability APIs are
        /// not trustworthy: common NICs return ERROR_BAD_DRIVER in both states).
        /// </summary>
        bool KernelTimestampsObserved { get; }
    }

    public static class ReceiveTimestamping
    {
        /// <summary>
        /// Enable kernel receive timestamps on the socket using the current platform's
        /// mechanism (Linux SO_TIMESTAMPNS, Windows SIO_TIMESTAMPING, macOS SO_TIMESTAMP).
        /// Returns null when the platform or kernel does not support it — callers then use
        /// their portable receive path with user-space timestamps unchanged.
        /// </summary>
        public static IReceiveTimestamping? TryCreate(Socket socket)
        {
            return LinuxReceiveTimestamping.TryCreate(socket)
                ?? WindowsReceiveTimestamping.TryCreate(socket)
                ?? (IReceiveTimestamping?)MacReceiveTimestamping.TryCreate(socket);
        }
    }
}
