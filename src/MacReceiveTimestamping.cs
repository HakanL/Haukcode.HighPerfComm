using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;

namespace Haukcode.HighPerfComm
{
    /// <summary>
    /// macOS kernel receive timestamping (SO_TIMESTAMP): every datagram is stamped when it
    /// enters the network stack, so time spent waiting in the socket buffer no longer
    /// distorts receive timestamps. The timestamp arrives as a SCM_TIMESTAMP control message
    /// (a timeval, microsecond resolution), which .NET's Socket API does not surface, so
    /// this replaces Socket.ReceiveMessageFrom with a raw recvmsg call.
    ///
    /// Instances are not thread-safe: one per receive loop, matching the dedicated receive
    /// thread in Client. Layout assumptions: 64-bit little-endian Darwin (msghdr with 32-bit
    /// lengths, 4-byte-aligned cmsghdr entries, sockaddr_in with sin_len/sin_family bytes),
    /// which holds for every macOS target we ship on (x64 and arm64). TryCreate returns null
    /// anywhere else, so callers fall back to the portable receive path.
    /// </summary>
    public sealed unsafe class MacReceiveTimestamping : IReceiveTimestamping
    {
        private const int SOL_SOCKET = 0xffff;
        private const int SO_TIMESTAMP = 0x0400;
        private const int SCM_TIMESTAMP = 0x02;
        private const int IPPROTO_IP = 0;
        private const int IP_PKTINFO = 26;      // also IP_RECVPKTINFO; same value enables and tags
        private const byte AF_INET = 2;

        [StructLayout(LayoutKind.Sequential)]
        private struct IoVec
        {
            public IntPtr Base;
            public UIntPtr Length;
        }

        // Darwin msghdr: msg_namelen and msg_controllen are socklen_t (32-bit) and
        // msg_iovlen is int, each followed by padding on 64-bit — different from glibc
        [StructLayout(LayoutKind.Sequential)]
        private struct MsgHdr
        {
            public IntPtr Name;
            public uint NameLen;
            public IntPtr Iov;
            public int IovLen;
            public IntPtr Control;
            public uint ControlLen;
            public int Flags;
        }

        [DllImport("libc", SetLastError = true)]
        private static extern nint recvmsg(nint sockfd, MsgHdr* msg, int flags);

        [DllImport("libc", SetLastError = true)]
        private static extern int setsockopt(nint sockfd, int level, int optname, int* optval, uint optlen);

        private readonly Socket socket;
        private readonly byte[] nameBuffer = new byte[16];      // sockaddr_in
        private readonly byte[] controlBuffer = new byte[256];

        // A stream sees a handful of distinct sources and destination groups but tens of
        // thousands of packets per second; caching keeps the receive loop allocation-free.
        // Capped as a safety valve; misses past the cap just allocate like before.
        private readonly Dictionary<long, IPEndPoint> endPointCache = new();
        private readonly Dictionary<uint, IPAddress> addressCache = new();
        private const int MaxCacheEntries = 4096;

        private MacReceiveTimestamping(Socket socket)
        {
            this.socket = socket;
        }

        /// <summary>
        /// Enable kernel receive timestamps on the socket. Returns null when unavailable
        /// (not macOS, or the kernel refused the option) — callers then use their portable
        /// receive path unchanged.
        /// </summary>
        public static MacReceiveTimestamping? TryCreate(Socket socket)
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.OSX) || IntPtr.Size != 8)
                return null;

            try
            {
                int enable = 1;
                if (setsockopt(socket.Handle, SOL_SOCKET, SO_TIMESTAMP, &enable, sizeof(int)) != 0)
                    return null;

                // The kernel only attaches the destination-address control message when
                // packet info is enabled; set it through libc directly so we do not depend
                // on how the runtime maps SocketOptionName on this platform
                if (setsockopt(socket.Handle, IPPROTO_IP, IP_PKTINFO, &enable, sizeof(int)) != 0)
                    return null;

                return new MacReceiveTimestamping(socket);
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// True once at least one received packet carried a nonzero kernel timestamp.
        /// See IReceiveTimestamping.KernelTimestampsObserved.
        /// </summary>
        public bool KernelTimestampsObserved { get; private set; }

        public int Receive(ArraySegment<byte> buffer, out IPEndPoint? remoteEndPoint, out IPAddress? destinationAddress, out long kernelTimestampNS)
        {
            remoteEndPoint = null;
            destinationAddress = null;
            kernelTimestampNS = 0;

            nint received;
            int errorCode = 0;
            uint nameLen;
            int controlLen;

            fixed (byte* dataPtr = &buffer.Array![buffer.Offset])
            fixed (byte* namePtr = this.nameBuffer)
            fixed (byte* controlPtr = this.controlBuffer)
            {
                var ioVec = new IoVec
                {
                    Base = (IntPtr)dataPtr,
                    Length = (UIntPtr)buffer.Count
                };

                var msg = new MsgHdr
                {
                    Name = (IntPtr)namePtr,
                    NameLen = (uint)this.nameBuffer.Length,
                    Iov = (IntPtr)(&ioVec),
                    IovLen = 1,
                    Control = (IntPtr)controlPtr,
                    ControlLen = (uint)this.controlBuffer.Length,
                    Flags = 0
                };

                received = recvmsg(this.socket.Handle, &msg, 0);

                if (received < 0)
                {
                    errorCode = Marshal.GetLastWin32Error();
                    nameLen = 0;
                    controlLen = 0;
                }
                else
                {
                    nameLen = msg.NameLen;
                    controlLen = (int)msg.ControlLen;
                }
            }

            if (received < 0)
                throw new SocketException(errorCode);

            ParseControlMessages(controlLen, out kernelTimestampNS, out destinationAddress);
            if (kernelTimestampNS != 0)
                KernelTimestampsObserved = true;

            remoteEndPoint = ParseSourceEndPoint(nameLen);

            return (int)received;
        }

        private void ParseControlMessages(int controlLen, out long kernelTimestampNS, out IPAddress? destinationAddress)
        {
            kernelTimestampNS = 0;
            destinationAddress = null;

            var control = this.controlBuffer.AsSpan();
            int offset = 0;

            // Darwin cmsghdr: socklen_t cmsg_len (32-bit), int cmsg_level, int cmsg_type,
            // then data; entries are aligned to 4 bytes (__DARWIN_ALIGN32)
            while (offset + 12 <= controlLen)
            {
                int cmsgLen = BinaryPrimitives.ReadInt32LittleEndian(control[offset..]);
                if (cmsgLen < 12 || offset + cmsgLen > controlLen)
                    break;

                int level = BinaryPrimitives.ReadInt32LittleEndian(control[(offset + 4)..]);
                int type = BinaryPrimitives.ReadInt32LittleEndian(control[(offset + 8)..]);
                var data = control[(offset + 12)..];

                if (level == SOL_SOCKET && type == SCM_TIMESTAMP && cmsgLen >= 12 + 12)
                {
                    // struct timeval { long tv_sec; int tv_usec; } — microsecond resolution
                    long seconds = BinaryPrimitives.ReadInt64LittleEndian(data);
                    int microseconds = BinaryPrimitives.ReadInt32LittleEndian(data[8..]);
                    kernelTimestampNS = seconds * 1_000_000_000 + microseconds * 1_000L;
                }
                else if (level == IPPROTO_IP && type == IP_PKTINFO && cmsgLen >= 12 + 12)
                {
                    // struct in_pktinfo { unsigned int ipi_ifindex; in_addr ipi_spec_dst;
                    // in_addr ipi_addr; } — ipi_addr is the header destination address
                    uint networkOrderAddress = BinaryPrimitives.ReadUInt32LittleEndian(data[8..]);
                    destinationAddress = GetCachedAddress(networkOrderAddress);
                }

                offset += (cmsgLen + 3) & ~3;
            }
        }

        private IPEndPoint? ParseSourceEndPoint(uint nameLen)
        {
            if (nameLen < 8)
                return null;

            var name = this.nameBuffer.AsSpan();

            // BSD sockaddr_in: uint8 sin_len, uint8 sin_family, then port and address
            if (name[1] != AF_INET)
                return null;

            ushort port = BinaryPrimitives.ReadUInt16BigEndian(name[2..]);
            uint networkOrderAddress = BinaryPrimitives.ReadUInt32LittleEndian(name[4..]);

            long key = ((long)networkOrderAddress << 16) | port;
            if (!this.endPointCache.TryGetValue(key, out var endPoint))
            {
                endPoint = new IPEndPoint(GetCachedAddress(networkOrderAddress), port);
                if (this.endPointCache.Count < MaxCacheEntries)
                    this.endPointCache.Add(key, endPoint);
            }

            return endPoint;
        }

        private IPAddress GetCachedAddress(uint networkOrderAddress)
        {
            if (!this.addressCache.TryGetValue(networkOrderAddress, out var address))
            {
                address = new IPAddress(networkOrderAddress);
                if (this.addressCache.Count < MaxCacheEntries)
                    this.addressCache.Add(networkOrderAddress, address);
            }

            return address;
        }
    }
}
