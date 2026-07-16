using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;

namespace Haukcode.HighPerfComm
{
    /// <summary>
    /// Linux kernel receive timestamping (SO_TIMESTAMPNS): every datagram is stamped when it
    /// enters the network stack from the driver, so time spent waiting in the socket buffer
    /// (because the process was descheduled, paused by the GC, or simply behind) no longer
    /// distorts receive timestamps — a packet that waited 400 ms still carries its true
    /// arrival time. Replaces Socket.ReceiveMessageFrom with a raw recvmsg call because the
    /// timestamp arrives as a control message and .NET's Socket API does not surface those.
    ///
    /// Instances are not thread-safe: one per receive loop, matching the dedicated receive
    /// thread in Client. Layout assumptions: little-endian 64-bit Linux with a glibc msghdr
    /// (size_t msg_iovlen), which holds for every Linux target we ship on (Debian/balena
    /// arm64 and x64). TryCreate returns null anywhere else, so callers fall back to the
    /// portable receive path with user-space timestamps.
    /// </summary>
    public sealed unsafe class LinuxReceiveTimestamping : IReceiveTimestamping
    {
        private const int SOL_SOCKET = 1;
        private const int SO_TIMESTAMPNS = 35;      // SO_TIMESTAMPNS_OLD: 64-bit time_t timespec on arm64/x64
        private const int IPPROTO_IP = 0;
        private const int IP_PKTINFO = 8;
        private const ushort AF_INET = 2;

        [StructLayout(LayoutKind.Sequential)]
        private struct IoVec
        {
            public IntPtr Base;
            public UIntPtr Length;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct MsgHdr
        {
            public IntPtr Name;
            public uint NameLen;
            public IntPtr Iov;
            public UIntPtr IovLen;
            public IntPtr Control;
            public UIntPtr ControlLen;
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

        private LinuxReceiveTimestamping(Socket socket)
        {
            this.socket = socket;
        }

        /// <summary>
        /// Enable kernel receive timestamps on the socket. Returns null when unavailable
        /// (not Linux, or the kernel refused the option) — callers then use their portable
        /// receive path unchanged.
        /// </summary>
        public static LinuxReceiveTimestamping? TryCreate(Socket socket)
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                return null;

            try
            {
                int enable = 1;
                if (setsockopt(socket.Handle, SOL_SOCKET, SO_TIMESTAMPNS, &enable, sizeof(int)) != 0)
                    return null;

                // The kernel only attaches the destination-address control message when packet
                // info is enabled (ReceiveMessageFrom normally turns this on lazily)
                socket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.PacketInformation, true);

                return new LinuxReceiveTimestamping(socket);
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

        /// <summary>
        /// Blocking receive. Returns the received byte count; kernelTimestampNS is the
        /// CLOCK_REALTIME arrival time in nanoseconds, or 0 when the kernel did not attach one.
        /// Throws SocketException on socket errors, matching Socket.ReceiveMessageFrom.
        /// </summary>
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
                    IovLen = (UIntPtr)1,
                    Control = (IntPtr)controlPtr,
                    ControlLen = (UIntPtr)this.controlBuffer.Length,
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

            // cmsghdr on 64-bit: size_t cmsg_len, int cmsg_level, int cmsg_type, then data;
            // entries are aligned to 8 bytes
            while (offset + 16 <= controlLen)
            {
                long cmsgLen = BinaryPrimitives.ReadInt64LittleEndian(control[offset..]);
                if (cmsgLen < 16 || offset + cmsgLen > controlLen)
                    break;

                int level = BinaryPrimitives.ReadInt32LittleEndian(control[(offset + 8)..]);
                int type = BinaryPrimitives.ReadInt32LittleEndian(control[(offset + 12)..]);
                var data = control[(offset + 16)..];

                if (level == SOL_SOCKET && type == SO_TIMESTAMPNS && cmsgLen >= 16 + 16)
                {
                    // struct timespec { long tv_sec; long tv_nsec; }
                    long seconds = BinaryPrimitives.ReadInt64LittleEndian(data);
                    long nanoseconds = BinaryPrimitives.ReadInt64LittleEndian(data[8..]);
                    kernelTimestampNS = seconds * 1_000_000_000 + nanoseconds;
                }
                else if (level == IPPROTO_IP && type == IP_PKTINFO && cmsgLen >= 16 + 12)
                {
                    // struct in_pktinfo { int ipi_ifindex; in_addr ipi_spec_dst; in_addr ipi_addr; }
                    // ipi_addr is the header destination address
                    uint networkOrderAddress = BinaryPrimitives.ReadUInt32LittleEndian(data[8..]);
                    destinationAddress = GetCachedAddress(networkOrderAddress);
                }

                offset += (int)((cmsgLen + 7) & ~7L);
            }
        }

        private IPEndPoint? ParseSourceEndPoint(uint nameLen)
        {
            if (nameLen < 8)
                return null;

            var name = this.nameBuffer.AsSpan();

            if (BinaryPrimitives.ReadUInt16LittleEndian(name) != AF_INET)
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
