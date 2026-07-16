using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;

namespace Haukcode.HighPerfComm
{
    /// <summary>
    /// Windows kernel receive timestamping (SIO_TIMESTAMPING, Windows 10 2004+ / Server 2022+,
    /// UDP only): every datagram is stamped with the QueryPerformanceCounter value at the time
    /// the network stack received it from the driver, so time spent waiting in the socket
    /// buffer no longer distorts receive timestamps. The timestamp arrives as a SO_TIMESTAMP
    /// control message, which .NET's Socket API does not surface, so this calls WSARecvMsg
    /// directly (obtained through SIO_GET_EXTENSION_FUNCTION_POINTER, the documented way).
    ///
    /// Instances are not thread-safe: one per receive loop, matching the dedicated receive
    /// thread in Client. TryCreate returns null on non-Windows and on Windows versions where
    /// the ioctl is unsupported, so callers fall back to the portable receive path.
    /// </summary>
    public sealed unsafe class WindowsReceiveTimestamping : IReceiveTimestamping
    {
        // SIO_TIMESTAMPING = _WSAIOW(IOC_VENDOR, 235)
        private const int SIO_TIMESTAMPING = unchecked((int)0x980000EB);
        // SIO_GET_EXTENSION_FUNCTION_POINTER = _WSAIORW(IOC_WS2, 6)
        private const int SIO_GET_EXTENSION_FUNCTION_POINTER = unchecked((int)0xC8000006);
        private const uint TIMESTAMPING_FLAG_RX = 0x1;
        private const int SOL_SOCKET = 0xffff;
        private const int SO_TIMESTAMP = 0x300A;
        private const int IPPROTO_IP = 0;
        private const int IP_PKTINFO = 19;
        private const ushort AF_INET = 2;
        private const int SOCKET_ERROR = -1;

        private static readonly Guid WSARecvMsgGuid = new Guid(0xf689d7c8, 0x6f1f, 0x436b, 0x8a, 0x53, 0xe5, 0x4f, 0xe3, 0x51, 0xc3, 0x22);

        [DllImport("iphlpapi.dll")]
        private static extern int ConvertInterfaceIndexToLuid(uint interfaceIndex, ulong* interfaceLuid);

        [DllImport("iphlpapi.dll")]
        private static extern int GetInterfaceActiveTimestampCapabilities(ulong* interfaceLuid, byte* timestampCapabilities);

        [UnmanagedFunctionPointer(CallingConvention.StdCall, SetLastError = true)]
        private delegate int WSARecvMsgDelegate(IntPtr socket, IntPtr msg, out uint bytesReceived, IntPtr overlapped, IntPtr completionRoutine);

        [StructLayout(LayoutKind.Sequential)]
        private struct WsaBuf
        {
            public uint Length;
            public IntPtr Buffer;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct WsaMsg
        {
            public IntPtr Name;
            public int NameLen;
            public IntPtr Buffers;
            public uint BufferCount;
            public WsaBuf Control;
            public uint Flags;
        }

        private readonly Socket socket;
        private readonly WSARecvMsgDelegate recvMsg;
        private readonly byte[] nameBuffer = new byte[16];      // sockaddr_in
        private readonly byte[] controlBuffer = new byte[256];

        // Stopwatch is QPC-backed on Windows, so this is QueryPerformanceFrequency
        private static readonly long qpcFrequency = Stopwatch.Frequency;

        // A stream sees a handful of distinct sources and destination groups but tens of
        // thousands of packets per second; caching keeps the receive loop allocation-free.
        // Capped as a safety valve; misses past the cap just allocate like before.
        private readonly Dictionary<long, IPEndPoint> endPointCache = new();
        private readonly Dictionary<uint, IPAddress> addressCache = new();
        private const int MaxCacheEntries = 4096;

        private WindowsReceiveTimestamping(Socket socket, WSARecvMsgDelegate recvMsg)
        {
            this.socket = socket;
            this.recvMsg = recvMsg;
        }

        /// <summary>
        /// Enable kernel receive timestamps on the socket. Returns null when unavailable
        /// (not Windows, or the OS refused the ioctl — pre-2004 builds and non-UDP sockets
        /// do) — callers then use their portable receive path unchanged.
        /// </summary>
        public static WindowsReceiveTimestamping? TryCreate(Socket socket)
        {
            // The control-message parsing assumes the 64-bit WSACMSGHDR layout
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows) || IntPtr.Size != 8)
                return null;

            try
            {
                // Enabling the socket option is not enough on Windows: the SO_TIMESTAMP
                // control message arrives with a ZERO value unless the adapter's miniport
                // driver has software timestamping enabled (the *SoftwareTimestamp INF
                // keyword — off by default on most drivers). Decline here when no adapter
                // stamps received packets so callers report the honest user-space path
                // instead of claiming kernel timestamps that never materialize.
                if (!AnyAdapterStampsReceivedPackets())
                    return null;

                // TIMESTAMPING_CONFIG { ULONG Flags; USHORT TxTimestampsBuffered; }
                var config = new byte[8];
                BinaryPrimitives.WriteUInt32LittleEndian(config, TIMESTAMPING_FLAG_RX);
                socket.IOControl(SIO_TIMESTAMPING, config, null);

                var functionPointer = new byte[IntPtr.Size];
                socket.IOControl(SIO_GET_EXTENSION_FUNCTION_POINTER, WSARecvMsgGuid.ToByteArray(), functionPointer);
                var recvMsgPtr = (IntPtr)BinaryPrimitives.ReadInt64LittleEndian(functionPointer);
                if (recvMsgPtr == IntPtr.Zero)
                    return null;

                var recvMsg = Marshal.GetDelegateForFunctionPointer<WSARecvMsgDelegate>(recvMsgPtr);

                // The stack only attaches the destination-address control message when packet
                // info is enabled (ReceiveMessageFrom normally turns this on lazily)
                socket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.PacketInformation, true);

                return new WindowsReceiveTimestamping(socket, recvMsg);
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// True when at least one up, non-loopback adapter has SOFTWARE receive timestamping
        /// active (INTERFACE_TIMESTAMP_CAPABILITIES.SoftwareCapabilities.AllReceive). Hardware
        /// timestamping deliberately does not qualify: those stamps use the NIC's own clock,
        /// not QPC, so this class could not convert them. If the capability API is missing
        /// (Windows 10 before build 20348) this returns true and per-packet zero-timestamp
        /// fallback in the caller covers the difference.
        /// </summary>
        private static bool AnyAdapterStampsReceivedPackets()
        {
            try
            {
                byte* capabilities = stackalloc byte[32];

                foreach (var nic in System.Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces())
                {
                    if (nic.OperationalStatus != System.Net.NetworkInformation.OperationalStatus.Up
                        || nic.NetworkInterfaceType == System.Net.NetworkInformation.NetworkInterfaceType.Loopback)
                        continue;

                    uint index;
                    try
                    {
                        index = (uint)nic.GetIPProperties().GetIPv4Properties().Index;
                    }
                    catch
                    {
                        continue;
                    }

                    ulong luid;
                    if (ConvertInterfaceIndexToLuid(index, &luid) != 0)
                        continue;

                    // INTERFACE_TIMESTAMP_CAPABILITIES: ULONG64 HardwareClockFrequencyHz,
                    // BOOLEAN SupportsCrossTimestamp, 11 hardware BOOLEANs, then
                    // SoftwareCapabilities { AllReceive, AllTransmit, TaggedTransmit } —
                    // software AllReceive is at byte offset 20
                    if (GetInterfaceActiveTimestampCapabilities(&luid, capabilities) == 0 && capabilities[20] != 0)
                        return true;
                }

                return false;
            }
            catch (EntryPointNotFoundException)
            {
                return true;
            }
            catch (DllNotFoundException)
            {
                return true;
            }
        }

        public int Receive(ArraySegment<byte> buffer, out IPEndPoint? remoteEndPoint, out IPAddress? destinationAddress, out long kernelTimestampNS)
        {
            remoteEndPoint = null;
            destinationAddress = null;
            kernelTimestampNS = 0;

            int result;
            uint received = 0;
            int errorCode = 0;
            int nameLen;
            int controlLen;

            fixed (byte* dataPtr = &buffer.Array![buffer.Offset])
            fixed (byte* namePtr = this.nameBuffer)
            fixed (byte* controlPtr = this.controlBuffer)
            {
                var wsaBuf = new WsaBuf
                {
                    Length = (uint)buffer.Count,
                    Buffer = (IntPtr)dataPtr
                };

                var msg = new WsaMsg
                {
                    Name = (IntPtr)namePtr,
                    NameLen = this.nameBuffer.Length,
                    Buffers = (IntPtr)(&wsaBuf),
                    BufferCount = 1,
                    Control = new WsaBuf
                    {
                        Length = (uint)this.controlBuffer.Length,
                        Buffer = (IntPtr)controlPtr
                    },
                    Flags = 0
                };

                result = this.recvMsg(this.socket.Handle, (IntPtr)(&msg), out received, IntPtr.Zero, IntPtr.Zero);

                if (result == SOCKET_ERROR)
                {
                    errorCode = Marshal.GetLastWin32Error();
                    nameLen = 0;
                    controlLen = 0;
                }
                else
                {
                    nameLen = msg.NameLen;
                    controlLen = (int)msg.Control.Length;
                }
            }

            if (result == SOCKET_ERROR)
                throw new SocketException(errorCode);

            ParseControlMessages(controlLen, out kernelTimestampNS, out destinationAddress);
            remoteEndPoint = ParseSourceEndPoint(nameLen);

            return (int)received;
        }

        private void ParseControlMessages(int controlLen, out long kernelTimestampNS, out IPAddress? destinationAddress)
        {
            kernelTimestampNS = 0;
            destinationAddress = null;

            var control = this.controlBuffer.AsSpan();
            int offset = 0;

            // WSACMSGHDR on x64: SIZE_T cmsg_len, INT cmsg_level, INT cmsg_type, then data;
            // entries are aligned to 8 bytes (same shape as the 64-bit Linux cmsghdr)
            while (offset + 16 <= controlLen)
            {
                long cmsgLen = BinaryPrimitives.ReadInt64LittleEndian(control[offset..]);
                if (cmsgLen < 16 || offset + cmsgLen > controlLen)
                    break;

                int level = BinaryPrimitives.ReadInt32LittleEndian(control[(offset + 8)..]);
                int type = BinaryPrimitives.ReadInt32LittleEndian(control[(offset + 12)..]);
                var data = control[(offset + 16)..];

                if (level == SOL_SOCKET && type == SO_TIMESTAMP && cmsgLen >= 16 + 8)
                {
                    // UINT64 QueryPerformanceCounter value at arrival; convert to nanoseconds
                    // with integer math so no precision is lost at large uptimes
                    long qpc = BinaryPrimitives.ReadInt64LittleEndian(data);
                    kernelTimestampNS = qpc / qpcFrequency * 1_000_000_000
                        + qpc % qpcFrequency * 1_000_000_000 / qpcFrequency;
                }
                else if (level == IPPROTO_IP && type == IP_PKTINFO && cmsgLen >= 16 + 8)
                {
                    // IN_PKTINFO { IN_ADDR ipi_addr; ULONG ipi_ifindex; } — the header
                    // destination address comes first on Windows (unlike Linux)
                    uint networkOrderAddress = BinaryPrimitives.ReadUInt32LittleEndian(data);
                    destinationAddress = GetCachedAddress(networkOrderAddress);
                }

                offset += (int)((cmsgLen + 7) & ~7L);
            }
        }

        private IPEndPoint? ParseSourceEndPoint(int nameLen)
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
