using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Threading;

namespace Haukcode.HighPerfComm
{
    /// <summary>
    /// A MemoryPool for the fixed-size packet buffers on the send/receive hot paths.
    /// MemoryPool&lt;byte&gt;.Shared sits on ArrayPool, which retains only a few dozen arrays per
    /// size class — but the bounded send queues can hold thousands of packets in flight per
    /// shard, so under load nearly every Rent misses the pool and allocates (measured ~5 MB/s of
    /// young garbage at 36,000 packets/sec, where gen0 pause time costs output frames). This
    /// pool retains owner+buffer pairs together, so a Rent allocates nothing on a hit. Beyond
    /// the cap it degrades to plain allocation (the pre-pool behavior); rents larger than the
    /// fixed size are delegated to MemoryPool&lt;byte&gt;.Shared. Like ArrayPool, buffers are NOT
    /// zeroed on rent and the returned Memory is the full fixed size, which may exceed the
    /// requested length (ArrayPool's power-of-two rounding already behaved that way). A second
    /// Dispose of the same owner is a no-op, so a double-return can never hand one buffer to
    /// two renters.
    /// </summary>
    public sealed class FixedSizeMemoryPool : MemoryPool<byte>
    {
        private readonly ConcurrentQueue<PooledOwner> pool = new();
        private readonly int bufferSize;
        private readonly int maxPooled;
        private int pooledCount;

        public FixedSizeMemoryPool(int bufferSize, int maxPooled)
        {
            this.bufferSize = bufferSize;
            this.maxPooled = maxPooled;
        }

        public override int MaxBufferSize => int.MaxValue;

        public override IMemoryOwner<byte> Rent(int minBufferSize = -1)
        {
            if (minBufferSize > this.bufferSize)
                return Shared.Rent(minBufferSize);

            if (this.pool.TryDequeue(out var owner))
            {
                Interlocked.Decrement(ref this.pooledCount);
                owner.Reset();

                return owner;
            }

            return new PooledOwner(this);
        }

        private void Return(PooledOwner owner)
        {
            if (Interlocked.Increment(ref this.pooledCount) <= this.maxPooled)
            {
                this.pool.Enqueue(owner);
            }
            else
            {
                // Pool is full: let the owner and its buffer become plain garbage.
                Interlocked.Decrement(ref this.pooledCount);
            }
        }

        protected override void Dispose(bool disposing)
        {
        }

        private sealed class PooledOwner : IMemoryOwner<byte>
        {
            private readonly FixedSizeMemoryPool owningPool;
            private readonly byte[] buffer;
            private int returned;

            internal PooledOwner(FixedSizeMemoryPool owningPool)
            {
                this.owningPool = owningPool;
                this.buffer = new byte[owningPool.bufferSize];
            }

            public Memory<byte> Memory => this.buffer;

            internal void Reset()
            {
                this.returned = 0;
            }

            public void Dispose()
            {
                if (Interlocked.Exchange(ref this.returned, 1) == 0)
                    this.owningPool.Return(this);
            }
        }
    }
}
