/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.block.RefCountedByteBuffer;
import org.opensearch.index.store.metrics.CryptoMetricsService;

/**
 * Bounded allocator for direct ByteBuffers with freelist recycling.
 *
 * <p>When a {@link RefCountedByteBuffer} wrapper is GC'd, the {@link Cleaner}
 * allocates a <b>new</b> DirectByteBuffer and adds it to the freelist.
 * The old buffer is freed by GC's own DirectByteBuffer Cleaner.
 * This ensures:
 * <ul>
 *   <li>No use-after-recycle corruption (old buffer is never reused)</li>
 *   <li>Zero malloc on the hot read path (freelist provides pre-allocated buffers)</li>
 *   <li>Bounded native memory (pool tracks total count)</li>
 * </ul>
 *
 * <p>Requires jemalloc for efficient malloc/free without fragmentation.
 *
 * @opensearch.internal
 */
public class ByteBufferPool extends AbstractPool<RefCountedByteBuffer> {

    private static final Logger LOGGER = LogManager.getLogger(ByteBufferPool.class);
    private static final Cleaner CLEANER = Cleaner.create();
    private static final long GC_REQUEST_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

    private final ConcurrentLinkedQueue<ByteBuffer> freeList = new ConcurrentLinkedQueue<>();
    private final AtomicInteger buffersInUse = new AtomicInteger(0);
    private final AtomicInteger totalProvisioned = new AtomicInteger(0);
    private final AtomicLong lastGCRequestNanos = new AtomicLong(0);

    public ByteBufferPool(long totalMemory, int segmentSize) {
        super(totalMemory, segmentSize);
    }

    private static final int MAX_RETRY_ATTEMPTS = 10;
    private static final long RETRY_WAIT_NANOS = TimeUnit.MILLISECONDS.toNanos(50);

    @Override
    public RefCountedByteBuffer acquire() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Thread interrupted before pool acquire");
        }
        ensureOpen();

        // Fast path: grab from freelist (no malloc, lock-free CAS)
        ByteBuffer buf = freeList.poll();
        if (buf != null) {
            buf.clear().order(ByteOrder.LITTLE_ENDIAN);
            buffersInUse.incrementAndGet();
            return wrapAndRegister(buf);
        }

        // Freelist empty — trigger GC and retry
        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            requestGCIfNeeded();
            java.util.concurrent.locks.LockSupport.parkNanos(RETRY_WAIT_NANOS * attempt);

            buf = freeList.poll();
            if (buf != null) {
                buf.clear().order(ByteOrder.LITTLE_ENDIAN);
                buffersInUse.incrementAndGet();
                return wrapAndRegister(buf);
            }

            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Thread interrupted while waiting for freelist");
            }
        }

        // All retries exhausted — allocate new as last resort
        LOGGER
            .warn(
                "Freelist empty after {} retries (provisioned={}, max={}, inUse={}, freeList={}), allocating new buffer",
                MAX_RETRY_ATTEMPTS,
                totalProvisioned.get(),
                maxSegments,
                buffersInUse.get(),
                freeList.size()
            );
        return allocateNewAndWrap();
    }

    @Override
    public RefCountedByteBuffer tryAcquire(long timeout, TimeUnit unit) throws Exception {
        ensureOpen();

        ByteBuffer buf = freeList.poll();
        if (buf != null) {
            buf.clear().order(ByteOrder.LITTLE_ENDIAN);
            buffersInUse.incrementAndGet();
            return wrapAndRegister(buf);
        }

        // Freelist empty — trigger GC and retry with timeout
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            requestGCIfNeeded();
            java.util.concurrent.locks.LockSupport.parkNanos(RETRY_WAIT_NANOS * attempt);

            buf = freeList.poll();
            if (buf != null) {
                buf.clear().order(ByteOrder.LITTLE_ENDIAN);
                buffersInUse.incrementAndGet();
                return wrapAndRegister(buf);
            }

            if (System.nanoTime() > deadlineNanos)
                break;
        }

        LOGGER
            .warn(
                "Freelist empty after retries (provisioned={}, max={}, inUse={}, freeList={}), allocating for write path",
                totalProvisioned.get(),
                maxSegments,
                buffersInUse.get(),
                freeList.size()
            );
        return allocateNewAndWrap();
    }

    private RefCountedByteBuffer allocateNewAndWrap() {
        ByteBuffer direct = ByteBuffer.allocateDirect(segmentSize).order(ByteOrder.LITTLE_ENDIAN);
        totalProvisioned.incrementAndGet();
        buffersInUse.incrementAndGet();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Allocated new DirectByteBuffer, provisioned={}/{}", totalProvisioned.get(), maxSegments);
        }
        return wrapAndRegister(direct);
    }

    private RefCountedByteBuffer wrapAndRegister(ByteBuffer direct) {
        RefCountedByteBuffer wrapper = new RefCountedByteBuffer(direct, segmentSize);
        // When wrapper is GC'd: old buffer freed by GC, NEW buffer allocated into freelist
        CLEANER.register(wrapper, new ReplenishFreeList(freeList, segmentSize, buffersInUse, this));
        return wrapper;
    }

    /**
     * Invoked by Cleaner when a RefCountedByteBuffer wrapper becomes phantom reachable.
     * Decrements buffersInUse and allocates a NEW DirectByteBuffer into the freelist.
     * The old buffer is freed separately by DirectByteBuffer's own JDK Cleaner.
     *
     * <p>Must NOT reference the old wrapper or its ByteBuffer (would prevent GC).
     */
    private static class ReplenishFreeList implements Runnable {
        private final ConcurrentLinkedQueue<ByteBuffer> freeList;
        private final int size;
        private final AtomicInteger buffersInUse;
        private final ByteBufferPool pool;

        ReplenishFreeList(ConcurrentLinkedQueue<ByteBuffer> freeList, int size, AtomicInteger buffersInUse, ByteBufferPool pool) {
            this.freeList = freeList;
            this.size = size;
            this.buffersInUse = buffersInUse;
            this.pool = pool;
        }

        @Override
        public void run() {
            buffersInUse.decrementAndGet();
            try {
                ByteBuffer fresh = ByteBuffer.allocateDirect(size).order(ByteOrder.LITTLE_ENDIAN);
                freeList.offer(fresh);
            } catch (OutOfMemoryError e) {
                LogManager.getLogger(ByteBufferPool.class).warn("Failed to replenish freelist: direct memory exhausted");
            }
        }
    }

    private void requestGCIfNeeded() {
        long now = System.nanoTime();
        long last = lastGCRequestNanos.get();
        if (now - last > GC_REQUEST_INTERVAL_NANOS && lastGCRequestNanos.compareAndSet(last, now)) {
            LOGGER
                .warn(
                    "Pool exhausted (inUse={}, provisioned={}, max={}, freeList={}), requesting GC",
                    buffersInUse.get(),
                    totalProvisioned.get(),
                    maxSegments,
                    freeList.size()
                );
            System.gc();
        }
    }

    @Override
    public void release(RefCountedByteBuffer buffer) {
        // no-op: Cleaner handles replenishment when wrapper is GC'd
    }

    @Override
    public long availableMemory() {
        return (long) (freeList.size() + Math.max(0, maxSegments - totalProvisioned.get())) * segmentSize;
    }

    public int getBuffersInUse() {
        return buffersInUse.get();
    }

    public int getTotalProvisioned() {
        return totalProvisioned.get();
    }

    public int getFreeListSize() {
        return freeList.size();
    }

    @Override
    public boolean isUnderPressure() {
        return freeList.isEmpty() && (maxSegments - totalProvisioned.get()) < (maxSegments * 0.1);
    }

    @Override
    public void warmUp(long targetSegments) {
        long toAllocate = Math.min(targetSegments, maxSegments);
        LOGGER.info("Warming up freelist with {} buffers (max={})", toAllocate, maxSegments);
        for (long i = 0; i < toAllocate; i++) {
            try {
                ByteBuffer buf = ByteBuffer.allocateDirect(segmentSize).order(ByteOrder.LITTLE_ENDIAN);
                freeList.offer(buf);
                totalProvisioned.incrementAndGet();
            } catch (OutOfMemoryError e) {
                LOGGER.warn("Warmup stopped at {} buffers: direct memory exhausted", i);
                break;
            }
        }
        LOGGER.info("Warmup complete: freeList={}, provisioned={}", freeList.size(), totalProvisioned.get());
    }

    @Override
    public void close() {
        if (closed)
            return;
        closed = true;
        int drained = 0;
        while (freeList.poll() != null)
            drained++;
        LOGGER
            .info(
                "ByteBufferPool closed: drained {} freelist buffers, inUse={}, provisioned={}",
                drained,
                buffersInUse.get(),
                totalProvisioned.get()
            );
    }

    @Override
    public String poolStats() {
        int inUse = buffersInUse.get();
        int provisioned = totalProvisioned.get();
        int free = freeList.size();
        return String
            .format(
                "ByteBufferPool[max=%d, provisioned=%d, inUse=%d, freeList=%d, utilization=%.1f%%]",
                maxSegments,
                provisioned,
                inUse,
                free,
                maxSegments > 0 ? (double) inUse / maxSegments * 100 : 0
            );
    }

    @Override
    public void recordStats() {
        int inUse = buffersInUse.get();
        int free = freeList.size();
        double pct = maxSegments > 0 ? (double) inUse / maxSegments : 0;
        CryptoMetricsService.getInstance().recordPoolStats(SegmentType.PRIMARY, maxSegments, inUse, free, pct, pct);
    }
}
