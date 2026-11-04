/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.snapshot;

import org.apache.iotdb.commons.i18n.CommonMessages;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.SoftReference;

/**
 * Creates the buffered streams used to write and read ConfigNode snapshot files.
 *
 * <p>The buffer size is bounded by {@link #bufferSizeMax}, which is configurable through {@code
 * config_node_snapshot_buffer_size_max} (0 disables buffering). This replaces the previous fixed
 * 32MB buffer of {@code PartitionInfo}: memory-constrained deployments can lower the cap, while the
 * default keeps snapshot I/O fast for large partition tables. Write buffers are pooled per thread
 * through {@link SoftReference}s, so consecutive snapshots on the same thread reuse the array
 * instead of re-allocating it.
 */
public final class SnapshotStreamFactory {

  /** Default upper bound of a snapshot stream buffer, 4MB. */
  public static final long DEFAULT_BUFFER_SIZE_MAX = 4 * 1024 * 1024L;

  private static volatile long bufferSizeMax = DEFAULT_BUFFER_SIZE_MAX;

  /** Thread-local pool of reusable write buffers, kept only as long as GC allows. */
  private static final ThreadLocal<SoftReference<byte[]>> WRITE_BUFFER_POOL =
      ThreadLocal.withInitial(() -> new SoftReference<>(null));

  private SnapshotStreamFactory() {
    // Utility class
  }

  /**
   * Set the upper bound of snapshot stream buffers, in bytes. Values below or equal to zero disable
   * buffering entirely (the raw stream is returned unchanged). Thread-safe; takes effect on the
   * next stream creation.
   */
  public static void setBufferSizeMax(final long sizeInBytes) {
    if (sizeInBytes > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format(
              CommonMessages
                  .EXCEPTION_SNAPSHOT_BUFFER_SIZE_MUST_NOT_EXCEED_ARG_BYTES_BUT_WAS_ARG_D1DA6F7E,
              Integer.MAX_VALUE,
              sizeInBytes));
    }
    bufferSizeMax = Math.max(0L, sizeInBytes);
  }

  public static long getBufferSizeMax() {
    return bufferSizeMax;
  }

  /**
   * Wrap {@code raw} with a buffered output stream whose buffer is at most {@link #bufferSizeMax}
   * bytes. The buffer is reusable, so the same thread writing several snapshots does not repeatedly
   * allocate it.
   *
   * @param raw the raw stream to buffer
   * @return a buffered stream, or {@code raw} itself if buffering is disabled
   */
  public static OutputStream createOutputStream(final OutputStream raw) {
    final int bufferSize = (int) bufferSizeMax;
    return bufferSize <= 0 ? raw : new ReusableBufferedOutputStream(raw, bufferSize);
  }

  /**
   * Wrap {@code raw} with a buffered input stream whose buffer is sized to at most {@code
   * fileSize}, capped by {@link #bufferSizeMax}. Reading never allocates more than the configured
   * cap, no matter how large the snapshot file is.
   *
   * @param raw the raw stream to buffer
   * @param fileSize size of the file being read, in bytes
   * @return a buffered stream, or {@code raw} itself if buffering is disabled or the file is empty
   */
  public static InputStream createInputStream(final InputStream raw, final long fileSize) {
    final long bufferSize = Math.min(fileSize, bufferSizeMax);
    return bufferSize <= 0 ? raw : new BufferedInputStream(raw, (int) bufferSize);
  }

  /**
   * Borrow a reusable buffer of at least {@code minSize} bytes that does not exceed the current
   * cap. If the thread's pool holds a large enough buffer that still fits within {@link
   * #bufferSizeMax}, it is handed out (and removed from the pool, so concurrent borrowers never see
   * the same array); otherwise a new buffer is allocated.
   */
  static byte[] acquireBuffer(final int minSize) {
    final long cap = bufferSizeMax;
    final SoftReference<byte[]> reference = WRITE_BUFFER_POOL.get();
    final byte[] cached = reference == null ? null : reference.get();
    // Reuse only a buffer that fits the current cap: after the cap has been lowered, a larger
    // pooled buffer must not be handed out again, since the backing buffer may never exceed
    // bufferSizeMax.
    if (cached != null && cached.length >= minSize && cached.length <= cap) {
      // Take ownership of the cached buffer so a nested borrower allocates its own instead of
      // silently sharing the array.
      WRITE_BUFFER_POOL.set(new SoftReference<>(null));
      return cached;
    }
    return new byte[minSize];
  }

  /** Return a buffer to the thread's pool for reuse by a later snapshot on the same thread. */
  static void releaseBuffer(final byte[] buffer) {
    if (buffer != null) {
      WRITE_BUFFER_POOL.set(new SoftReference<>(buffer));
    }
  }
}
