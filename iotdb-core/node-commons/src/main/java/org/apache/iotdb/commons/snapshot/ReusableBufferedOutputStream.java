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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * A {@link java.io.BufferedOutputStream} variant whose backing byte array is borrowed from and
 * returned to {@link SnapshotStreamFactory}'s per-thread pool instead of being freshly allocated
 * per stream. Behavior follows {@link java.io.BufferedOutputStream}: {@link #close()} propagates a
 * flush failure while still closing the underlying stream.
 *
 * <p>Writes larger than the buffer bypass it entirely, exactly like the JDK implementation would
 * after flushing.
 */
final class ReusableBufferedOutputStream extends OutputStream {

  private final OutputStream out;
  private final int bufferSize;

  /** Lazily borrowed from {@link SnapshotStreamFactory} on the first write. */
  private byte[] buffer;

  private int count;

  private boolean closed;

  ReusableBufferedOutputStream(final OutputStream out, final int bufferSize) {
    this.out = Objects.requireNonNull(out);
    this.bufferSize = bufferSize;
  }

  @Override
  public void write(final int b) throws IOException {
    ensureOpen();
    ensureBuffer();
    if (count >= buffer.length) {
      flushBuffer();
    }
    buffer[count++] = (byte) b;
  }

  @Override
  public void write(final byte[] b, final int off, final int len) throws IOException {
    ensureOpen();
    Objects.checkFromIndexSize(off, len, b.length);
    if (len == 0) {
      return;
    }
    if (buffer == null && len >= bufferSize) {
      // Large write while the buffer has not even been allocated: skip the buffer entirely
      // instead of allocating it just to flush it immediately.
      out.write(b, off, len);
      return;
    }
    ensureBuffer();
    if (len >= buffer.length) {
      flushBuffer();
      out.write(b, off, len);
      return;
    }
    if (len > buffer.length - count) {
      flushBuffer();
    }
    System.arraycopy(b, off, buffer, count, len);
    count += len;
  }

  @Override
  public void flush() throws IOException {
    ensureOpen();
    flushBuffer();
    out.flush();
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    try (final OutputStream outputStream = out) {
      flush();
    } finally {
      closed = true;
      releaseBuffer();
    }
  }

  private void ensureBuffer() {
    if (buffer == null) {
      buffer = SnapshotStreamFactory.acquireBuffer(bufferSize);
    }
  }

  private void ensureOpen() throws IOException {
    if (closed) {
      throw new IOException();
    }
  }

  private void flushBuffer() throws IOException {
    if (count > 0) {
      out.write(buffer, 0, count);
      count = 0;
    }
  }

  private void releaseBuffer() {
    count = 0;
    if (buffer != null) {
      SnapshotStreamFactory.releaseBuffer(buffer);
      buffer = null;
    }
  }
}
