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

package org.apache.iotdb.rpc;

import org.apache.iotdb.rpc.i18n.RpcMessages;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.util.Arrays;

/**
 * Note that this class is mainly copied from class {@link
 * org.apache.thrift.transport.AutoExpandingBufferWriteTransport}. since that class does not support
 * inheritance, so rewrite this class.
 */
public class AutoScalingBufferWriteTransport extends NonOpenTransport {

  private final AutoResizingBuffer buf;
  private final int maxReusableBufferSize;
  private byte[] temporaryBuffer;
  private int pos;

  public AutoScalingBufferWriteTransport(int initialCapacity) throws IOException {
    this(initialCapacity, Integer.MAX_VALUE);
  }

  AutoScalingBufferWriteTransport(int initialCapacity, int maxReusableBufferSize)
      throws IOException {
    this.buf = new AutoResizingBuffer(initialCapacity);
    this.maxReusableBufferSize = maxReusableBufferSize;
    this.pos = 0;
  }

  @Override
  public int read(byte[] buf, int off, int len) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(byte[] toWrite, int off, int len) throws TTransportException {
    write(toWrite, off, len, AutoResizingBuffer.MAX_ARRAY_SIZE);
  }

  void write(byte[] toWrite, int off, int len, int maxCapacity) throws TTransportException {
    int limit = Math.min(maxCapacity, AutoResizingBuffer.MAX_ARRAY_SIZE);
    long requiredCapacity = (long) pos + len;
    if (requiredCapacity > limit) {
      throw new TTransportException(
          TTransportException.CORRUPTED_DATA,
          String.format(
              RpcMessages
                  .EXCEPTION_WRITE_BUFFER_SIZE_ARG_LARGER_THAN_MAXIMUM_BUFFER_SIZE_ARG_C0A47F3D,
              requiredCapacity,
              limit));
    }
    // A small prefix during serialization does not indicate a small frame. Only the complete
    // frame size, supplied after flushing, may trigger shrinking.
    try {
      ensureCapacity((int) requiredCapacity, limit);
    } catch (IOException e) {
      throw new TTransportException(e);
    }
    System.arraycopy(toWrite, off, getBuffer(), pos, len);
    pos += len;
  }

  // Framed callers keep capacity within their limit. A fitting write needs no further limit
  // checks; compare remaining space rather than adding pos and len, which could overflow.
  boolean tryWriteWithinCapacity(byte[] toWrite, int off, int len) {
    byte[] array = getBuffer();
    // A closed reusable buffer must reserve its memory again. Crossing the reuse threshold
    // must also go through ensureCapacity so the large frame uses temporary storage.
    if (buf.isClosed()
        || len > array.length - pos
        || (temporaryBuffer == null && len > maxReusableBufferSize - pos)) {
      return false;
    }
    System.arraycopy(toWrite, off, array, pos, len);
    pos += len;
    return true;
  }

  public int getPos() {
    return pos;
  }

  public void reset() {
    pos = 0;
    temporaryBuffer = null;
  }

  void resetAndResize() throws IOException {
    int length = pos;
    reset();
    // A completed large frame must not recreate the temporary array just released by reset().
    if (length <= maxReusableBufferSize) {
      buf.resizeIfNecessary(length);
    }
  }

  @Override
  public void close() {
    super.close();
    reset();
    buf.close();
  }

  public void resizeIfNecessary(int size) throws IOException {
    if (size > maxReusableBufferSize) {
      ensureCapacity(size);
    } else {
      buf.resizeIfNecessary(size);
      temporaryBuffer = null;
    }
  }

  private void ensureCapacity(int size) throws IOException {
    ensureCapacity(size, AutoResizingBuffer.MAX_ARRAY_SIZE);
  }

  private void ensureCapacity(int size, int maxCapacity) throws IOException {
    if (size > maxReusableBufferSize) {
      if (temporaryBuffer == null || temporaryBuffer.length < size) {
        // Preserve an unfinished frame when incremental serialization crosses the threshold.
        // Keep amortized growth for subsequent small writes within the same temporary frame.
        int capacity =
            temporaryBuffer == null
                ? size
                : Math.min(
                    AutoResizingBuffer.calculateNewCapacity(temporaryBuffer.length, size),
                    maxCapacity);
        byte[] newBuffer = Arrays.copyOf(getBuffer(), capacity);
        buf.resetToInitialCapacity();
        temporaryBuffer = newBuffer;
      }
    } else {
      buf.ensureCapacity(size, maxCapacity);
    }
  }

  @Override
  public byte[] getBuffer() {
    return temporaryBuffer == null ? buf.array() : temporaryBuffer;
  }

  @Override
  public TConfiguration getConfiguration() {
    // should never call this method.
    return null;
  }

  @Override
  public void updateKnownMessageSize(long size) throws TTransportException {
    // do Nothing
  }

  @Override
  public void checkReadBytesAvailable(long numBytes) throws TTransportException {
    // do Nothing
  }
}
