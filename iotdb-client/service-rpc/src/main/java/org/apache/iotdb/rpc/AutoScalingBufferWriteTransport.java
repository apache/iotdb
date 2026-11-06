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
    try {
      // A small prefix during serialization does not indicate a small frame. Only the complete
      // frame size, supplied after flushing, may trigger shrinking.
      ensureCapacity(pos + len);
    } catch (IOException e) {
      throw new TTransportException(e);
    }
    System.arraycopy(toWrite, off, getBuffer(), pos, len);
    pos += len;
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
    if (size > maxReusableBufferSize) {
      if (temporaryBuffer == null || temporaryBuffer.length < size) {
        // Preserve an unfinished frame when incremental serialization crosses the threshold.
        // Keep amortized growth for subsequent small writes within the same temporary frame.
        int capacity =
            temporaryBuffer == null
                ? size
                : Math.max(size, temporaryBuffer.length + (temporaryBuffer.length >> 1));
        byte[] newBuffer = Arrays.copyOf(getBuffer(), capacity);
        buf.resetToInitialCapacity();
        temporaryBuffer = newBuffer;
      }
    } else {
      buf.ensureCapacity(size);
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
