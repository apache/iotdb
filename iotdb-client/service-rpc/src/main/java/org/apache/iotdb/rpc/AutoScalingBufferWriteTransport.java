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

/**
 * Note that this class is mainly copied from class {@link
 * org.apache.thrift.transport.AutoExpandingBufferWriteTransport}. since that class does not support
 * inheritance, so rewrite this class.
 */
public class AutoScalingBufferWriteTransport extends NonOpenTransport {

  private final AutoResizingBuffer buf;
  private int pos;

  public AutoScalingBufferWriteTransport(int initialCapacity) {
    this.buf = new AutoResizingBuffer(initialCapacity);
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

  // Framed callers keep capacity within their limit. A fitting write needs no further limit
  // checks; compare remaining space rather than adding pos and len, which could overflow.
  boolean tryWriteWithinCapacity(byte[] toWrite, int off, int len) {
    byte[] array = buf.array();
    if (len > array.length - pos) {
      return false;
    }
    System.arraycopy(toWrite, off, array, pos, len);
    pos += len;
    return true;
  }

  void write(byte[] toWrite, int off, int len, int maxCapacity) throws TTransportException {
    int limit = Math.min(maxCapacity, AutoResizingBuffer.MAX_ARRAY_SIZE);
    long requiredCapacity = (long) pos + len;
    if (requiredCapacity > limit) {
      throw new TTransportException(
          TTransportException.CORRUPTED_DATA,
          "Write buffer size ("
              + requiredCapacity
              + ") larger than maximum buffer size ("
              + limit
              + ")!");
    }
    // A small prefix during serialization does not indicate a small frame. Only the complete
    // frame size, supplied after flushing, may trigger shrinking.
    buf.ensureCapacity((int) requiredCapacity, limit);
    System.arraycopy(toWrite, off, buf.array(), pos, len);
    pos += len;
  }

  public int getPos() {
    return pos;
  }

  public void reset() {
    pos = 0;
  }

  public void resizeIfNecessary(int size) {
    buf.resizeIfNecessary(size);
  }

  @Override
  public byte[] getBuffer() {
    return buf.array();
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
