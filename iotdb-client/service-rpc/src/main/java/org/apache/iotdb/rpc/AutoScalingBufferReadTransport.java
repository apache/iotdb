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
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;

public class AutoScalingBufferReadTransport extends NonOpenTransport {

  private final AutoResizingBuffer buf;
  private final int maxReusableBufferSize;
  private byte[] temporaryBuffer;
  private int pos = 0;
  private int limit = 0;

  public AutoScalingBufferReadTransport(int initialCapacity) throws IOException {
    this(initialCapacity, Integer.MAX_VALUE);
  }

  AutoScalingBufferReadTransport(int initialCapacity, int maxReusableBufferSize)
      throws IOException {
    this.buf = new AutoResizingBuffer(initialCapacity);
    this.maxReusableBufferSize = maxReusableBufferSize;
  }

  public void fill(TTransport inTrans, int length) throws TTransportException {
    try {
      resizeIfNecessary(length);
    } catch (IOException e) {
      throw new TTransportException(e);
    }
    inTrans.readAll(getBuffer(), 0, length);
    pos = 0;
    limit = length;
  }

  @Override
  public final int read(byte[] target, int off, int len) {
    int amtToRead = Math.min(len, getBytesRemainingInBuffer());
    System.arraycopy(getBuffer(), pos, target, off, amtToRead);
    consumeBuffer(amtToRead);
    return amtToRead;
  }

  @Override
  public void write(byte[] buf, int off, int len) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void consumeBuffer(int len) {
    pos += len;
    if (pos == limit && temporaryBuffer != null) {
      // Protocols may keep their own reference to a consumed binary value. Drop only our reference.
      temporaryBuffer = null;
      pos = 0;
      limit = 0;
    }
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

  @Override
  public final byte[] getBuffer() {
    return temporaryBuffer == null ? buf.array() : temporaryBuffer;
  }

  @Override
  public final int getBufferPosition() {
    return pos;
  }

  @Override
  public final int getBytesRemainingInBuffer() {
    return limit - pos;
  }

  public void resizeIfNecessary(int size) throws IOException {
    if (size > maxReusableBufferSize) {
      byte[] newBuffer = new byte[size];
      buf.resetToInitialCapacity();
      temporaryBuffer = newBuffer;
    } else {
      buf.resizeIfNecessary(size);
      temporaryBuffer = null;
    }
  }

  @Override
  public void close() {
    super.close();
    temporaryBuffer = null;
    pos = 0;
    limit = 0;
    buf.close();
  }

  public void limit(int newLimit) {
    this.limit = newLimit;
  }

  public void position(int newPosition) {
    this.pos = newPosition;
  }
}
