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
import org.apache.thrift.transport.TEndpointTransport;
import org.apache.thrift.transport.TTransportException;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * Source code of this class is a copy of {@link org.apache.thrift.transport.TByteBuffer}. However,
 * TByteBuffer is a final class and has only one construction method which uses the default
 * TConfiguration. In some cases, the capacity of our ByteBuffer could be so large that it reaches
 * the MaxMessageSize of thrift. We need to customize the TConfiguration and that is why we use this
 * class.
 */
public class ConfigurableTByteBuffer extends TEndpointTransport {
  private final ByteBuffer byteBuffer;

  public ConfigurableTByteBuffer(ByteBuffer byteBuffer) throws TTransportException {
    this(byteBuffer, new TConfiguration());
  }

  public ConfigurableTByteBuffer(ByteBuffer byteBuffer, TConfiguration configuration)
      throws TTransportException {
    super(configuration);
    this.byteBuffer = byteBuffer;
    this.updateKnownMessageSize(byteBuffer.capacity());
  }

  public boolean isOpen() {
    return true;
  }

  public void open() {}

  public void close() {}

  public int read(byte[] buf, int off, int len) throws TTransportException {
    this.checkReadBytesAvailable((long) len);
    int n = Math.min(this.byteBuffer.remaining(), len);
    if (n > 0) {
      try {
        this.byteBuffer.get(buf, off, n);
      } catch (BufferUnderflowException e) {
        throw new TTransportException("Unexpected end of input buffer", e);
      }
    }

    return n;
  }

  public void write(byte[] buf, int off, int len) throws TTransportException {
    try {
      this.byteBuffer.put(buf, off, len);
    } catch (BufferOverflowException e) {
      throw new TTransportException("Not enough room in output buffer", e);
    }
  }

  public ByteBuffer getByteBuffer() {
    return this.byteBuffer;
  }

  public ConfigurableTByteBuffer clear() {
    this.byteBuffer.clear();
    return this;
  }

  public ConfigurableTByteBuffer flip() {
    this.byteBuffer.flip();
    return this;
  }

  public byte[] toByteArray() {
    byte[] data = new byte[this.byteBuffer.remaining()];
    this.byteBuffer.slice().get(data);
    return data;
  }
}
