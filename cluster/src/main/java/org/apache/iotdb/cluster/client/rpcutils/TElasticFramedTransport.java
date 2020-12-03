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
package org.apache.iotdb.cluster.client.rpcutils;

import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

public class TElasticFramedTransport extends TFastFramedTransport {

  /**
   * It is used to prevent the size of the parsing package from being too large and allocating the
   * buffer will cause oom. Therefore, the maximum length of the requested memory is limited when
   * reading. The default value is 512MB
   */
  private static final int PROTECT_MAX_LENGTH = 536870912;

  public static class Factory extends TTransportFactory {

    private final int initialCapacity;
    private final int maxLength;

    public Factory() {
      this(DEFAULT_BUF_CAPACITY, DEFAULT_MAX_LENGTH);
    }

    public Factory(int initialCapacity) {
      this(initialCapacity, DEFAULT_MAX_LENGTH);
    }

    public Factory(int initialCapacity, int maxLength) {
      this.initialCapacity = initialCapacity;
      this.maxLength = maxLength;
    }

    @Override
    public TTransport getTransport(TTransport trans) {
      return new TElasticFramedTransport(trans, initialCapacity, maxLength);
    }
  }

  public TElasticFramedTransport(TTransport underlying) {
    this(underlying, DEFAULT_BUF_CAPACITY, DEFAULT_MAX_LENGTH);
  }

  public TElasticFramedTransport(TTransport underlying, int initialBufferCapacity, int maxLength) {
    super(underlying, initialBufferCapacity, maxLength);
    this.underlying = underlying;
    this.maxLength = maxLength;
    readBuffer = new AutoScalingBufferReadTransport(initialBufferCapacity, 1.5);
    writeBuffer = new AutoScalingBufferWriteTransport(initialBufferCapacity, 1.5);
  }

  private final int maxLength;
  private final TTransport underlying;
  private AutoScalingBufferReadTransport readBuffer;
  private AutoScalingBufferWriteTransport writeBuffer;
  private final byte[] i32buf = new byte[4];

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    int got = readBuffer.read(buf, off, len);
    if (got > 0) {
      return got;
    }

    // Read another frame of data
    readFrame();
    return readBuffer.read(buf, off, len);
  }

  @SuppressWarnings("java:S2177") // no better name
  private void readFrame() throws TTransportException {
    underlying.readAll(i32buf, 0, 4);
    int size = TFramedTransport.decodeFrameSize(i32buf);

    if (size < 0) {
      close();
      throw new TTransportException(TTransportException.CORRUPTED_DATA,
          "Read a negative frame size (" + size + ")!");
    }

    if (size > PROTECT_MAX_LENGTH) {
      close();
      throw new TTransportException(TTransportException.CORRUPTED_DATA,
          "Frame size (" + size + ") larger than protect max length (" + PROTECT_MAX_LENGTH + ")!");
    }

    if (size < maxLength) {
      readBuffer.shrinkSizeIfNecessary(maxLength);
    }
    readBuffer.fill(underlying, size);
  }

  @Override
  public void flush() throws TTransportException {
    int length = writeBuffer.getPos();
    TFramedTransport.encodeFrameSize(length, i32buf);
    underlying.write(i32buf, 0, 4);
    underlying.write(writeBuffer.getBuf().array(), 0, length);
    writeBuffer.reset();
    writeBuffer.shrinkSizeIfNecessary(maxLength);
    underlying.flush();
  }

  @Override
  public void write(byte[] buf, int off, int len) {
    writeBuffer.write(buf, off, len);
  }
}
