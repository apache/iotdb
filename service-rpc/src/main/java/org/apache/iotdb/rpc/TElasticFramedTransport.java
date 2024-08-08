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
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.layered.TFramedTransport;

// https://github.com/apache/thrift/blob/master/doc/specs/thrift-rpc.md
public class TElasticFramedTransport extends TTransport {

  public static class Factory extends TTransportFactory {

    /**
     * It is used to prevent the size of the parsing package from being too large and allocating the
     * buffer will cause oom. Therefore, the maximum length of the requested memory is limited when
     * reading.
     */
    protected final int thriftMaxFrameSize;

    /**
     * The capacity of the underlying buffer is allowed to exceed thriftDefaultBufferSize, but if
     * adjacent requests all have sizes smaller than thriftDefaultBufferSize, the underlying buffer
     * will be shrunk beneath thriftDefaultBufferSize. The shrinking is limited at most once per
     * minute to reduce overhead when thriftDefaultBufferSize is set unreasonably or the workload
     * naturally contains both ver large and very small requests.
     */
    protected final int thriftDefaultBufferSize;

    public Factory() {
      this(RpcUtils.THRIFT_DEFAULT_BUF_CAPACITY, RpcUtils.THRIFT_FRAME_MAX_SIZE);
    }

    public Factory(int thriftDefaultBufferSize, int thriftMaxFrameSize) {
      this.thriftDefaultBufferSize = thriftDefaultBufferSize;
      this.thriftMaxFrameSize = thriftMaxFrameSize;
    }

    @Override
    public TTransport getTransport(TTransport trans) {
      return new TElasticFramedTransport(trans, thriftDefaultBufferSize, thriftMaxFrameSize);
    }
  }

  public TElasticFramedTransport(TTransport underlying) {
    this(underlying, RpcUtils.THRIFT_DEFAULT_BUF_CAPACITY, RpcUtils.THRIFT_FRAME_MAX_SIZE);
  }

  public TElasticFramedTransport(
      TTransport underlying, int thriftDefaultBufferSize, int thriftMaxFrameSize) {
    this.underlying = underlying;
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    readBuffer = new AutoScalingBufferReadTransport(thriftDefaultBufferSize);
    writeBuffer = new AutoScalingBufferWriteTransport(thriftDefaultBufferSize);
  }

  protected final int thriftDefaultBufferSize;
  protected final int thriftMaxFrameSize;

  protected final TTransport underlying;
  protected AutoScalingBufferReadTransport readBuffer;
  protected AutoScalingBufferWriteTransport writeBuffer;
  protected final byte[] i32buf = new byte[4];

  @Override
  public boolean isOpen() {
    return underlying.isOpen();
  }

  @Override
  public void open() throws TTransportException {
    underlying.open();
  }

  @Override
  public void close() {
    underlying.close();
  }

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

  protected void readFrame() throws TTransportException {
    underlying.readAll(i32buf, 0, 4);
    int size = TFramedTransport.decodeFrameSize(i32buf);

    if (size < 0) {
      close();
      throw new TTransportException(
          TTransportException.CORRUPTED_DATA, "Read a negative frame size (" + size + ")!");
    }

    if (size > thriftMaxFrameSize) {
      close();
      throw new TTransportException(
          TTransportException.CORRUPTED_DATA,
          "Frame size (" + size + ") larger than protect max size (" + thriftMaxFrameSize + ")!");
    }
    readBuffer.fill(underlying, size);
  }

  @Override
  public void flush() throws TTransportException {
    int length = writeBuffer.getPos();
    TFramedTransport.encodeFrameSize(length, i32buf);
    underlying.write(i32buf, 0, 4);
    underlying.write(writeBuffer.getBuffer(), 0, length);
    writeBuffer.reset();
    if (length > thriftDefaultBufferSize) {
      writeBuffer.resizeIfNecessary(thriftDefaultBufferSize);
    }
    underlying.flush();
  }

  @Override
  public TConfiguration getConfiguration() {
    return underlying.getConfiguration();
  }

  @Override
  public void updateKnownMessageSize(long size) throws TTransportException {
    // do nothing now.
  }

  @Override
  public void checkReadBytesAvailable(long numBytes) throws TTransportException {
    // do nothing now.
    // here we can do some checkm, e.g., see whether the memory is enough.
  }

  @Override
  public void write(byte[] buf, int off, int len) {
    writeBuffer.write(buf, off, len);
  }

  public TTransport getSocket() {
    return underlying;
  }
}
