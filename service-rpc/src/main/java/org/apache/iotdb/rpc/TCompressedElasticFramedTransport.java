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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.thrift.transport.TByteBuffer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public abstract class TCompressedElasticFramedTransport extends TElasticFramedTransport {

  private TByteBuffer writeCompressBuffer;
  private TByteBuffer readCompressBuffer;

  protected TCompressedElasticFramedTransport(TTransport underlying, int initialBufferCapacity,
      int maxLength) {
    super(underlying, initialBufferCapacity, maxLength);
    writeCompressBuffer = new TByteBuffer(ByteBuffer.allocate(initialBufferCapacity));
    readCompressBuffer = new TByteBuffer(ByteBuffer.allocate(initialBufferCapacity));
  }

  @Override
  protected void readFrame() throws TTransportException {
    underlying.readAll(i32buf, 0, 4);
    int size = TFramedTransport.decodeFrameSize(i32buf);

    if (size < 0) {
      close();
      throw new TTransportException(TTransportException.CORRUPTED_DATA,
          "Read a negative frame size (" + size + ")!");
    }

    readBuffer.fill(underlying, size);
    RpcStat.readCompressedBytes.addAndGet(size);
    try {
      int uncompressedLength = uncompressedLength(readBuffer.getBuffer(), 0, size);
      RpcStat.readBytes.addAndGet(uncompressedLength);
      readCompressBuffer = resizeCompressBuf(uncompressedLength, readCompressBuffer);
      uncompress(readBuffer.getBuffer(), 0, size, readCompressBuffer.getByteBuffer().array(), 0);
      readCompressBuffer.getByteBuffer().limit(uncompressedLength);
      readCompressBuffer.getByteBuffer().position(0);

      readBuffer.fill(readCompressBuffer, uncompressedLength);
    } catch (IOException e) {
      throw new TTransportException(e);
    }
  }

  private TByteBuffer resizeCompressBuf(int size, TByteBuffer byteBuffer) {
    double expandFactor = 1.5;
    double loadFactor = 0.6;
    if (byteBuffer.getByteBuffer().capacity() < size) {
      int newCap = (int) Math.min(size * expandFactor, maxLength);
      byteBuffer = new TByteBuffer(ByteBuffer.allocate(newCap));
    } else if (byteBuffer.getByteBuffer().capacity() * loadFactor > size) {
      byteBuffer = new TByteBuffer(ByteBuffer.allocate(size));
    }
    return byteBuffer;
  }

  @Override
  public void flush() throws TTransportException {
    int length = writeBuffer.getPos();
    RpcStat.writeBytes.addAndGet(length);
    try {
      int maxCompressedLength = maxCompressedLength(length);
      writeCompressBuffer = resizeCompressBuf(maxCompressedLength, writeCompressBuffer);
      int compressedLength = compress(writeBuffer.getBuf().array(), 0, length,
          writeCompressBuffer.getByteBuffer().array(), 0);
      RpcStat.writeCompressedBytes.addAndGet(compressedLength);
      TFramedTransport.encodeFrameSize(compressedLength, i32buf);
      underlying.write(i32buf, 0, 4);

      underlying.write(writeCompressBuffer.getByteBuffer().array(), 0, compressedLength);
    } catch (IOException e) {
      throw new TTransportException(e);
    }

    writeBuffer.reset();
    if (maxLength < length) {
      writeBuffer.resizeIfNecessary(maxLength);
    }
    underlying.flush();
  }

  protected abstract int uncompressedLength(byte[] but, int off, int len) throws IOException;

  protected abstract int maxCompressedLength(int len);

  protected abstract int compress(byte[] input, int inOff, int len, byte[] output,
      int outOff) throws IOException;

  protected abstract void uncompress(byte[] input, int inOff, int size, byte[] output,
      int outOff) throws IOException;
}
