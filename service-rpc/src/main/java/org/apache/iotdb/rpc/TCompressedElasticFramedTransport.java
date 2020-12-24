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

  private static final long MIN_SHRINK_INTERVAL = 60_000L;
  private static final int MAX_BUFFER_OVERSIZE_TIME = 5;
  private long lastShrinkTime;
  private int bufTooLargeCounter = MAX_BUFFER_OVERSIZE_TIME;

  protected TCompressedElasticFramedTransport(TTransport underlying, int initialBufferCapacity,
      int maxSoftLength) {
    super(underlying, initialBufferCapacity, maxSoftLength);
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

  private TByteBuffer resizeCompressBuf(int size, TByteBuffer byteBuffer)
      throws TTransportException {
    if (size > RpcUtils.FRAME_HARD_MAX_LENGTH) {
      close();
      throw new TTransportException(TTransportException.CORRUPTED_DATA,
          "Frame size (" + size + ") larger than protect max length (" + RpcUtils.FRAME_HARD_MAX_LENGTH
              + ")!");
    }

    final int currentCapacity = byteBuffer.getByteBuffer().capacity();
    final double loadFactor = 0.6;
    if (currentCapacity < size) {
      // Increase by a factor of 1.5x
      int growCapacity = currentCapacity + (currentCapacity >> 1);
      int newCapacity = Math.max(growCapacity, size);
      byteBuffer = new TByteBuffer(ByteBuffer.allocate(newCapacity));
      bufTooLargeCounter = MAX_BUFFER_OVERSIZE_TIME;
    } else if (currentCapacity > maxSoftLength && currentCapacity * loadFactor > size
        && bufTooLargeCounter-- <= 0
        && System.currentTimeMillis() - lastShrinkTime > MIN_SHRINK_INTERVAL) {
      // do not shrink beneath the initial size and do not shrink too often
      byteBuffer = new TByteBuffer(ByteBuffer.allocate(size + (currentCapacity - size) / 2));
      lastShrinkTime = System.currentTimeMillis();
      bufTooLargeCounter = MAX_BUFFER_OVERSIZE_TIME;
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
    if (maxSoftLength < length) {
      writeBuffer.resizeIfNecessary(maxSoftLength);
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
