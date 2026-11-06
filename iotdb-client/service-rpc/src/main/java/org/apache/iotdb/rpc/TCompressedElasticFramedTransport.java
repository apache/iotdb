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

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;

import java.io.IOException;

public abstract class TCompressedElasticFramedTransport extends TElasticFramedTransport {

  private AutoScalingBufferWriteTransport writeCompressBuffer;
  private AutoScalingBufferReadTransport readCompressBuffer;

  protected TCompressedElasticFramedTransport(
      TTransport underlying,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize,
      boolean copyBinary)
      throws TTransportException {
    this(underlying, thriftDefaultBufferSize, thriftMaxFrameSize, copyBinary, Integer.MAX_VALUE);
  }

  /**
   * @param maxReusableBufferSize requests larger than this number of bytes use temporary arrays in
   *     all four buffers and reset their reusable storage to {@code thriftDefaultBufferSize};
   *     {@link Integer#MAX_VALUE} preserves the original resizing policy
   */
  protected TCompressedElasticFramedTransport(
      TTransport underlying,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize,
      boolean copyBinary,
      int maxReusableBufferSize)
      throws TTransportException {
    super(
        underlying, thriftDefaultBufferSize, thriftMaxFrameSize, copyBinary, maxReusableBufferSize);
    try {
      writeCompressBuffer =
          new AutoScalingBufferWriteTransport(thriftDefaultBufferSize, maxReusableBufferSize);
      readCompressBuffer =
          new AutoScalingBufferReadTransport(thriftDefaultBufferSize, maxReusableBufferSize);
    } catch (IOException e) {
      closeAllocatedBuffers();
      throw new TTransportException(e);
    }
  }

  @Override
  protected void closeAllocatedBuffers() {
    super.closeAllocatedBuffers();
    if (writeCompressBuffer != null) {
      writeCompressBuffer.close();
    }
    if (readCompressBuffer != null) {
      readCompressBuffer.close();
    }
  }

  @Override
  protected void readFrame() throws TTransportException {
    // Discard the previous frame's budget before reading the next frame header and payload.
    resetMessageSizeAndConsumedBytes();
    underlying.readAll(i32buf, 0, 4);
    int size = TFramedTransport.decodeFrameSize(i32buf);
    validateFrame(size);
    readBuffer.fill(underlying, size);
    // Bind subsequent protocol reads to the current compressed frame size.
    resetMessageSizeAndConsumedBytes(size);
    RpcStat.readCompressedBytes.addAndGet(size);
    try {
      int uncompressedLength = uncompressedLength(readBuffer.getBuffer(), 0, size);
      RpcStat.readBytes.addAndGet(uncompressedLength);
      readCompressBuffer.resizeIfNecessary(uncompressedLength);
      uncompress(readBuffer.getBuffer(), 0, size, readCompressBuffer.getBuffer(), 0);
      readCompressBuffer.limit(uncompressedLength);
      readCompressBuffer.position(0);
      readBuffer.fill(readCompressBuffer, uncompressedLength);
    } catch (IOException e) {
      throw new TTransportException(e);
    }
  }

  @Override
  public void flush() throws TTransportException {
    int length = writeBuffer.getPos();
    RpcStat.writeBytes.addAndGet(length);
    try {
      int maxCompressedLength = maxCompressedLength(length);
      writeCompressBuffer.resizeIfNecessary(maxCompressedLength);
      int compressedLength =
          compress(writeBuffer.getBuffer(), 0, length, writeCompressBuffer.getBuffer(), 0);
      checkWriteFrameSize(compressedLength);
      RpcStat.writeCompressedBytes.addAndGet(compressedLength);
      TFramedTransport.encodeFrameSize(compressedLength, i32buf);
      underlying.write(i32buf, 0, 4);
      underlying.write(writeCompressBuffer.getBuffer(), 0, compressedLength);
    } catch (IOException e) {
      throw new TTransportException(e);
    } finally {
      // The compressed output has been handed to the underlying transport, so do not retain it.
      writeCompressBuffer.reset();
    }

    try {
      writeBuffer.resetAndResize();
    } catch (IOException e) {
      throw new TTransportException(e);
    }
    underlying.flush();
  }

  protected abstract int uncompressedLength(byte[] but, int off, int len) throws IOException;

  protected abstract int maxCompressedLength(int len);

  protected abstract int compress(byte[] input, int inOff, int len, byte[] output, int outOff)
      throws IOException;

  protected abstract void uncompress(byte[] input, int inOff, int size, byte[] output, int outOff)
      throws IOException;
}
