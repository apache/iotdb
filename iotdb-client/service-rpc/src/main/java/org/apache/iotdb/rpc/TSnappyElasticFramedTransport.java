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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class TSnappyElasticFramedTransport extends TCompressedElasticFramedTransport {
  private static final Logger LOGGER = LoggerFactory.getLogger(TSnappyElasticFramedTransport.class);
  private static final AtomicLong totalUncompressionTime = new AtomicLong(0);
  private static final AtomicLong totalUncompressionCount = new AtomicLong(0);
  private static final AtomicLong totalOriginalDataSize = new AtomicLong(0);
  private static final AtomicLong totalCompressedDataSize = new AtomicLong(0);

  public static class Factory extends TElasticFramedTransport.Factory {

    public Factory() {
      this(RpcUtils.THRIFT_DEFAULT_BUF_CAPACITY, RpcUtils.THRIFT_FRAME_MAX_SIZE);
    }

    public Factory(int thriftDefaultBufferSize) {
      this(thriftDefaultBufferSize, RpcUtils.THRIFT_FRAME_MAX_SIZE);
    }

    public Factory(int thriftDefaultBufferSize, int thriftMaxFrameSize) {
      super(thriftDefaultBufferSize, thriftMaxFrameSize);
    }

    @Override
    public TTransport getTransport(TTransport trans) {
      return new TSnappyElasticFramedTransport(trans, thriftDefaultBufferSize, thriftMaxFrameSize);
    }
  }

  public TSnappyElasticFramedTransport(TTransport underlying) {
    this(underlying, RpcUtils.THRIFT_DEFAULT_BUF_CAPACITY, RpcUtils.THRIFT_FRAME_MAX_SIZE);
  }

  public TSnappyElasticFramedTransport(
      TTransport underlying, int thriftDefaultBufferSize, int thriftMaxFrameSize) {
    super(underlying, thriftDefaultBufferSize, thriftMaxFrameSize);
  }

  @Override
  protected int uncompressedLength(byte[] buf, int off, int len) throws IOException {
    return Snappy.uncompressedLength(buf, off, len);
  }

  @Override
  protected int maxCompressedLength(int len) {
    return Snappy.maxCompressedLength(len);
  }

  @Override
  protected int compress(byte[] input, int inOff, int len, byte[] output, int outOff)
      throws IOException {
    return Snappy.compress(input, inOff, len, output, outOff);
  }

  @Override
  protected void uncompress(byte[] input, int inOff, int size, byte[] output, int outOff)
      throws IOException {

    long startTime = System.nanoTime();
    int uncompressedSize = Snappy.uncompress(input, inOff, size, output, outOff);
    long endTime = System.nanoTime();
    totalUncompressionTime.addAndGet(endTime - startTime);
    totalOriginalDataSize.addAndGet(uncompressedSize);
    totalCompressedDataSize.addAndGet(size);
    long count = totalUncompressionCount.incrementAndGet();
    if (count % 1000 == 0) {
      LOGGER.info(
          "Average uncompression time: {} ms, average compression rate: {}",
          totalUncompressionTime.doubleValue() / count / 1000000,
          totalOriginalDataSize.doubleValue() / totalCompressedDataSize.doubleValue());
      totalUncompressionCount.set(0);
      totalUncompressionTime.set(0);
      totalOriginalDataSize.set(0);
      totalCompressedDataSize.set(0);
    }
  }
}
