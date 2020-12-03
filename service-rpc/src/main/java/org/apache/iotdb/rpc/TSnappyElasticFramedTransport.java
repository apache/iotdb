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
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.xerial.snappy.Snappy;

public class TSnappyElasticFramedTransport extends TCompressedElasticFramedTransport {

  /**
   * How big should the default read and write buffers be?
   */
  public static final int DEFAULT_BUF_CAPACITY = 4 * 1024 * 1024;
  /**
   * How big is the largest allowable frame? Defaults to 16MB.
   */
  public static final int DEFAULT_MAX_LENGTH = 16384000;

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
      return new TSnappyElasticFramedTransport(trans, initialCapacity, maxLength);
    }
  }

  public TSnappyElasticFramedTransport(TTransport underlying) {
    this(underlying, DEFAULT_BUF_CAPACITY, DEFAULT_MAX_LENGTH);
  }

  public TSnappyElasticFramedTransport(TTransport underlying, int initialBufferCapacity, int maxLength) {
    super(underlying, initialBufferCapacity, maxLength);
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
    Snappy.uncompress(input, inOff, size, output, outOff);
  }

}
