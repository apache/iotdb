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
package org.apache.iotdb.session.rpccompress;

import org.apache.tsfile.compress.ICompressor;
import org.apache.tsfile.file.metadata.enums.CompressionType;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RpcCompressor {

  public static ICompressor compressor;

  public RpcCompressor(CompressionType name) {
    compressor = ICompressor.getCompressor(name);
  }

  public ByteBuffer compress(ByteBuffer input) {
    byte[] src;
    int offset, length;

    if (input.hasArray()) {
      offset = input.arrayOffset() + input.position();
      length = input.remaining();
      if (offset == 0 && length == input.array().length) {
        src = input.array();
      } else {
        src = new byte[length];
        System.arraycopy(input.array(), offset, src, 0, length);
      }
    } else {
      length = input.remaining();
      src = new byte[length];
      input.slice().get(src);
    }

    byte[] compressed = null;
    try {
      compressed = compress(src);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return ByteBuffer.wrap(compressed);
  }

  public byte[] compress(byte[] data) throws IOException {
    return compressor.compress(data);
  }

  public byte[] compress(byte[] data, int offset, int length) throws IOException {
    return compressor.compress(data, offset, length);
  }

  public int compress(byte[] data, int offset, int length, byte[] compressed) throws IOException {
    return compressor.compress(data, offset, length, compressed);
  }

  /**
   * Compress ByteBuffer, this method is better for longer data
   *
   * @return byte length of compressed data.
   */
  public int compress(ByteBuffer data, ByteBuffer compressed) throws IOException {
    return compressor.compress(data, compressed);
  }

  public int getMaxBytesForCompression(int uncompressedDataSize) {
    return compressor.getMaxBytesForCompression(uncompressedDataSize);
  }

  public CompressionType getType() {
    return compressor.getType();
  }
}
