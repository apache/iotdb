/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.pipe.sink.compressor;

import java.io.IOException;

public abstract class PipeCompressor {

  public enum PipeCompressionType {
    SNAPPY((byte) 0),
    GZIP((byte) 1),
    LZ4((byte) 2),
    ZSTD((byte) 3),
    LZMA2((byte) 4);

    final byte index;

    PipeCompressionType(byte index) {
      this.index = index;
    }

    public byte getIndex() {
      return index;
    }
  }

  private final PipeCompressionType compressionType;

  protected PipeCompressor(PipeCompressionType compressionType) {
    this.compressionType = compressionType;
  }

  public abstract byte[] compress(byte[] data) throws IOException;

  /**
   * Decompress the byte array to a byte array. NOTE: the length of the decompressed byte array is
   * not provided in this method, and some decompressors (LZ4) may construct large byte arrays,
   * leading to potential OOM.
   *
   * @param byteArray the byte array to be decompressed
   * @return the decompressed byte array
   * @throws IOException
   */
  public abstract byte[] decompress(byte[] byteArray) throws IOException;

  /**
   * Decompress the byte array to a byte array with a known length.
   *
   * @param byteArray the byte array to be decompressed
   * @param decompressedLength the length of the decompressed byte array
   * @return the decompressed byte array
   * @throws IOException
   */
  public abstract byte[] decompress(byte[] byteArray, int decompressedLength) throws IOException;

  public byte serialize() {
    return compressionType.getIndex();
  }
}
