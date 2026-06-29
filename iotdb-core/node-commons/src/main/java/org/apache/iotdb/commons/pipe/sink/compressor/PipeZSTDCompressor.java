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

import com.github.luben.zstd.Zstd;

import java.io.IOException;

public class PipeZSTDCompressor extends PipeCompressor {

  private final int compressionLevel;

  public PipeZSTDCompressor(int compressionLevel) {
    super(PipeCompressionType.ZSTD);
    this.compressionLevel = compressionLevel;
  }

  @Override
  public byte[] compress(byte[] data) throws IOException {
    return Zstd.compress(data, compressionLevel);
  }

  @Override
  public byte[] decompress(byte[] byteArray) {
    return Zstd.decompress(byteArray, (int) Zstd.decompressedSize(byteArray, 0, byteArray.length));
  }

  @Override
  public byte[] decompress(byte[] byteArray, int decompressedLength) {
    return Zstd.decompress(byteArray, decompressedLength);
  }
}
