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

import org.apache.tsfile.compress.ICompressor;
import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.file.metadata.enums.CompressionType;

import java.io.IOException;

public class PipeLZMA2Compressor extends PipeCompressor {

  private static final ICompressor COMPRESSOR = ICompressor.getCompressor(CompressionType.LZMA2);
  private static final IUnCompressor DECOMPRESSOR =
      IUnCompressor.getUnCompressor(CompressionType.LZMA2);

  public PipeLZMA2Compressor() {
    super(PipeCompressionType.LZMA2);
  }

  @Override
  public byte[] compress(byte[] data) throws IOException {
    return COMPRESSOR.compress(data);
  }

  @Override
  public byte[] decompress(byte[] byteArray) throws IOException {
    return DECOMPRESSOR.uncompress(byteArray);
  }

  @Override
  public byte[] decompress(byte[] byteArray, int decompressedLength) throws IOException {
    byte[] uncompressed = new byte[decompressedLength];
    DECOMPRESSOR.uncompress(byteArray, 0, byteArray.length, uncompressed, 0);
    return uncompressed;
  }
}
