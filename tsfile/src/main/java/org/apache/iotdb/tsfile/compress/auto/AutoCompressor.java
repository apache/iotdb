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

package org.apache.iotdb.tsfile.compress.auto;

import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AutoCompressor implements ICompressor {

  private CompressionSampler sampler;

  public AutoCompressor() {
    List<CompressionType> compressionTypes = collectCompressionTypes();
    double alpha = 1.0;
    long minSampleIntervalMS = 1000;
    sampler = new CompressionSampler(compressionTypes, alpha, minSampleIntervalMS);
  }

  public AutoCompressor(double alpha, long minSampleIntervalMS) {
    List<CompressionType> compressionTypes = collectCompressionTypes();
    sampler = new CompressionSampler(compressionTypes, alpha, minSampleIntervalMS);
  }

  private static List<CompressionType> collectCompressionTypes() {
    List<CompressionType> compressionTypeList =
        new ArrayList<>(CompressionType.values().length - 1);
    for (CompressionType type : CompressionType.values()) {
      if (!type.equals(CompressionType.AUTO) && !type.equals(CompressionType.UNCOMPRESSED)) {
        compressionTypeList.add(type);
      }
    }
    return compressionTypeList;
  }

  @Override
  public byte[] compress(byte[] data) throws IOException {
    if (sampler.shouldSample()) {
      return sampler.sample(data);
    }
    ICompressor preferredSampler = sampler.getPreferredSampler();
    byte[] compress = preferredSampler.compress(data);
    byte[] result = new byte[compress.length + 1];
    System.arraycopy(compress, 0, result, 0, compress.length);
    // the last byte is for the real compression type
    result[compress.length] = preferredSampler.getType().serialize();
    return result;
  }

  @Override
  public byte[] compress(byte[] data, int offset, int length) throws IOException {
    if (sampler.shouldSample()) {
      return sampler.sample(data, offset, length);
    }
    ICompressor preferredSampler = sampler.getPreferredSampler();
    byte[] compress = preferredSampler.compress(data, offset, length);
    byte[] result = new byte[compress.length + 1];
    System.arraycopy(compress, 0, result, 0, compress.length);
    // the last byte is for the real compression type
    result[compress.length] = preferredSampler.getType().serialize();
    return result;
  }

  @Override
  public int compress(byte[] data, int offset, int length, byte[] compressed) throws IOException {
    if (sampler.shouldSample()) {
      return sampler.sample(data, offset, length, compressed);
    }
    ICompressor preferredSampler = sampler.getPreferredSampler();
    int compressedLength = preferredSampler.compress(data, offset, length, compressed);
    // the last byte is for the real compression type
    compressed[compressedLength] = preferredSampler.getType().serialize();
    return compressedLength + 1;
  }

  @Override
  public int compress(ByteBuffer data, ByteBuffer compressed) throws IOException {
    if (sampler.shouldSample()) {
      return sampler.sample(data, compressed);
    }
    ICompressor preferredSampler = sampler.getPreferredSampler();
    int compressedLength = preferredSampler.compress(data, compressed);
    // the last byte is for the real compression type
    compressed.mark();
    compressed.position(compressed.position() + compressedLength);
    compressed.put(preferredSampler.getType().serialize());
    compressed.reset();
    return compressedLength + 1;
  }

  @Override
  public int getMaxBytesForCompression(int uncompressedDataSize) {
    return sampler.getMaxBytesForCompression(uncompressedDataSize);
  }

  @Override
  public CompressionType getType() {
    return CompressionType.AUTO;
  }
}
