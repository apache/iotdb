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

package org.apache.iotdb.tsfile.compress.btrblocks;

import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CompressionSampler {

  private static final Logger logger = LoggerFactory.getLogger(CompressionSampler.class);

  private static final int minSampleLength = 16;
  private List<CompressionType> compressionTypes;
  private double sampleRatio;
  private List<ICompressor> compressors;
  private ICompressor preferredCompressor = ICompressor.getCompressor(CompressionType.UNCOMPRESSED);
  private Random random;

  public CompressionSampler(List<CompressionType> compressionTypes, double sampleRatio) {
    this.compressionTypes = compressionTypes;
    this.sampleRatio = sampleRatio;
    this.compressors = new ArrayList<>(compressionTypes.size());

    this.random = new Random();

    for (CompressionType compressionType : compressionTypes) {
      compressors.add(ICompressor.getCompressor(compressionType));
    }
  }

  public ICompressor getPreferredCompressor() {
    return preferredCompressor;
  }

  public void sample(byte[] data) throws IOException {
    sample(data, 0, data.length);
  }

  public void sample(byte[] data, int offset, int length) throws IOException {
    int sampleLength = (int) (length * sampleRatio);
    if (sampleLength < minSampleLength) {
      // sample too small, use the default compressor
      return;
    }

    int sampleStartRange = offset + length - sampleLength;
    int sampleStart = random.nextInt(sampleStartRange);

    int smallestLength = length;

    for (int i = 0; i < compressionTypes.size(); i++) {
      ICompressor compressor = compressors.get(i);
      byte[] compressed = compressor.compress(data, sampleStart, sampleLength);
      int bytesAfterCompression = compressed.length;

      if (bytesAfterCompression < smallestLength) {
        smallestLength = bytesAfterCompression;
        preferredCompressor = compressor;
      }
    }
  }

  public int sample(byte[] data, int offset, int length, byte[] compressed) throws IOException {
    sample(data, offset, length);
    int compressLength = preferredCompressor.compress(data, offset, length, compressed);
    compressed[compressLength] = preferredCompressor.getType().serialize();
    return compressLength + 1;
  }

  public int sample(ByteBuffer data, ByteBuffer compressed) throws IOException {
    sample(data.array(), data.arrayOffset() + data.position(), data.remaining());
    int compressLength = preferredCompressor.compress(data, compressed);
    compressed.put(preferredCompressor.getType().serialize());
    return compressLength + 1;
  }

  public int getMaxBytesForCompression(int uncompressedDataSize) {
    int maxBytes = 0;
    for (ICompressor compressor : compressors) {
      maxBytes = Math.max(maxBytes, compressor.getMaxBytesForCompression(uncompressedDataSize));
    }
    // the last byte is for the real compression type
    return maxBytes + 1;
  }
}
