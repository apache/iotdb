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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class CompressionSampler {

  private static final Logger logger = LoggerFactory.getLogger(CompressionSampler.class);

  private List<CompressionType> compressionTypes;
  private long minSampleInterval;
  private long lastSampleTimeMS;
  private List<ICompressor> compressors;
  private List<CompressionMonitor> monitors;
  private int preferredCompressorIndex;

  public CompressionSampler(
      List<CompressionType> compressionTypes, double alpha, long minSampleInterval) {
    this.compressionTypes = compressionTypes;
    this.minSampleInterval = minSampleInterval;
    this.monitors = new ArrayList<>(compressionTypes.size());
    this.compressors = new ArrayList<>(compressionTypes.size());

    int maxSampleNum = 10;

    for (CompressionType compressionType : compressionTypes) {
      monitors.add(new CompressionMonitor(maxSampleNum, alpha));
      compressors.add(ICompressor.getCompressor(compressionType));
    }
  }

  public boolean shouldSample() {
    return System.currentTimeMillis() - lastSampleTimeMS >= minSampleInterval;
  }

  public ICompressor getPreferredSampler() {
    return compressors.get(preferredCompressorIndex);
  }

  public byte[] sample(byte[] data) throws IOException {
    return sample(data, 0, data.length);
  }

  public byte[] sample(byte[] data, int offset, int length) throws IOException {
    CompressionType bestType = CompressionType.UNCOMPRESSED;
    int smallestLength = length;
    byte[] bestResult = data;

    for (int i = 0; i < compressionTypes.size(); i++) {
      ICompressor compressor = compressors.get(i);
      CompressionMonitor monitor = monitors.get(i);
      long startTime = System.currentTimeMillis();
      byte[] compressed = compressor.compress(data, offset, length);
      int bytesBeforeCompression = data.length;
      int bytesAfterCompression = compressed.length;
      long timeConsumption = System.currentTimeMillis() - startTime;
      monitor.addSample(bytesBeforeCompression, bytesAfterCompression, timeConsumption);

      if (bytesAfterCompression < smallestLength) {
        smallestLength = bytesAfterCompression;
        bestType = compressionTypes.get(i);
        bestResult = compressed;
      }
    }

    lastSampleTimeMS = System.currentTimeMillis();
    updatePreferredIndex();

    // the last byte is for the real compression type
    byte[] result = new byte[bestResult.length + 1];
    System.arraycopy(bestResult, 0, result, 0, bestResult.length);
    result[bestResult.length] = bestType.serialize();
    return result;
  }

  public int sample(byte[] data, int offset, int length, byte[] compressed) throws IOException {
    CompressionType bestType = CompressionType.UNCOMPRESSED;
    int smallestLength = length;

    for (int i = 0; i < compressionTypes.size(); i++) {
      ICompressor compressor = compressors.get(i);
      CompressionMonitor monitor = monitors.get(i);
      long startTime = System.currentTimeMillis();
      int bytesAfterCompression = compressor.compress(data, offset, length, compressed);
      int bytesBeforeCompression = data.length;
      long timeConsumption = System.currentTimeMillis() - startTime;
      monitor.addSample(bytesBeforeCompression, bytesAfterCompression, timeConsumption);

      if (bytesAfterCompression < smallestLength) {
        smallestLength = bytesAfterCompression;
        bestType = compressionTypes.get(i);
      }
    }

    lastSampleTimeMS = System.currentTimeMillis();
    updatePreferredIndex();

    // the last byte is for the real compression type
    compressed[smallestLength] = bestType.serialize();
    return smallestLength + 1;
  }

  public int sample(ByteBuffer data, ByteBuffer compressed) throws IOException {
    CompressionType bestType = CompressionType.UNCOMPRESSED;
    int smallestLength = data.remaining();

    for (int i = 0; i < compressionTypes.size(); i++) {
      ICompressor compressor = compressors.get(i);
      CompressionMonitor monitor = monitors.get(i);
      long startTime = System.currentTimeMillis();
      int bytesAfterCompression = compressor.compress(data, compressed);
      int bytesBeforeCompression = data.remaining();
      long timeConsumption = System.currentTimeMillis() - startTime;
      monitor.addSample(bytesBeforeCompression, bytesAfterCompression, timeConsumption);

      if (bytesAfterCompression < smallestLength) {
        smallestLength = bytesAfterCompression;
        bestType = compressionTypes.get(i);
      }
    }

    lastSampleTimeMS = System.currentTimeMillis();
    updatePreferredIndex();

    // the last byte is for the real compression type
    compressed.mark();
    compressed.position(compressed.position() + smallestLength);
    compressed.put(bestType.serialize());
    compressed.reset();
    return smallestLength + 1;
  }

  public int getMaxBytesForCompression(int uncompressedDataSize) {
    int maxBytes = 0;
    for (ICompressor compressor : compressors) {
      maxBytes = Math.max(maxBytes, compressor.getMaxBytesForCompression(uncompressedDataSize));
    }
    // the last byte is for the real compression type
    return maxBytes + 1;
  }

  private void updatePreferredIndex() {
    double bestScore = 0;
    int prevIndex = preferredCompressorIndex;
    for (int i = 0; i < monitors.size(); i++) {
      double score = monitors.get(i).score();
      if (score > bestScore) {
        preferredCompressorIndex = i;
      }
    }
    if (prevIndex != preferredCompressorIndex) {
      logger.info("Preferred compressor changed to {}", compressors.get(preferredCompressorIndex));
    }
  }

  private static class CompressionSample {

    private long bytesBeforeCompression;
    private long bytesAfterCompression;
    private long timeConsumptionNS;

    public CompressionSample(
        long bytesBeforeCompression, long bytesAfterCompression, long timeConsumptionNS) {
      this.bytesBeforeCompression = bytesBeforeCompression;
      this.bytesAfterCompression = bytesAfterCompression;
      this.timeConsumptionNS = timeConsumptionNS;
    }
  }

  private static class CompressionMonitor {

    private Queue<CompressionSample> samples;
    private int maxSampleNum;
    private double alpha;
    private long bytesBeforeCompressionSum;
    private long bytesAfterCompressionSum;
    private long timeConsumptionSumNS;

    private CompressionMonitor(int maxSampleNum, double alpha) {
      this.maxSampleNum = maxSampleNum;
      this.samples = new ArrayDeque<>(maxSampleNum);
      this.alpha = alpha;
    }

    private double compressionRatio() {
      return bytesAfterCompressionSum * 1.0 / bytesBeforeCompressionSum;
    }

    private double throughput() {
      return bytesBeforeCompressionSum * 1.0 / timeConsumptionSumNS;
    }

    private double score() {
      return Math.pow(throughput(), alpha) / compressionRatio();
    }

    private void addSample(
        long bytesBeforeCompression, long bytesAfterCompression, long timeConsumptionNS) {
      CompressionSample sample =
          new CompressionSample(bytesBeforeCompression, bytesAfterCompression, timeConsumptionNS);
      if (samples.size() < maxSampleNum) {
        addSample(sample);
      } else {
        removeSample();
      }
    }

    private void addSample(CompressionSample sample) {
      bytesAfterCompressionSum += sample.bytesAfterCompression;
      bytesBeforeCompressionSum += sample.bytesBeforeCompression;
      timeConsumptionSumNS += sample.timeConsumptionNS;
      samples.add(sample);
    }

    private void removeSample() {
      CompressionSample sample = samples.remove();
      bytesBeforeCompressionSum -= sample.bytesBeforeCompression;
      bytesAfterCompressionSum -= sample.bytesAfterCompression;
      timeConsumptionSumNS -= sample.timeConsumptionNS;
    }
  }
}
