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

package org.apache.iotdb.db.storageengine.dataregion.consistency;

import org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleEntry;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.XxHash64;
import org.apache.iotdb.db.utils.EncryptDBUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Scans a TsFile and computes per-(device, measurement, timeBucket) hash entries for the .merkle
 * sidecar file. Each entry's hash is the XxHash64 of all sorted (timestamp, value) pairs in that
 * bucket.
 */
public class MerkleHashComputer {

  static final long DEFAULT_TIME_BUCKET_INTERVAL_MS = 3_600_000L; // 1 hour

  private MerkleHashComputer() {}

  /**
   * Scan a TsFile and compute all MerkleEntry hashes.
   *
   * @param tsFilePath absolute path to the TsFile
   * @return sorted list of MerkleEntries (sorted by device -> measurement -> timeBucketStart)
   */
  public static List<MerkleEntry> computeEntries(String tsFilePath) throws IOException {
    return computeEntries(tsFilePath, DEFAULT_TIME_BUCKET_INTERVAL_MS);
  }

  public static List<MerkleEntry> computeEntries(String tsFilePath, long timeBucketIntervalMs)
      throws IOException {
    // Accumulator: device -> measurement -> bucketStart -> BucketAccumulator
    TreeMap<String, TreeMap<String, TreeMap<Long, BucketAccumulator>>> accumulator =
        new TreeMap<>();

    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(
            tsFilePath, EncryptDBUtils.getFirstEncryptParamFromTSFilePath(tsFilePath))) {

      TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();
      while (deviceIterator.hasNext()) {
        Pair<IDeviceID, Boolean> devicePair = deviceIterator.next();
        IDeviceID deviceId = devicePair.left;
        String deviceIdStr = deviceId.toString();

        TreeMap<String, TreeMap<Long, BucketAccumulator>> deviceAccum =
            accumulator.computeIfAbsent(deviceIdStr, k -> new TreeMap<>());

        Iterator<Map<String, List<ChunkMetadata>>> measurementChunkIter =
            reader.getMeasurementChunkMetadataListMapIterator(deviceId);

        while (measurementChunkIter.hasNext()) {
          Map<String, List<ChunkMetadata>> measurementChunks = measurementChunkIter.next();
          for (Map.Entry<String, List<ChunkMetadata>> entry : measurementChunks.entrySet()) {
            String measurement = entry.getKey();
            // Skip empty or time-column measurements
            if (measurement.isEmpty()) {
              continue;
            }
            TreeMap<Long, BucketAccumulator> measAccum =
                deviceAccum.computeIfAbsent(measurement, k -> new TreeMap<>());

            for (ChunkMetadata chunkMeta : entry.getValue()) {
              TSDataType dataType = chunkMeta.getDataType();
              Chunk chunk = reader.readMemChunk(chunkMeta);
              ChunkReader chunkReader = new ChunkReader(chunk);

              while (chunkReader.hasNextSatisfiedPage()) {
                BatchData batchData = chunkReader.nextPageData();
                while (batchData.hasCurrent()) {
                  long timestamp = batchData.currentTime();
                  long bucketStart = (timestamp / timeBucketIntervalMs) * timeBucketIntervalMs;

                  BucketAccumulator bucket =
                      measAccum.computeIfAbsent(
                          bucketStart,
                          bs -> new BucketAccumulator(bs, bs + timeBucketIntervalMs, dataType));
                  bucket.addPoint(timestamp, batchData.currentValue());
                  batchData.next();
                }
              }
            }
          }
        }
      }
    }

    return flattenToEntries(accumulator);
  }

  private static List<MerkleEntry> flattenToEntries(
      TreeMap<String, TreeMap<String, TreeMap<Long, BucketAccumulator>>> accumulator) {
    List<MerkleEntry> entries = new ArrayList<>();
    for (Map.Entry<String, TreeMap<String, TreeMap<Long, BucketAccumulator>>> deviceEntry :
        accumulator.entrySet()) {
      String deviceId = deviceEntry.getKey();
      for (Map.Entry<String, TreeMap<Long, BucketAccumulator>> measEntry :
          deviceEntry.getValue().entrySet()) {
        String measurement = measEntry.getKey();
        for (BucketAccumulator bucket : measEntry.getValue().values()) {
          entries.add(
              new MerkleEntry(
                  deviceId,
                  measurement,
                  bucket.bucketStart,
                  bucket.bucketEnd,
                  bucket.pointCount,
                  bucket.computeHash()));
        }
      }
    }
    return entries;
  }

  /** Accumulates (timestamp, value) pairs in a time bucket and computes their combined hash. */
  static class BucketAccumulator {
    final long bucketStart;
    final long bucketEnd;
    final TSDataType dataType;
    int pointCount;
    final TreeMap<Long, Object> points;

    BucketAccumulator(long bucketStart, long bucketEnd, TSDataType dataType) {
      this.bucketStart = bucketStart;
      this.bucketEnd = bucketEnd;
      this.dataType = dataType;
      this.pointCount = 0;
      this.points = new TreeMap<>();
    }

    void addPoint(long timestamp, Object value) {
      points.put(timestamp, value);
      pointCount++;
    }

    long computeHash() {
      XxHash64 hasher = new XxHash64();
      byte[] buf = new byte[8];
      for (Map.Entry<Long, Object> entry : points.entrySet()) {
        longToBytes(entry.getKey(), buf);
        hasher.update(buf);
        byte[] valueBytes = valueToBytes(entry.getValue(), dataType);
        hasher.update(valueBytes);
      }
      return hasher.hash();
    }
  }

  static void longToBytes(long v, byte[] buf) {
    buf[0] = (byte) (v >>> 56);
    buf[1] = (byte) (v >>> 48);
    buf[2] = (byte) (v >>> 40);
    buf[3] = (byte) (v >>> 32);
    buf[4] = (byte) (v >>> 24);
    buf[5] = (byte) (v >>> 16);
    buf[6] = (byte) (v >>> 8);
    buf[7] = (byte) v;
  }

  static byte[] valueToBytes(Object value, TSDataType dataType) {
    if (value == null) {
      return new byte[0];
    }
    switch (dataType) {
      case BOOLEAN:
        return new byte[] {(byte) ((Boolean) value ? 1 : 0)};
      case INT32:
      case DATE:
        return intToBytes((Integer) value);
      case INT64:
      case TIMESTAMP:
        return longToBytesNew((Long) value);
      case FLOAT:
        return intToBytes(Float.floatToIntBits((Float) value));
      case DOUBLE:
        return longToBytesNew(Double.doubleToLongBits((Double) value));
      case TEXT:
      case STRING:
      case BLOB:
        if (value instanceof byte[]) {
          return (byte[]) value;
        }
        return value.toString().getBytes();
      default:
        return value.toString().getBytes();
    }
  }

  private static byte[] intToBytes(int v) {
    return new byte[] {(byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v};
  }

  private static byte[] longToBytesNew(long v) {
    byte[] buf = new byte[8];
    longToBytes(v, buf);
    return buf;
  }
}
