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

package org.apache.iotdb.commons.consensus.iotv2.consistency.ibf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

/**
 * Temporary in-memory index built during IBF construction that maps composite keys back to the
 * original (deviceId, measurement, timestamp) coordinates. This is the critical bridge between IBF
 * decoding (which recovers composite keys) and repair execution (which needs actual data point
 * identifiers).
 *
 * <p>The index is built from the negotiated device/measurement mapping and discarded after repair.
 */
public class RowRefIndex {

  private final List<String> deviceIdByIndex;
  private final List<List<String>> measurementsByDevice;
  private final long timeBucketStart;
  private final long timestampResolution;

  private RowRefIndex(
      List<String> deviceIdByIndex,
      List<List<String>> measurementsByDevice,
      long timeBucketStart,
      long timestampResolution) {
    this.deviceIdByIndex = deviceIdByIndex;
    this.measurementsByDevice = measurementsByDevice;
    this.timeBucketStart = timeBucketStart;
    this.timestampResolution = timestampResolution;
  }

  /**
   * Resolve a decoded composite key back to original data point coordinates.
   *
   * @param compositeKey the 8-byte composite key from IBF decoding
   * @return the resolved data point locator
   * @throws IndexOutOfBoundsException if indices are invalid
   */
  public DataPointLocator resolve(long compositeKey) {
    int deviceIdx = CompositeKeyCodec.extractDeviceIndex(compositeKey);
    int measIdx = CompositeKeyCodec.extractMeasurementIndex(compositeKey);
    long timestamp =
        CompositeKeyCodec.recoverTimestamp(compositeKey, timeBucketStart, timestampResolution);

    return new DataPointLocator(
        deviceIdByIndex.get(deviceIdx),
        measurementsByDevice.get(deviceIdx).get(measIdx),
        timestamp);
  }

  public List<String> getDeviceIdByIndex() {
    return Collections.unmodifiableList(deviceIdByIndex);
  }

  public List<List<String>> getMeasurementsByDevice() {
    return Collections.unmodifiableList(measurementsByDevice);
  }

  public long getTimeBucketStart() {
    return timeBucketStart;
  }

  public long getTimestampResolution() {
    return timestampResolution;
  }

  /** Estimate heap memory consumed by this index. */
  public int estimatedMemoryBytes() {
    int bytes = 64; // object overhead
    for (String deviceId : deviceIdByIndex) {
      bytes += 40 + deviceId.length() * 2;
    }
    for (List<String> measurements : measurementsByDevice) {
      bytes += 40;
      for (String m : measurements) {
        bytes += 40 + m.length() * 2;
      }
    }
    return bytes;
  }

  /** Builder for constructing RowRefIndex from device/measurement lists. */
  public static class Builder {
    private final TreeMap<String, List<String>> deviceMeasurements = new TreeMap<>();
    private long timeBucketStart = 0;
    private long timestampResolution = CompositeKeyCodec.DEFAULT_TIMESTAMP_RESOLUTION;

    public Builder addDevice(String deviceId, List<String> measurements) {
      List<String> sorted = new ArrayList<>(measurements);
      Collections.sort(sorted);
      deviceMeasurements.put(deviceId, sorted);
      return this;
    }

    public Builder setTimeBucketStart(long timeBucketStart) {
      this.timeBucketStart = timeBucketStart;
      return this;
    }

    public Builder setTimestampResolution(long timestampResolution) {
      this.timestampResolution = timestampResolution;
      return this;
    }

    /**
     * Build the RowRefIndex and return a mapping from (deviceId, measurement) to their ordinal
     * indices.
     */
    public RowRefIndex build() {
      List<String> deviceList = new ArrayList<>(deviceMeasurements.keySet());
      List<List<String>> measurementList = new ArrayList<>();
      for (String device : deviceList) {
        measurementList.add(deviceMeasurements.get(device));
      }
      return new RowRefIndex(deviceList, measurementList, timeBucketStart, timestampResolution);
    }

    /** Get the device index for a given deviceId. Returns -1 if not found. */
    public int getDeviceIndex(String deviceId) {
      int idx = 0;
      for (String d : deviceMeasurements.keySet()) {
        if (d.equals(deviceId)) {
          return idx;
        }
        idx++;
      }
      return -1;
    }

    /** Get the measurement index for a given device and measurement. Returns -1 if not found. */
    public int getMeasurementIndex(String deviceId, String measurement) {
      List<String> measurements = deviceMeasurements.get(deviceId);
      if (measurements == null) {
        return -1;
      }
      return Collections.binarySearch(measurements, measurement);
    }
  }
}
