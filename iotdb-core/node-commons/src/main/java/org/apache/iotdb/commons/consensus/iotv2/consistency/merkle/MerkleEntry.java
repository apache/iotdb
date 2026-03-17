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

package org.apache.iotdb.commons.consensus.iotv2.consistency.merkle;

/**
 * A single entry in a .merkle file representing the hash of a (device, measurement, timeBucket)
 * slice.
 */
public class MerkleEntry {

  private final String deviceId;
  private final String measurement;
  private final long timeBucketStart;
  private final long timeBucketEnd;
  private final int pointCount;
  private final long entryHash;

  public MerkleEntry(
      String deviceId,
      String measurement,
      long timeBucketStart,
      long timeBucketEnd,
      int pointCount,
      long entryHash) {
    this.deviceId = deviceId;
    this.measurement = measurement;
    this.timeBucketStart = timeBucketStart;
    this.timeBucketEnd = timeBucketEnd;
    this.pointCount = pointCount;
    this.entryHash = entryHash;
  }

  public String getDeviceId() {
    return deviceId;
  }

  public String getMeasurement() {
    return measurement;
  }

  public long getTimeBucketStart() {
    return timeBucketStart;
  }

  public long getTimeBucketEnd() {
    return timeBucketEnd;
  }

  public int getPointCount() {
    return pointCount;
  }

  public long getEntryHash() {
    return entryHash;
  }
}
