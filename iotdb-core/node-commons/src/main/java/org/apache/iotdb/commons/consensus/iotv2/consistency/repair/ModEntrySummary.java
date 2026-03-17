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

package org.apache.iotdb.commons.consensus.iotv2.consistency.repair;

/**
 * Summary of a mod file entry (deletion) exchanged during the NEGOTIATE_KEY_MAPPING phase. Contains
 * enough information to determine if a deletion covers a specific data point, along with the
 * ProgressIndex for causal ordering.
 */
public class ModEntrySummary {

  private final String devicePattern;
  private final String measurementPattern;
  private final long timeRangeStart;
  private final long timeRangeEnd;
  private final long progressIndex;

  public ModEntrySummary(
      String devicePattern,
      String measurementPattern,
      long timeRangeStart,
      long timeRangeEnd,
      long progressIndex) {
    this.devicePattern = devicePattern;
    this.measurementPattern = measurementPattern;
    this.timeRangeStart = timeRangeStart;
    this.timeRangeEnd = timeRangeEnd;
    this.progressIndex = progressIndex;
  }

  /**
   * Check if this deletion covers a specific data point.
   *
   * @param deviceId the device ID of the data point
   * @param measurement the measurement name
   * @param timestamp the timestamp of the data point
   * @return true if this deletion covers the specified data point
   */
  public boolean covers(String deviceId, String measurement, long timestamp) {
    if (timestamp < timeRangeStart || timestamp > timeRangeEnd) {
      return false;
    }
    return matchesPattern(devicePattern, deviceId)
        && matchesPattern(measurementPattern, measurement);
  }

  private boolean matchesPattern(String pattern, String value) {
    if (pattern == null || pattern.equals("*") || pattern.equals("**")) {
      return true;
    }
    return pattern.equals(value);
  }

  public String getDevicePattern() {
    return devicePattern;
  }

  public String getMeasurementPattern() {
    return measurementPattern;
  }

  public long getTimeRangeStart() {
    return timeRangeStart;
  }

  public long getTimeRangeEnd() {
    return timeRangeEnd;
  }

  public long getProgressIndex() {
    return progressIndex;
  }

  @Override
  public String toString() {
    return String.format(
        "ModEntrySummary{device=%s, meas=%s, range=[%d,%d], pi=%d}",
        devicePattern, measurementPattern, timeRangeStart, timeRangeEnd, progressIndex);
  }
}
