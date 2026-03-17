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

import java.util.Objects;

/**
 * Locates a specific data point by its logical coordinates: (deviceId, measurement, timestamp).
 * Resolved from a composite key via the RowRefIndex.
 */
public class DataPointLocator {

  private final String deviceId;
  private final String measurement;
  private final long timestamp;

  public DataPointLocator(String deviceId, String measurement, long timestamp) {
    this.deviceId = deviceId;
    this.measurement = measurement;
    this.timestamp = timestamp;
  }

  public String getDeviceId() {
    return deviceId;
  }

  public String getMeasurement() {
    return measurement;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DataPointLocator)) return false;
    DataPointLocator that = (DataPointLocator) o;
    return timestamp == that.timestamp
        && Objects.equals(deviceId, that.deviceId)
        && Objects.equals(measurement, that.measurement);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deviceId, measurement, timestamp);
  }

  @Override
  public String toString() {
    return String.format("(%s, %s, %d)", deviceId, measurement, timestamp);
  }
}
