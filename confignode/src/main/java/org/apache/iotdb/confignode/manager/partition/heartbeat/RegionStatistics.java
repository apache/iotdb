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
package org.apache.iotdb.confignode.manager.partition.heartbeat;

import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class RegionStatistics {

  private RegionStatus regionStatus;

  public RegionStatistics() {
    // Empty constructor
  }

  public RegionStatistics(RegionStatus regionStatus) {
    this.regionStatus = regionStatus;
  }

  public RegionStatus getRegionStatus() {
    return regionStatus;
  }

  public RegionStatistics deepCopy() {
    return new RegionStatistics(regionStatus);
  }

  public RegionHeartbeatSample convertToRegionHeartbeatSample() {
    long currentTime = System.currentTimeMillis();
    return new RegionHeartbeatSample(currentTime, currentTime, regionStatus);
  }

  public void serialize(OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(regionStatus.getStatus(), stream);
  }

  // Deserializer for snapshot
  public void deserialize(InputStream inputStream) throws IOException {
    this.regionStatus = RegionStatus.parse(ReadWriteIOUtils.readString(inputStream));
  }

  // Deserializer for consensus-write
  public void deserialize(ByteBuffer buffer) {
    this.regionStatus = RegionStatus.parse(ReadWriteIOUtils.readString(buffer));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RegionStatistics that = (RegionStatistics) o;
    return regionStatus == that.regionStatus;
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionStatus);
  }

  @Override
  public String toString() {
    return "RegionStatistics{" + "regionStatus=" + regionStatus + '}';
  }
}
