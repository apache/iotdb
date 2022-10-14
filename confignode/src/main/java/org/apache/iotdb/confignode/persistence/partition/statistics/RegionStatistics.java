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
package org.apache.iotdb.confignode.persistence.partition.statistics;

import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class RegionStatistics {

  // For confirm the leadership.
  // The Region who claim itself as the leader and
  // has the maximum versionTimestamp is considered as the true leader
  private long versionTimestamp;
  private boolean isLeader;

  private RegionStatus regionStatus;

  public RegionStatistics() {
    // Empty constructor
  }

  public RegionStatistics(long versionTimestamp, boolean isLeader, RegionStatus regionStatus) {
    this.versionTimestamp = versionTimestamp;
    this.isLeader = isLeader;
    this.regionStatus = regionStatus;
  }

  public long getVersionTimestamp() {
    return versionTimestamp;
  }

  public boolean isLeader() {
    return isLeader;
  }

  public RegionStatus getRegionStatus() {
    return regionStatus;
  }

  public void serialize(OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(versionTimestamp, stream);
    ReadWriteIOUtils.write(isLeader, stream);
    ReadWriteIOUtils.write(regionStatus.getStatus(), stream);
  }

  public void deserialize(ByteBuffer buffer) {
    this.versionTimestamp = buffer.getLong();
    this.isLeader = ReadWriteIOUtils.readBool(buffer);
    this.regionStatus = RegionStatus.parse(ReadWriteIOUtils.readString(buffer));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RegionStatistics that = (RegionStatistics) o;
    return versionTimestamp == that.versionTimestamp
        && isLeader == that.isLeader
        && regionStatus == that.regionStatus;
  }

  @Override
  public int hashCode() {
    return Objects.hash(versionTimestamp, isLeader, regionStatus);
  }
}
