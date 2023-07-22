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

package org.apache.iotdb.commons.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;

import java.security.SecureRandom;
import java.util.Objects;

public class DataPartitionEntry implements Comparable<DataPartitionEntry> {

  private final TSeriesPartitionSlot seriesPartitionSlot;
  private final TTimePartitionSlot timePartitionSlot;
  private final TConsensusGroupId dataRegionGroup;
  private final int weight;

  public DataPartitionEntry(
      TSeriesPartitionSlot seriesPartitionSlot,
      TTimePartitionSlot timePartitionSlot,
      TConsensusGroupId dataRegionGroup) {
    this.seriesPartitionSlot = seriesPartitionSlot;
    this.timePartitionSlot = timePartitionSlot;
    this.dataRegionGroup = dataRegionGroup;
    this.weight = new SecureRandom().nextInt();
  }

  public TSeriesPartitionSlot getSeriesPartitionSlot() {
    return seriesPartitionSlot;
  }

  public TTimePartitionSlot getTimePartitionSlot() {
    return timePartitionSlot;
  }

  public TConsensusGroupId getDataRegionGroup() {
    return dataRegionGroup;
  }

  @Override
  public int compareTo(DataPartitionEntry o) {
    // The timePartitionSlot will be in descending order
    // After invoke Collections.sort()
    if (!timePartitionSlot.equals(o.timePartitionSlot)) {
      return o.timePartitionSlot.compareTo(timePartitionSlot);
    }
    return Integer.compare(weight, o.weight);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataPartitionEntry that = (DataPartitionEntry) o;
    return weight == that.weight
        && seriesPartitionSlot.equals(that.seriesPartitionSlot)
        && timePartitionSlot.equals(that.timePartitionSlot)
        && dataRegionGroup.equals(that.dataRegionGroup);
  }

  @Override
  public int hashCode() {
    return Objects.hash(seriesPartitionSlot, timePartitionSlot, dataRegionGroup, weight);
  }
}
