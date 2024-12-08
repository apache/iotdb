/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.partition;

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.List;

// todo replace this data structure with PartitionTable
public abstract class Partition {
  protected String seriesSlotExecutorName;
  protected int seriesPartitionSlotNum;

  // todo decouple this executor with Partition
  private final SeriesPartitionExecutor executor;

  protected Partition(String seriesSlotExecutorName, int seriesPartitionSlotNum) {
    this.seriesSlotExecutorName = seriesSlotExecutorName;
    this.seriesPartitionSlotNum = seriesPartitionSlotNum;
    executor =
        SeriesPartitionExecutor.getSeriesPartitionExecutor(
            seriesSlotExecutorName, seriesPartitionSlotNum);
  }

  public TSeriesPartitionSlot calculateDeviceGroupId(IDeviceID deviceID) {
    return executor.getSeriesPartitionSlot(deviceID);
  }

  public abstract List<RegionReplicaSetInfo> getDistributionInfo();

  public abstract boolean isEmpty();
}
