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

package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.commons.consensus.DataRegionId;

/** Time partition info records necessary info of a time partition for a data region */
public class TimePartitionInfo {
  DataRegionId dataRegionId;

  long partitionId;

  boolean isActive;

  long lastSystemFlushTime;

  boolean isLatestPartition;

  long memSize;

  public TimePartitionInfo(
      DataRegionId dataRegionId,
      long partitionId,
      boolean isActive,
      long lastSystemFlushTime,
      long memsize,
      boolean isLatestPartition) {
    this.dataRegionId = dataRegionId;
    this.partitionId = partitionId;
    this.isActive = isActive;
    this.lastSystemFlushTime = lastSystemFlushTime;
    this.memSize = memsize;
    this.isLatestPartition = isLatestPartition;
  }

  public int comparePriority(TimePartitionInfo timePartitionInfo) {
    int cmp = Boolean.compare(isActive, timePartitionInfo.isActive);
    if (cmp != 0) {
      return cmp;
    }

    cmp = Boolean.compare(isLatestPartition, timePartitionInfo.isLatestPartition);
    if (cmp != 0) {
      return cmp;
    }

    return Long.compare(lastSystemFlushTime, timePartitionInfo.lastSystemFlushTime);
  }
}
