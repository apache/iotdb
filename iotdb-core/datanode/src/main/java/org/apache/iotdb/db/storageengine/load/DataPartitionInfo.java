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

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;

import java.util.Objects;

/**
 * LOAD partition key: immutable identifier of one staged partition file: (DataRegion, time
 * partition slot).
 */
final class DataPartitionInfo {

  private final DataRegion dataRegion;
  private final TTimePartitionSlot timePartitionSlot;

  DataPartitionInfo(DataRegion dataRegion, TTimePartitionSlot timePartitionSlot) {
    this.dataRegion = dataRegion;
    this.timePartitionSlot = timePartitionSlot;
  }

  DataRegion getDataRegion() {
    return dataRegion;
  }

  TTimePartitionSlot getTimePartitionSlot() {
    return timePartitionSlot;
  }

  @Override
  public String toString() {
    return String.join(
        IoTDBConstant.FILE_NAME_SEPARATOR,
        dataRegion.getDatabaseName(),
        dataRegion.getDataRegionIdString(),
        Long.toString(timePartitionSlot.getStartTime()));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataPartitionInfo that = (DataPartitionInfo) o;
    return Objects.equals(dataRegion, that.dataRegion)
        && timePartitionSlot.getStartTime() == that.timePartitionSlot.getStartTime();
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataRegion, timePartitionSlot.getStartTime());
  }
}
