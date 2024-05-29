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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Binary;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ActiveDeviceRegionScanOperator extends AbstractRegionScanDataSourceOperator {
  // The devices which need to be checked.
  private final Map<IDeviceID, Boolean> deviceToAlignedMap;

  public ActiveDeviceRegionScanOperator(
      OperatorContext operatorContext,
      PlanNodeId sourceId,
      Map<IDeviceID, Boolean> deviceToAlignedMap,
      Filter timeFilter) {
    this.sourceId = sourceId;
    this.operatorContext = operatorContext;
    this.deviceToAlignedMap = deviceToAlignedMap;
    this.regionScanUtil = new RegionScanForActiveDeviceUtil(timeFilter);
  }

  @Override
  public boolean hasNext() throws Exception {
    if (retainedTsBlock != null) {
      return true;
    }
    try {
      // start stopwatch
      long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
      long start = System.nanoTime();

      do {
        if (regionScanUtil.isCurrentTsFileFinished()
            && !((RegionScanForActiveDeviceUtil) regionScanUtil)
                .nextTsFileHandle(deviceToAlignedMap)) {
          // There is no more fileScanHandles in queryDataSource
          break;
        }

        // For filter method, it will return false if the phase of calculation is finished,
        // otherwise, it will return true to execute in the next loop.
        if (regionScanUtil.filterChunkMetaData()) {
          // There is still some chunkMetaData in current TsFile
          continue;
        }

        if (regionScanUtil.filterChunkData() && !regionScanUtil.isCurrentTsFileFinished()) {
          // There is still some pageData in current TsFile
          continue;
        }

        updateActiveDevices();
        regionScanUtil.finishCurrentFile();

      } while (System.nanoTime() - start < maxRuntime && !resultTsBlockBuilder.isFull());

      finished =
          resultTsBlockBuilder.isEmpty()
              && ((!regionScanUtil.hasMoreData() && regionScanUtil.isCurrentTsFileFinished())
                  || deviceToAlignedMap.isEmpty());

      return !finished;
    } catch (IOException e) {
      throw new IOException("Error happened while scanning active devices", e);
    }
  }

  private void updateActiveDevices() {
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] columnBuilders = resultTsBlockBuilder.getValueColumnBuilders();

    List<IDeviceID> activeDevices =
        ((RegionScanForActiveDeviceUtil) regionScanUtil).getActiveDevices();
    for (IDeviceID deviceID : activeDevices) {
      timeColumnBuilder.writeLong(-1);
      columnBuilders[0].writeBinary(new Binary(deviceID.getBytes()));
      columnBuilders[1].writeBinary(
          new Binary(
              String.valueOf(deviceToAlignedMap.get(deviceID)), TSFileConfig.STRING_CHARSET));
      columnBuilders[2].appendNull();
      resultTsBlockBuilder.declarePosition();
      deviceToAlignedMap.remove(deviceID);
    }
  }

  @Override
  protected List<TSDataType> getResultDataTypes() {
    return ColumnHeaderConstant.showDevicesColumnHeaders.stream()
        .map(ColumnHeader::getColumnType)
        .collect(Collectors.toList());
  }
}
