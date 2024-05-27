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

import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Binary;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DeviceRegionScanOperator extends AbstractRegionScanDataSourceOperator {
  private final RegionScanForActiveDeviceUtil regionScanUtil;
  private final Map<IDeviceID, Boolean> deviceToAlignedMap;
  private final OperatorContext context;
  // targetDevices is the devices that need to be checked
  private final Set<IDeviceID> targetDevices;
  private boolean finished = false;

  public DeviceRegionScanOperator(
      OperatorContext operatorContext,
      PlanNodeId sourceId,
      Map<IDeviceID, Boolean> deviceToAlignedMap,
      Filter timeFilter) {
    this.sourceId = sourceId;
    this.context = operatorContext;
    this.deviceToAlignedMap = deviceToAlignedMap;
    this.targetDevices = deviceToAlignedMap.keySet();
    this.regionScanUtil = new RegionScanForActiveDeviceUtil(timeFilter);
  }

  @Override
  public void initQueryDataSource(IQueryDataSource dataSource) {
    regionScanUtil.initQueryDataSource(dataSource);
    super.initQueryDataSource(dataSource);
  }

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }
    resultTsBlock = resultTsBlockBuilder.build();
    resultTsBlockBuilder.reset();
    return checkTsBlockSizeAndGetResult();
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
        if (regionScanUtil.isCurrentTsFileFinished()) {
          if (!regionScanUtil.nextTsFileHandle(targetDevices)) {
            // There is no more fileScanHandles in queryDataSource
            break;
          }
        }

        if (!regionScanUtil.filterChunkMetaData()) {
          // There is still some chunkMetaData in current TsFile
          continue;
        }

        if (!regionScanUtil.filterChunkData()) {
          // There is still some pageData in current TsFile
          continue;
        }

        updateActiveDevices();
        regionScanUtil.finishCurrentFile();

      } while (System.nanoTime() - start < maxRuntime && !resultTsBlockBuilder.isFull());

      finished =
          resultTsBlockBuilder.isEmpty()
              && (regionScanUtil.isCurrentTsFileFinished() || targetDevices.isEmpty());

      return !finished;
    } catch (IOException e) {
      throw new RuntimeException("Error happened while scanning the file", e);
    }
  }

  private void updateActiveDevices() {
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] columnBuilders = resultTsBlockBuilder.getValueColumnBuilders();

    List<IDeviceID> activeDevices = regionScanUtil.getActiveDevices();
    activeDevices.forEach(targetDevices::remove);

    for (IDeviceID deviceID : activeDevices) {
      timeColumnBuilder.writeLong(-1);
      columnBuilders[0].writeBinary(new Binary(deviceID.getBytes()));
      columnBuilders[1].writeBoolean(deviceToAlignedMap.get(deviceID));
      columnBuilders[2].appendNull();
      resultTsBlockBuilder.declarePosition();
    }
  }

  @Override
  public void close() throws Exception {
    // do nothing
  }

  @Override
  public boolean isFinished() throws Exception {
    return finished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return Math.max(
        maxReturnSize, TSFileDescriptor.getInstance().getConfig().getPageSizeInByte() * 3L);
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return calculateMaxPeekMemoryWithCounter() - calculateMaxReturnSize();
  }

  @Override
  protected List<TSDataType> getResultDataTypes() {
    return Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return context;
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
}
