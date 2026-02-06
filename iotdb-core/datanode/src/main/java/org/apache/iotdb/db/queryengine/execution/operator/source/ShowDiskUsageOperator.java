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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.utils.StorageEngineTimePartitionIterator;
import org.apache.iotdb.db.storageengine.dataregion.utils.TreeDiskUsageStatisticUtil;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class ShowDiskUsageOperator implements SourceOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ShowDiskUsageOperator.class);

  private final OperatorContext operatorContext;
  private final PlanNodeId sourceId;
  private final PartialPath pathPattern;
  private final StorageEngineTimePartitionIterator timePartitionIterator;
  private final PaginationController paginationController;
  private final TsBlockBuilder tsBlockBuilder =
      new TsBlockBuilder(DatasetHeaderFactory.getShowDiskUsageHeader().getRespDataTypes());
  private TreeDiskUsageStatisticUtil statisticUtil;
  private boolean allConsumed = false;

  public ShowDiskUsageOperator(
      OperatorContext operatorContext,
      PlanNodeId sourceId,
      PartialPath pathPattern,
      Filter pushDownFilter,
      PaginationController paginationController) {
    this.operatorContext = operatorContext;
    this.sourceId = sourceId;
    this.pathPattern = pathPattern;
    this.paginationController = paginationController;
    this.timePartitionIterator =
        new StorageEngineTimePartitionIterator(
            Optional.of(
                dataRegion -> {
                  String databaseName = dataRegion.getDatabaseName();
                  return !dataRegion.isTableModel()
                      && pathPattern.matchPrefixPath(new PartialPath(databaseName));
                }),
            Optional.of(
                (dataRegion, timePartition) -> {
                  if (pushDownFilter != null) {
                    Object[] row = new Object[4];
                    row[0] = new Binary(dataRegion.getDatabaseName(), TSFileConfig.STRING_CHARSET);
                    row[1] = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
                    row[2] = dataRegion.getDataRegionId().getId();
                    row[3] = timePartition;
                    if (!pushDownFilter.satisfyRow(0, row)) {
                      return false;
                    }
                  }
                  if (paginationController.hasCurOffset()) {
                    paginationController.consumeOffset();
                    return false;
                  }
                  return paginationController.hasCurLimit();
                }));
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() throws Exception {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();
    do {
      if (statisticUtil != null && statisticUtil.hasNextFile()) {
        statisticUtil.calculateNextFile();
        continue;
      }
      if (statisticUtil != null) {
        tsBlockBuilder.getTimeColumnBuilder().writeLong(0);
        tsBlockBuilder.getValueColumnBuilders()[0].writeBinary(
            new Binary(
                timePartitionIterator.currentDataRegion().getDatabaseName(),
                TSFileConfig.STRING_CHARSET));
        tsBlockBuilder.getValueColumnBuilders()[1].writeInt(
            IoTDBDescriptor.getInstance().getConfig().getDataNodeId());
        tsBlockBuilder.getValueColumnBuilders()[2].writeInt(
            Integer.parseInt(timePartitionIterator.currentDataRegion().getDataRegionIdString()));
        tsBlockBuilder.getValueColumnBuilders()[3].writeLong(
            timePartitionIterator.currentTimePartition());
        tsBlockBuilder.getValueColumnBuilders()[4].writeLong(statisticUtil.getResult()[0]);
        tsBlockBuilder.declarePosition();
        paginationController.consumeLimit();
        statisticUtil.close();
      }
      if (paginationController.hasCurLimit() && timePartitionIterator.nextTimePartition()) {
        DataRegion dataRegion = timePartitionIterator.currentDataRegion();
        long timePartition = timePartitionIterator.currentTimePartition();
        statisticUtil =
            new TreeDiskUsageStatisticUtil(
                dataRegion.getTsFileManager(),
                timePartition,
                pathPattern,
                Optional.ofNullable(operatorContext.getInstanceContext()));
      } else {
        allConsumed = true;
      }
    } while (System.nanoTime() - start < maxRuntime && !allConsumed);

    if (!allConsumed) {
      return null;
    }
    return tsBlockBuilder.build();
  }

  @Override
  public boolean hasNext() throws Exception {
    return !allConsumed;
  }

  @Override
  public void close() throws Exception {
    if (statisticUtil != null) {
      statisticUtil.close();
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return allConsumed;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
  }

  @Override
  public long calculateMaxReturnSize() {
    return TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + TreeDiskUsageStatisticUtil.SHALLOW_SIZE
        + RamUsageEstimator.sizeOfObject(timePartitionIterator)
        + MemoryEstimationHelper.getEstimatedSizeOfPartialPath(pathPattern)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sourceId);
  }
}
