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
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.utils.StorageEngineTimePartitionIterator;
import org.apache.iotdb.db.storageengine.dataregion.utils.TreeDiskUsageStatisticUtil;

import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class ShowDiskUsageOperator implements SourceOperator {

  private final OperatorContext operatorContext;
  private final PlanNodeId sourceId;
  private final PartialPath pathPattern;
  private final StorageEngineTimePartitionIterator timePartitionIterator;
  private TreeDiskUsageStatisticUtil statisticUtil;
  private boolean allConsumed = false;
  private long result = 0;

  public ShowDiskUsageOperator(
      OperatorContext operatorContext, PlanNodeId sourceId, PartialPath pathPattern) {
    this.operatorContext = operatorContext;
    this.sourceId = sourceId;
    this.pathPattern = pathPattern;
    this.timePartitionIterator =
        new StorageEngineTimePartitionIterator(
            Optional.of(
                dataRegion -> {
                  String databaseName = dataRegion.getDatabaseName();
                  return !PathUtils.isTableModelDatabase(databaseName)
                      && pathPattern.matchPrefixPath(new PartialPath(databaseName));
                }),
            Optional.empty());
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
        result += statisticUtil.getResult()[0];
        statisticUtil.close();
      }
      if (timePartitionIterator.next()) {
        DataRegion dataRegion = timePartitionIterator.currentDataRegion();
        long timePartition = timePartitionIterator.currentTimePartition();
        statisticUtil =
            new TreeDiskUsageStatisticUtil(
                dataRegion.getTsFileManager(), timePartition, pathPattern);
      } else {
        allConsumed = true;
      }
    } while (System.nanoTime() - start < maxRuntime && !allConsumed);

    if (!allConsumed) {
      return null;
    }
    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(1, DatasetHeaderFactory.getShowDiskUsageHeader().getRespDataTypes());
    tsBlockBuilder.getTimeColumnBuilder().writeLong(0);
    tsBlockBuilder.getValueColumnBuilders()[0].writeInt(
        IoTDBDescriptor.getInstance().getConfig().getDataNodeId());
    tsBlockBuilder.getValueColumnBuilders()[1].writeLong(result);
    tsBlockBuilder.declarePosition();
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
    return 0;
  }

  @Override
  public long calculateMaxReturnSize() {
    return 0;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0;
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
}
