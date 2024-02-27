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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class AggregationMergeSortOperator extends AbstractConsumeAllOperator {

  // private final ITimeRangeIterator timeRangeIterator;

  // Current interval of aggregation window [curStartTime, curEndTime)
  private TimeRange curTimeRange;

  private final List<TSDataType> dataTypes;
  private final TsBlockBuilder tsBlockBuilder;

  private final boolean[] noMoreTsBlocks;

  private boolean finished;

  private boolean currentFinished;

  private String currentDevice;

  private long currentTime;

  public AggregationMergeSortOperator(
      OperatorContext operatorContext, List<Operator> children, List<TSDataType> dataTypes) {
    super(operatorContext, children);
    this.dataTypes = dataTypes;
    this.tsBlockBuilder = new TsBlockBuilder(dataTypes);
    this.noMoreTsBlocks = new boolean[this.inputOperatorsCount];
  }

  @Override
  public TsBlock next() throws Exception {
    long startTime = System.nanoTime();
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);

    // 1. fill consumed up TsBlock
    if (!prepareInput()) {
      return null;
    }

    tsBlockBuilder.reset();
    TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (TsBlock tsBlock : inputTsBlocks) {
      timeBuilder.writeLong(tsBlock.getTimeColumn().getLong(0));
      valueColumnBuilders[0].writeBinary(tsBlock.getValueColumns()[0].getBinary(0));
    }

    return tsBlockBuilder.build();
  }

  @Override
  public boolean hasNext() throws Exception {
    // TODO the child of DeviceViewNode already calc TimeRange?
    // return curTimeRange != null || timeRangeIterator.hasNextTimeRange();

    if (finished) {
      return false;
    }
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!isEmpty(i)) {
        return true;
      } else if (!noMoreTsBlocks[i]) {
        if (!canCallNext[i] || children.get(i).hasNextWithTimer()) {
          return true;
        } else {
          children.get(i).close();
          children.set(i, null);
          noMoreTsBlocks[i] = true;
          inputTsBlocks[i] = null;
        }
      }
    }
    return false;
  }

  @Override
  public boolean isFinished() throws Exception {
    if (finished) {
      return true;
    }
    finished = true;

    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!noMoreTsBlocks[i] || !isEmpty(i)) {
        finished = false;
        break;
      }
    }
    return finished;
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
}
