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

package org.apache.iotdb.calc.execution.operator.process;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.plan.planner.CommonOperatorUtils;
import org.apache.iotdb.commons.queryengine.execution.MemoryEstimationHelper;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.concurrent.TimeUnit;

/** A bounded, row-major expansion of a single input block at a time. */
public final class UnpivotOperator implements ProcessOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(UnpivotOperator.class);

  private final CommonOperatorContext context;
  private final Operator child;
  private final int[] retained;
  private final int[][] sources;
  private final Binary[] names;
  private final boolean includeNulls;
  private final List<TSDataType> outputTypes;
  private TsBlock input;
  private int row;
  private int source;

  public UnpivotOperator(
      CommonOperatorContext context,
      Operator child,
      int[] retained,
      int[][] sources,
      List<String> names,
      boolean includeNulls,
      List<TSDataType> outputTypes) {
    if (sources.length == 0
        || sources.length != names.size()
        || sources[0].length == 0
        || outputTypes.size() != retained.length + sources[0].length + 1) {
      throw new IllegalArgumentException();
    }
    for (int[] group : sources) {
      if (group.length != sources[0].length) {
        throw new IllegalArgumentException();
      }
    }
    this.context = context;
    this.child = child;
    this.retained = retained.clone();
    this.sources = new int[sources.length][];
    for (int i = 0; i < sources.length; i++) {
      this.sources[i] = sources[i].clone();
    }
    this.names =
        names.stream()
            .map(name -> new Binary(name, TSFileConfig.STRING_CHARSET))
            .toArray(Binary[]::new);
    this.includeNulls = includeNulls;
    this.outputTypes = List.copyOf(outputTypes);
  }

  @Override
  public CommonOperatorContext getOperatorContext() {
    return context;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return input != null && row < input.getPositionCount() ? NOT_BLOCKED : child.isBlocked();
  }

  @Override
  public TsBlock next() throws Exception {
    TsBlockBuilder output = new TsBlockBuilder(outputTypes);
    long startTime = System.nanoTime();
    long maxRuntime = context.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    boolean processed = false;
    while (!output.isFull()) {
      if (processed && System.nanoTime() - startTime >= maxRuntime) {
        break;
      }
      processed = true;
      if (input == null || row >= input.getPositionCount()) {
        input = null;
        if (!child.hasNextWithTimer()) {
          break;
        }
        input = child.nextWithTimer();
        row = 0;
        source = 0;
        if (input == null) {
          break;
        }
        if (input.isEmpty()) {
          input = null;
          break;
        }
      }
      boolean valid = true;
      if (!includeNulls) {
        for (int channel : sources[source]) {
          if (input.getColumn(channel).isNull(row)) {
            valid = false;
            break;
          }
        }
      }
      if (valid) {
        int target = 0;
        for (int channel : retained) {
          copy(input.getColumn(channel), row, output, target++);
        }
        output.getColumnBuilder(target++).writeBinary(names[source]);
        for (int channel : sources[source]) {
          copy(input.getColumn(channel), row, output, target++);
        }
        output.declarePosition();
      }
      if (++source == sources.length) {
        source = 0;
        row++;
      }
    }
    if (output.isEmpty()) {
      return null;
    }
    return output.build(
        new RunLengthEncodedColumn(
            CommonOperatorUtils.TIME_COLUMN_TEMPLATE, output.getPositionCount()));
  }

  private static void copy(Column column, int row, TsBlockBuilder output, int channel) {
    if (column.isNull(row)) {
      output.getColumnBuilder(channel).appendNull();
    } else {
      output.getColumnBuilder(channel).write(column, row);
    }
  }

  @Override
  public boolean hasNext() throws Exception {
    return (input != null && row < input.getPositionCount()) || child.hasNextWithTimer();
  }

  @Override
  public boolean isFinished() throws Exception {
    return (input == null || row >= input.getPositionCount()) && child.isFinished();
  }

  @Override
  public void close() throws Exception {
    input = null;
    child.close();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return child.calculateMaxPeekMemoryWithCounter() + child.calculateMaxReturnSize();
  }

  @Override
  public long calculateMaxReturnSize() {
    return child.calculateMaxReturnSize();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return child.calculateRetainedSizeAfterCallingNext() + child.calculateMaxReturnSize();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(context);
  }
}
