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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.RamUsageEstimator;

import static com.google.common.base.Preconditions.checkArgument;

public class CountAllAccumulator implements TableAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(CountAllAccumulator.class);
  private long countState = 0;

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new CountAllAccumulator();
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    checkArgument(arguments.length == 1, "argument of COUNT should be one column");
    int count = mask.getSelectedPositionCount();
    countState += count;
  }

  @Override
  public void removeInput(Column[] arguments) {
    checkArgument(arguments.length == 1, "argument of COUNT should be one column");
    int count = arguments[0].getPositionCount();
    countState -= count;
  }

  @Override
  public void addIntermediate(Column argument) {
    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (argument.isNull(i)) {
        continue;
      }
      countState += argument.getLong(i);
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    columnBuilder.writeLong(countState);
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    columnBuilder.writeLong(countState);
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    throw new UnsupportedOperationException("CountAllAccumulator does not support statistics.");
  }

  @Override
  public void reset() {
    countState = 0;
  }

  @Override
  public boolean removable() {
    return true;
  }
}
