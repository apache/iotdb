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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractDataSourceOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.TableAggregator;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class TableLastQueryOperator extends AbstractDataSourceOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableLastQueryOperator.class);

  private boolean finished = false;
  // TODO not need all table aggregators when match last cache
  private final List<TableAggregator> tableAggregators;
  private final List<ColumnSchema> groupingKeySchemas;

  private final List<DeviceEntry> deviceEntries;
  private int currentDeviceIndex;

  public TableLastQueryOperator(
      List<TableAggregator> tableAggregators,
      List<ColumnSchema> groupingKeySchemas,
      List<DeviceEntry> deviceEntries) {
    this.tableAggregators = tableAggregators;
    this.groupingKeySchemas = groupingKeySchemas;
    this.deviceEntries = deviceEntries;
  }

  @Override
  public boolean isFinished() throws Exception {
    if (!finished) {
      finished = !hasNextWithTimer();
    }
    return finished;
  }

  @Override
  public boolean hasNext() throws Exception {
    if (retainedTsBlock != null) {
      return true;
    }

    return currentDeviceIndex < deviceEntries.size();
  }

  @Override
  public TsBlock next() throws Exception {
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();

    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }

    while (!resultTsBlockBuilder.isFull()) {
      if (processFinished() || System.nanoTime() - start > maxRuntime) {
        break;
      }
    }

    if (resultTsBlockBuilder.isEmpty()) {
      return null;
    }

    buildResultTsBlock();
    return checkTsBlockSizeAndGetResult();
  }

  private boolean processFinished() {

    return true;
  }

  private void buildResultTsBlock() {
    resultTsBlock =
        resultTsBlockBuilder.build(
            new RunLengthEncodedColumn(
                TIME_COLUMN_TEMPLATE, resultTsBlockBuilder.getPositionCount()));
    resultTsBlockBuilder.reset();
  }

  @Override
  protected List<TSDataType> getResultDataTypes() {
    int groupingKeySize = groupingKeySchemas != null ? groupingKeySchemas.size() : 0;
    List<TSDataType> resultDataTypes = new ArrayList<>(groupingKeySize + tableAggregators.size());

    if (groupingKeySchemas != null) {
      for (int i = 0; i < groupingKeySchemas.size(); i++) {
        resultDataTypes.add(TSDataType.STRING);
      }
    }

    for (TableAggregator aggregator : tableAggregators) {
      resultDataTypes.add(aggregator.getType());
    }

    return resultDataTypes;
  }

  @Override
  public long calculateMaxPeekMemory() {
    // TODO
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
