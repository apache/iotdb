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

package org.apache.iotdb.db.mpp.execution.operator.schema;

import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.ProcessOperator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class SchemaQueryOrderByHeatOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private boolean isFinished = false;
  private final List<Operator> operators;
  private final List<TsBlock> showTimeSeriesResult;
  private final List<TsBlock> lastQueryResult;

  private final List<TSDataType> outputDataTypes;
  private final int columnCount;

  private int currentIndex;

  public SchemaQueryOrderByHeatOperator(OperatorContext operatorContext, List<Operator> operators) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    this.operators = operators;
    this.showTimeSeriesResult = new ArrayList<>();
    this.lastQueryResult = new ArrayList<>();
    this.outputDataTypes =
        ColumnHeaderConstant.showTimeSeriesColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    this.columnCount = outputDataTypes.size();

    currentIndex = 0;
  }

  @Override
  public TsBlock next() {
    isFinished = true;

    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(outputDataTypes);

    // Step 1: get last point result
    Map<String, Long> timeseriesToLastTimestamp = new HashMap<>();
    for (TsBlock tsBlock : lastQueryResult) {
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        String timeseries = tsBlock.getColumn(0).getBinary(i).toString();
        long time = tsBlock.getTimeByIndex(i);
        timeseriesToLastTimestamp.put(timeseries, time);
      }
    }

    // Step 2: get last point timestamp to timeseries map
    Map<Long, List<Object[]>> lastTimestampToTsSchema = new HashMap<>();
    for (TsBlock tsBlock : showTimeSeriesResult) {
      TsBlock.TsBlockRowIterator tsBlockRowIterator = tsBlock.getTsBlockRowIterator();
      while (tsBlockRowIterator.hasNext()) {
        Object[] line = tsBlockRowIterator.next();
        String timeseries = line[0].toString();
        long time = timeseriesToLastTimestamp.getOrDefault(timeseries, 0L);
        if (!lastTimestampToTsSchema.containsKey(time)) {
          lastTimestampToTsSchema.put(time, new ArrayList<>());
        }
        lastTimestampToTsSchema.get(time).add(line);
      }
    }

    // Step 3: sort by last point's timestamp
    List<Long> timestamps = new ArrayList<>(lastTimestampToTsSchema.keySet());
    timestamps.sort(Comparator.reverseOrder());

    // Step 4: generate result
    for (Long time : timestamps) {
      List<Object[]> rows = lastTimestampToTsSchema.get(time);
      for (Object[] row : rows) {
        tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
        for (int i = 0; i < columnCount; i++) {
          Object value = row[i];
          if (null == value) {
            tsBlockBuilder.getColumnBuilder(i).appendNull();
          } else {
            tsBlockBuilder.getColumnBuilder(i).writeBinary(new Binary(value.toString()));
          }
        }
        tsBlockBuilder.declarePosition();
      }
    }

    return tsBlockBuilder.build();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    Operator operator;
    ListenableFuture<?> blocked;
    while (currentIndex < operators.size()) {
      operator = operators.get(currentIndex);
      blocked = readCurrentChild(operator);
      if (blocked != null) {
        // not null means blocked
        return blocked;
      } else {
        // null means current operator is finished
        currentIndex++;
      }
    }

    return NOT_BLOCKED;
  }

  private ListenableFuture<?> readCurrentChild(Operator operator) {
    while (!operator.isFinished()) {
      ListenableFuture<?> blocked = operator.isBlocked();
      if (!blocked.isDone()) {
        return blocked;
      }
      if (operator.hasNext()) {
        TsBlock tsBlock = operator.next();
        if (null != tsBlock && !tsBlock.isEmpty()) {
          if (isShowTimeSeriesBlock(tsBlock)) {
            showTimeSeriesResult.add(tsBlock);
          } else {
            lastQueryResult.add(tsBlock);
          }
        }
      }
    }
    return null;
  }

  private boolean isShowTimeSeriesBlock(TsBlock tsBlock) {
    return tsBlock.getValueColumnCount() == columnCount;
  }

  @Override
  public boolean hasNext() {
    return !isFinished;
  }

  @Override
  public void close() throws Exception {
    for (Operator operator : operators) {
      operator.close();
    }
  }

  @Override
  public boolean isFinished() {
    return isFinished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory = 0;

    for (Operator child : operators) {
      maxPeekMemory += child.calculateMaxReturnSize();
    }

    for (Operator child : operators) {
      maxPeekMemory = Math.max(maxPeekMemory, child.calculateMaxPeekMemory());
    }

    return maxPeekMemory;
  }

  @Override
  public long calculateMaxReturnSize() {
    long maxReturnSize = 0;

    for (Operator child : operators) {
      maxReturnSize += child.calculateMaxReturnSize();
    }

    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long retainedSize = 0L;

    for (Operator child : operators) {
      retainedSize += child.calculateMaxReturnSize();
    }

    for (Operator child : operators) {
      retainedSize += child.calculateRetainedSizeAfterCallingNext();
    }
    return retainedSize;
  }
}
