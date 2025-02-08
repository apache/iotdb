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

package org.apache.iotdb.db.queryengine.execution.operator.schema;

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.Futures.successfulAsList;
import static java.util.Objects.requireNonNull;

public class SchemaQueryOrderByHeatOperator implements ProcessOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SchemaQueryOrderByHeatOperator.class);

  private final OperatorContext operatorContext;
  private final List<Operator> operators;
  private final List<TsBlock> showTimeSeriesResult;
  private final List<TsBlock> lastQueryResult;

  private final List<TSDataType> outputDataTypes;
  private final int columnCount;

  private final boolean[] noMoreTsBlocks;
  private List<TsBlock> resultTsBlockList;
  private int currentIndex = 0;

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

    noMoreTsBlocks = new boolean[operators.size()];
  }

  @Override
  public TsBlock next() throws Exception {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    if (resultTsBlockList != null) {
      currentIndex++;
      return resultTsBlockList.get(currentIndex - 1);
    }

    boolean allChildrenReady = true;
    Operator operator;
    for (int i = 0; i < operators.size(); i++) {
      if (!noMoreTsBlocks[i]) {
        operator = operators.get(i);
        if (operator.isFinished()) {
          noMoreTsBlocks[i] = true;
        } else {
          if (operator.hasNextWithTimer()) {
            TsBlock tsBlock = operator.nextWithTimer();
            if (null != tsBlock && !tsBlock.isEmpty()) {
              if (isShowTimeSeriesBlock(tsBlock)) {
                showTimeSeriesResult.add(tsBlock);
              } else {
                lastQueryResult.add(tsBlock);
              }
            }
          }
        }
      }
      if (!noMoreTsBlocks[i]) {
        allChildrenReady = false;
      }
    }

    if (allChildrenReady) {
      generateResultTsBlockList();
      currentIndex++;
      return resultTsBlockList.get(currentIndex - 1);
    } else {
      return null;
    }
  }

  private void generateResultTsBlockList() {
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
        lastTimestampToTsSchema.computeIfAbsent(time, key -> new ArrayList<>()).add(line);
      }
    }

    // Step 3: sort by last point's timestamp
    List<Long> timestamps = new ArrayList<>(lastTimestampToTsSchema.keySet());
    timestamps.sort(Comparator.reverseOrder());

    // Step 4: generate result
    this.resultTsBlockList =
        SchemaTsBlockUtil.transferSchemaResultToTsBlockList(
            timestamps.iterator(),
            outputDataTypes,
            (time, tsBlockBuilder) -> {
              List<Object[]> rows = lastTimestampToTsSchema.get(time);
              for (Object[] row : rows) {
                tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
                for (int i = 0; i < columnCount; i++) {
                  Object value = row[i];
                  if (null == value) {
                    tsBlockBuilder.getColumnBuilder(i).appendNull();
                  } else {
                    tsBlockBuilder
                        .getColumnBuilder(i)
                        .writeBinary(new Binary(value.toString(), TSFileConfig.STRING_CHARSET));
                  }
                }
                tsBlockBuilder.declarePosition();
              }
            });
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    List<ListenableFuture<?>> listenableFutureList = new ArrayList<>(operators.size());
    for (int i = 0; i < operators.size(); i++) {
      if (noMoreTsBlocks[i]) {
        continue;
      }
      ListenableFuture<?> isBlocked = operators.get(i).isBlocked();
      if (!isBlocked.isDone()) {
        listenableFutureList.add(isBlocked);
      }
    }
    return listenableFutureList.isEmpty() ? NOT_BLOCKED : successfulAsList(listenableFutureList);
  }

  private boolean isShowTimeSeriesBlock(TsBlock tsBlock) {
    return tsBlock.getValueColumnCount() == columnCount;
  }

  @Override
  public boolean hasNext() throws Exception {
    return resultTsBlockList == null || currentIndex < resultTsBlockList.size();
  }

  @Override
  public void close() throws Exception {
    for (Operator operator : operators) {
      operator.close();
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return !hasNextWithTimer();
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

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + operators.stream().mapToLong(Accountable::ramBytesUsed).sum()
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + RamUsageEstimator.sizeOf(noMoreTsBlocks);
  }
}
