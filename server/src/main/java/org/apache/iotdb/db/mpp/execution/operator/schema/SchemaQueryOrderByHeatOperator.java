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

import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.ProcessOperator;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class SchemaQueryOrderByHeatOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final Operator left;
  private final Operator right;
  private boolean isFinished = false;
  private final List<TsBlock> leftResult;
  private final List<TsBlock> rightResult;

  public SchemaQueryOrderByHeatOperator(
      OperatorContext operatorContext, Operator left, Operator right) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    this.left = requireNonNull(left, "left child operator is null");
    this.right = requireNonNull(right, "right child operator is null");
    this.leftResult = new ArrayList<>();
    this.rightResult = new ArrayList<>();
  }

  @Override
  public TsBlock next() {
    isFinished = true;

    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(HeaderConstant.showTimeSeriesHeader.getRespDataTypes());

    // Step 1: get last point result
    Map<String, Long> timeseriesToLastTimestamp = new HashMap<>();
    for (TsBlock tsBlock : rightResult) {
      if (null == tsBlock || tsBlock.isEmpty()) {
        continue;
      }
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        String timeseries = tsBlock.getColumn(0).getBinary(i).toString();
        long time = tsBlock.getTimeByIndex(i);
        timeseriesToLastTimestamp.put(timeseries, time);
      }
    }

    // Step 2: get last point timestamp to timeseries map
    Map<Long, List<Object[]>> lastTimestampToTsSchema = new HashMap<>();
    for (TsBlock tsBlock : leftResult) {
      if (null == tsBlock || tsBlock.isEmpty()) {
        continue;
      }
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
        for (int i = 0; i < HeaderConstant.showTimeSeriesHeader.getRespDataTypes().size(); i++) {
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
  public ListenableFuture<Void> isBlocked() {
    ListenableFuture<Void> blocked = left.isBlocked();
    while (left.hasNext() && blocked.isDone()) {
      leftResult.add(left.next());
      blocked = left.isBlocked();
    }
    if (!blocked.isDone()) {
      return blocked;
    }
    blocked = right.isBlocked();
    while (right.hasNext() && blocked.isDone()) {
      rightResult.add(right.next());
      blocked = right.isBlocked();
    }
    if (!blocked.isDone()) {
      return blocked;
    }
    return NOT_BLOCKED;
  }

  @Override
  public boolean hasNext() {
    return !isFinished;
  }

  @Override
  public void close() throws Exception {
    left.close();
    right.close();
  }

  @Override
  public boolean isFinished() {
    return isFinished;
  }
}
