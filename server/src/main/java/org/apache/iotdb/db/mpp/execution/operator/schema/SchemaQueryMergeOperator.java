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
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaQueryMergeOperator implements ProcessOperator {
  private final PlanNodeId planNodeId;
  private final OperatorContext operatorContext;
  private final boolean[] noMoreTsBlocks;
  private final boolean orderByHeat;

  private final List<Operator> children;
  private boolean isFinished;

  public SchemaQueryMergeOperator(
      PlanNodeId planNodeId,
      boolean orderByHeat,
      OperatorContext operatorContext,
      List<Operator> children) {
    this.planNodeId = planNodeId;
    this.operatorContext = operatorContext;
    this.children = children;
    noMoreTsBlocks = new boolean[children.size()];
    this.orderByHeat = orderByHeat;
    isFinished = false;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    // ToDo @xinzhongtianxia consider SHOW LATEST
    if (orderByHeat) {
      isFinished = true;
      TsBlockBuilder tsBlockBuilder =
          new TsBlockBuilder(HeaderConstant.showTimeSeriesHeader.getRespDataTypes());
      // Step 1: load all rows
      Map<String, List<Object[]>> valueToLines = new HashMap<>();
      for (int i = 0; i < children.size(); i++) {
        while (children.get(i).hasNext()) {
          TsBlock tsBlock = children.get(i).next();
          TsBlock.TsBlockRowIterator tsBlockRowIterator = tsBlock.getTsBlockRowIterator();
          while (tsBlockRowIterator.hasNext()) {
            Object[] row = tsBlockRowIterator.next();
            String value = row[row.length - 2].toString();
            if (!valueToLines.containsKey(value)) {
              valueToLines.put(value, new ArrayList<>());
            }
            valueToLines.get(value).add(row);
          }
        }
      }
      // Step 2: sort and rewrite
      List<String> values = new ArrayList<>(valueToLines.keySet());
      values.sort(
          (o1, o2) -> {
            long value1;
            long value2;
            try {
              value1 = Long.parseLong(o1);
            } catch (Exception e) {
              return -1;
            }
            try {
              value2 = Long.parseLong(o2);
            } catch (Exception e) {
              return 1;
            }
            if (value1 > value2) {
              return 1;
            } else if (value1 == value2) {
              return 0;
            } else {
              return -1;
            }
          });
      for (String value : values) {
        List<Object[]> rows = valueToLines.get(value);
        for (Object[] row : rows) {
          tsBlockBuilder.getColumnBuilder(0).writeBinary(new Binary(row[0].toString()));
          tsBlockBuilder.getColumnBuilder(1).writeBinary(new Binary(row[1].toString()));
          tsBlockBuilder.getColumnBuilder(2).writeBinary(new Binary(row[2].toString()));
          tsBlockBuilder.getColumnBuilder(3).writeBinary(new Binary(row[3].toString()));
          tsBlockBuilder.getColumnBuilder(4).writeBinary(new Binary(row[4].toString()));
          tsBlockBuilder.getColumnBuilder(5).writeBinary(new Binary(row[5].toString()));
          tsBlockBuilder.getColumnBuilder(6).writeBinary(new Binary(row[6].toString()));
          tsBlockBuilder.getColumnBuilder(7).writeBinary(new Binary(row[7].toString()));
          tsBlockBuilder.declarePosition();
        }
      }
      return tsBlockBuilder.build();
    } else {
      for (int i = 0; i < children.size(); i++) {
        if (!noMoreTsBlocks[i]) {
          TsBlock tsBlock = children.get(i).next();
          if (!children.get(i).hasNext()) {
            noMoreTsBlocks[i] = true;
          }
          return tsBlock;
        }
      }
    }
    return null;
  }

  @Override
  public boolean hasNext() {
    if (orderByHeat) {
      return isFinished;
    }
    for (int i = 0; i < children.size(); i++) {
      if (!noMoreTsBlocks[i] && children.get(i).hasNext()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public ListenableFuture<Void> isBlocked() {
    for (int i = 0; i < children.size(); i++) {
      if (!noMoreTsBlocks[i]) {
        ListenableFuture<Void> blocked = children.get(i).isBlocked();
        if (!blocked.isDone()) {
          return blocked;
        }
      }
    }
    return NOT_BLOCKED;
  }

  @Override
  public boolean isFinished() {
    if (orderByHeat) {
      return isFinished;
    }
    return !hasNext();
  }
}
