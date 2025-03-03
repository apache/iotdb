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

package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.db.queryengine.execution.operator.AbstractOperator;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.Arrays;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.TIME_COLUMN_TEMPLATE;

public class TestTsBlockBuilder extends AbstractOperator {
  private TsBlockBuilder resultBuilder;
  private int positionCount = 9363;

  public TestTsBlockBuilder() {
    this.resultBuilder =
        new TsBlockBuilder(Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32));
  }

  public static void main(String[] args) {
    TestTsBlockBuilder testTsBlockBuilder = new TestTsBlockBuilder();
    try {
      while (testTsBlockBuilder.hasNext()) {
        TsBlock tsBlock = testTsBlockBuilder.next();
        System.out.println(tsBlock);
      }
      testTsBlockBuilder.next();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      getResultFromRetainedTsBlock();
    }
    int probeIndex = 0;
    while (probeIndex < positionCount) {
      appendValueToResult();
      probeIndex++;
    }
    if (resultBuilder.isEmpty()) {
      return null;
    }

    resultTsBlock =
        resultBuilder.build(
            new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, resultBuilder.getPositionCount()));
    resultBuilder.reset();
    return checkTsBlockSizeAndGetResult();
  }

  private void appendValueToResult() {
    for (int i = 0; i < resultBuilder.getValueColumnBuilders().length; i++) {
      ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(i);
      columnBuilder.writeInt(1);
    }
    resultBuilder.declarePositions(1);
  }

  @Override
  public boolean hasNext() throws Exception {
    return true;
  }

  @Override
  public void close() throws Exception {}

  @Override
  public boolean isFinished() throws Exception {
    return false;
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
