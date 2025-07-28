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

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.db.queryengine.execution.aggregation.CountAccumulator;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.AggregationMergeSortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.MergeSortComparator.getComparator;
import static org.junit.Assert.assertEquals;

public class AggregationMergeSortOperatorTest {

  @Test
  public void deviceInTwoRegionTest() throws Exception {
    OperatorContext operatorContext =
        new OperatorContext(1, new PlanNodeId("1"), "test-type", new DriverContext());

    MockDeviceViewOperator1 operator1 = new MockDeviceViewOperator1(operatorContext);
    MockDeviceViewOperator1 operator2 = new MockDeviceViewOperator2(operatorContext);

    List<SortItem> sortItemList =
        Arrays.asList(new SortItem("DEVICE", Ordering.ASC), new SortItem("TIME", Ordering.ASC));
    List<Integer> sortItemIndexList = Arrays.asList(0, -1);
    List<TSDataType> sortItemDataTypeList = Arrays.asList(TSDataType.TEXT, TSDataType.INT64);
    Comparator<SortKey> comparator =
        getComparator(sortItemList, sortItemIndexList, sortItemDataTypeList);

    AggregationMergeSortOperator operator =
        new AggregationMergeSortOperator(
            operatorContext,
            Arrays.asList(operator1, operator2),
            Arrays.asList(TSDataType.TEXT, TSDataType.INT64),
            Collections.singletonList(new CountAccumulator()),
            false,
            comparator);
    int cnt = 0;
    while (operator.isBlocked().isDone() && operator.hasNext()) {
      TsBlock block = operator.next();
      if (block != null && block.getPositionCount() > 0) {
        if (cnt == 0) {
          assertEquals("d1", block.getColumn(0).getBinary(0).toString());
          assertEquals(3, block.getColumn(1).getLong(0));
        } else {
          assertEquals("d2", block.getColumn(0).getBinary(0).toString());
          assertEquals(5, block.getColumn(1).getLong(0));
        }
        cnt++;
      }
    }
    assertEquals(2, cnt);
  }

  private static class MockDeviceViewOperator1 implements ProcessOperator {

    OperatorContext operatorContext;
    int invokeCount = 0;

    public MockDeviceViewOperator1(OperatorContext operatorContext) {
      this.operatorContext = operatorContext;
    }

    @Override
    public OperatorContext getOperatorContext() {
      return operatorContext;
    }

    @Override
    public TsBlock next() throws Exception {
      if (invokeCount == 0) {
        invokeCount++;
        return buildTsBlock("d1", 1);
      }
      return null;
    }

    @Override
    public boolean hasNext() throws Exception {
      return invokeCount < 1;
    }

    @Override
    public void close() throws Exception {}

    @Override
    public boolean isFinished() throws Exception {
      return invokeCount < 1;
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

  private static class MockDeviceViewOperator2 extends MockDeviceViewOperator1 {

    public MockDeviceViewOperator2(OperatorContext operatorContext) {
      super(operatorContext);
    }

    @Override
    public TsBlock next() throws Exception {
      if (invokeCount == 0) {
        invokeCount++;
        return buildTsBlock("d1", 2);
      } else if (invokeCount == 1) {
        invokeCount++;
        return buildTsBlock("d2", 5);
      }
      return null;
    }

    @Override
    public boolean hasNext() throws Exception {
      return invokeCount < 2;
    }
  }

  private static TsBlock buildTsBlock(String device, int count) {
    LongColumn timeColumn = new LongColumn(1, Optional.empty(), new long[] {0});
    BinaryColumn deviceColumn =
        new BinaryColumn(1, Optional.empty(), new Binary[] {new Binary(device.getBytes())});
    LongColumn countColumn = new LongColumn(1, Optional.empty(), new long[] {count});
    return new TsBlock(1, timeColumn, deviceColumn, countColumn);
  }
}
