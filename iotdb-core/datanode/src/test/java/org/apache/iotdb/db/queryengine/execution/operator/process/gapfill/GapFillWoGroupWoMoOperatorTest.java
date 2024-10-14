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

package org.apache.iotdb.db.queryengine.execution.operator.process.gapfill;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.MergeSortOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.MergeSortComparator.getComparatorForTable;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class GapFillWoGroupWoMoOperatorTest {

  private static final ExecutorService instanceNotificationExecutor =
      IoTDBThreadPoolFactory.newFixedThreadPool(
          1, "GapFillWoGroupWoMoOperator-test-instance-notification");

  private GapFillWoGroupWoMoOperator genGapFillWoGroupWoMoOperator() {
    // child output
    // Time,             city,       deviceId,   avg_temp
    // 1728856800000     yangzhou       d1       null
    // 1728856800000     yangzhou       d1       27.2
    // 1728864000000     yangzhou       d1       27.3
    // 1728874800000     yangzhou       d1       29.3
    // ------------------------------------------------ TsBlock-1
    // 1728849600000     beijing        d2       25.1
    // ------------------------------------------------ TsBlock-2
    // 1728874800000     beijing        d2       28.2
    // ------------------------------------------------ TsBlock-3
    // 1728864000000     shanghai       d3       25.8
    // 1728864000000     null           d4       24.8
    // ------------------------------------------------ TsBlock-4

    // Construct operator tree
    QueryId queryId = new QueryId("stub_query");

    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNodeId1 = new PlanNodeId("1");
    driverContext.addOperatorContext(1, planNodeId1, MergeSortOperator.class.getSimpleName());
    PlanNodeId planNodeId2 = new PlanNodeId("2");
    driverContext.addOperatorContext(
        2, planNodeId2, GapFillWoGroupWoMoOperator.class.getSimpleName());
    Operator childOperator =
        new Operator() {

          private final long[][] timeArray =
              new long[][] {
                {1728856800000L, 1728856800000L, 1728864000000L, 1728874800000L},
                {1728849600000L},
                {1728874800000L},
                {1728874800000L},
                {1728864000000L, 1728864000000L}
              };

          private final String[][] cityArray =
              new String[][] {
                {"yangzhou", "yangzhou", "yangzhou", "yangzhou"},
                {"beijing"},
                {"beijing"},
                {"shanghai", null},
              };

          private final String[][] deviceIdArray =
              new String[][] {
                {"d1", "d1", "d1", "d1"},
                {"d2"},
                {"d2"},
                {"d3", "d4"},
              };

          private final double[][] valueArray =
              new double[][] {
                {0.0, 27.2, 27.3, 29.3},
                {25.1},
                {28.2},
                {25.8, 24.8},
              };

          private final boolean[][] valueIsNull =
              new boolean[][] {
                {false, true, true, true},
                {true},
                {true},
                {true, true},
              };

          private int index = 0;

          @Override
          public OperatorContext getOperatorContext() {
            return driverContext.getOperatorContexts().get(0);
          }

          @Override
          public TsBlock next() {
            if (timeArray[index] == null) {
              index++;
              return null;
            }
            TsBlockBuilder builder =
                new TsBlockBuilder(
                    timeArray[index].length,
                    Arrays.asList(
                        TSDataType.TIMESTAMP,
                        TSDataType.STRING,
                        TSDataType.STRING,
                        TSDataType.DOUBLE));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getColumnBuilder(0).writeLong(timeArray[index][i]);
              if (cityArray[index][i] == null) {
                builder.getColumnBuilder(1).appendNull();
              } else {
                builder
                    .getColumnBuilder(1)
                    .writeBinary(new Binary(cityArray[index][i], TSFileConfig.STRING_CHARSET));
              }
              if (deviceIdArray[index][i] == null) {
                builder.getColumnBuilder(2).appendNull();
              } else {
                builder
                    .getColumnBuilder(2)
                    .writeBinary(new Binary(deviceIdArray[index][i], TSFileConfig.STRING_CHARSET));
              }
              if (valueIsNull[index][i]) {
                builder.getColumnBuilder(3).appendNull();
              } else {
                builder.getColumnBuilder(3).writeDouble(valueArray[index][i]);
              }
            }
            builder.declarePositions(timeArray[index].length);
            index++;
            return builder.build(
                new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, builder.getPositionCount()));
          }

          @Override
          public boolean hasNext() throws Exception {
            return index < valueIsNull.length;
          }

          @Override
          public void close() throws Exception {}

          @Override
          public boolean isFinished() throws Exception {
            return index >= valueIsNull.length;
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
        };

    OperatorContext operatorContext = driverContext.getOperatorContexts().get(1);

    List<SortOrder> sortOrderList =
        Arrays.asList(
            SortOrder.ASC_NULLS_FIRST, SortOrder.ASC_NULLS_FIRST, SortOrder.ASC_NULLS_FIRST);
    List<Integer> sortItemIndexList = Arrays.asList(1, 2, 3);
    List<TSDataType> sortItemDataTypeList =
        Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.INT32);

    Comparator<SortKey> comparator =
        getComparatorForTable(sortOrderList, sortItemIndexList, sortItemDataTypeList);

    Comparator<SortKey> streamKeyComparator =
        getComparatorForTable(
            sortOrderList.subList(0, 2),
            sortItemIndexList.subList(0, 2),
            sortItemDataTypeList.subList(0, 2));

    return new GapFillWoGroupWoMoOperator(
        operatorContext,
        childOperator,
        0,
        1728849600000L,
        1728874800000L,
        Arrays.asList(
            TSDataType.TIMESTAMP, TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE),
        3600000L);
  }
}
