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

package org.apache.iotdb.db.queryengine.execution.operator.process.window;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.TreeLinearFillOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.value.FirstValueFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.junit.Assert.fail;

public class TableWindowOperatorTest {
  private static final ExecutorService instanceNotificationExecutor =
      IoTDBThreadPoolFactory.newFixedThreadPool(1, "windowOperator-test-instance-notification");

  @Test
  public void windowOperatorTest() {
    try (TableWindowOperator windowOperator = genWindowOperator()) {
      while (windowOperator.hasNext()) {
        TsBlock block = windowOperator.next();
        System.out.println(block);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private TableWindowOperator genWindowOperator() {
    QueryId queryId = new QueryId("stub_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNode = new PlanNodeId("1");
    driverContext.addOperatorContext(1, planNode, TreeLinearFillOperator.class.getSimpleName());

    Operator childOperator =
        new Operator() {
          private int index = 0;

          private final long[][] timeArray =
              new long[][] {
                {1, 2, 3, 4},
                {1, 2},
              };
          private final String[][] deviceIdArray =
              new String[][] {
                {"d1", "d1", "d1", "d1"},
                {"d2", "d2"},
              };
          private final int[][] valueArray =
              new int[][] {
                {3, 5, 3, 1},
                {2, 4},
              };

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
                    Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TEXT, TSDataType.INT32));
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              builder.getColumnBuilder(0).writeLong(timeArray[index][i]);
              builder
                  .getColumnBuilder(1)
                  .writeBinary(new Binary(deviceIdArray[index][i], TSFileConfig.STRING_CHARSET));
              builder.getColumnBuilder(2).writeInt(valueArray[index][i]);
            }
            builder.declarePositions(timeArray[index].length);
            index++;
            return builder.build(
                new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, builder.getPositionCount()));
          }

          @Override
          public boolean hasNext() {
            return index < timeArray.length;
          }

          @Override
          public boolean isFinished() {
            return index >= timeArray.length;
          }

          @Override
          public void close() {
            // do nothing
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
    List<TSDataType> inputDataTypes =
        Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TEXT, TSDataType.INT32);
    List<TSDataType> outputDataTypes =
        Arrays.asList(TSDataType.TIMESTAMP, TSDataType.TEXT, TSDataType.INT32, TSDataType.INT32);
    WindowFunction windowFunction = new FirstValueFunction(2, true);
    FrameInfo frameInfo =
        new FrameInfo(
            FrameInfo.FrameType.ROWS,
            FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING,
            FrameInfo.FrameBoundType.CURRENT_ROW);

    return new TableWindowOperator(
        driverContext.getOperatorContexts().get(0),
        childOperator,
        inputDataTypes,
        outputDataTypes,
        Collections.singletonList(windowFunction),
        Collections.singletonList(frameInfo),
        Collections.singletonList(1),
        Collections.singletonList(2));
  }
}
