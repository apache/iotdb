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
package org.apache.iotdb.db.mpp.execution.operator;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.driver.DriverContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryUtil;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.mpp.execution.operator.AggregationOperatorTest.TEST_TIME_SLICE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LastQueryMergeOperatorTest {

  private ExecutorService instanceNotificationExecutor;

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    this.instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
  }

  @After
  public void tearDown() throws IOException {
    instanceNotificationExecutor.shutdown();
  }

  @Test
  public void testLastQueryMergeOperatorDesc() {

    QueryId queryId = new QueryId("stub_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNodeId1 = new PlanNodeId("1");
    driverContext.addOperatorContext(1, planNodeId1, LastQueryMergeOperator.class.getSimpleName());

    driverContext
        .getOperatorContexts()
        .forEach(operatorContext -> operatorContext.setMaxRunTime(TEST_TIME_SLICE));

    Operator operator1 =
        new Operator() {

          private final long[][] timeArray = new long[][] {{3, 4, 5, 3}, {5, 4, 4, 6}};
          private final String[][] timeSeriesArray =
              new String[][] {
                {"root.sg.d1.s1", "root.sg.d1.s2", "root.sg.d1.s3", "root.sg.d1.s4"},
                {"root.sg.d2.s1", "root.sg.d2.s2", "root.sg.d2.s3", "root.sg.d2.s4"}
              };
          private final String[][] valueArray =
              new String[][] {{"3", "4", "5", "3"}, {"5", "4", "4", "6"}};
          private final String[][] dataTypeArray =
              new String[][] {
                {"INT32", "INT32", "INT32", "INT32"}, {"INT32", "INT32", "INT32", "INT32"}
              };

          private int index = 1;

          @Override
          public OperatorContext getOperatorContext() {
            return driverContext.getOperatorContexts().get(0);
          }

          @Override
          public TsBlock next() {
            TsBlockBuilder builder = LastQueryUtil.createTsBlockBuilder(4);
            for (int i = timeArray[index].length - 1; i >= 0; i--) {
              LastQueryUtil.appendLastValue(
                  builder,
                  timeArray[index][i],
                  timeSeriesArray[index][i],
                  valueArray[index][i],
                  dataTypeArray[index][i]);
            }
            index--;
            return builder.build();
          }

          @Override
          public boolean hasNext() {
            return index >= 0;
          }

          @Override
          public boolean isFinished() {
            return !hasNext();
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
        };

    Operator operator2 =
        new Operator() {

          private final long[][] timeArray = new long[][] {{2, 3, 3, 2}, {5, 4, 4, 6}};
          private final String[][] timeSeriesArray =
              new String[][] {
                {"root.sg.d2.s1", "root.sg.d2.s2", "root.sg.d2.s3", "root.sg.d2.s4"},
                {"root.sg.d3.s1", "root.sg.d3.s2", "root.sg.d3.s3", "root.sg.d3.s4"}
              };
          private final String[][] valueArray =
              new String[][] {{"2", "3", "3", "2"}, {"5", "4", "4", "6"}};
          private final String[][] dataTypeArray =
              new String[][] {
                {"INT32", "INT32", "INT32", "INT32"}, {"INT32", "INT32", "INT32", "INT32"}
              };

          private int index = 1;

          @Override
          public OperatorContext getOperatorContext() {
            return driverContext.getOperatorContexts().get(0);
          }

          @Override
          public TsBlock next() {
            TsBlockBuilder builder = LastQueryUtil.createTsBlockBuilder(4);
            for (int i = timeArray[index].length - 1; i >= 0; i--) {
              LastQueryUtil.appendLastValue(
                  builder,
                  timeArray[index][i],
                  timeSeriesArray[index][i],
                  valueArray[index][i],
                  dataTypeArray[index][i]);
            }
            index--;
            return builder.build();
          }

          @Override
          public boolean hasNext() {
            return index >= 0;
          }

          @Override
          public boolean isFinished() {
            return !hasNext();
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
        };

    LastQueryMergeOperator lastQueryMergeOperator =
        new LastQueryMergeOperator(
            driverContext.getOperatorContexts().get(0),
            ImmutableList.of(operator1, operator2),
            Comparator.reverseOrder());

    final long[] timeArray = new long[] {3, 4, 5, 3, 5, 4, 4, 6, 5, 4, 4, 6};
    final String[] timeSeriesArray =
        new String[] {
          "root.sg.d1.s1",
          "root.sg.d1.s2",
          "root.sg.d1.s3",
          "root.sg.d1.s4",
          "root.sg.d2.s1",
          "root.sg.d2.s2",
          "root.sg.d2.s3",
          "root.sg.d2.s4",
          "root.sg.d3.s1",
          "root.sg.d3.s2",
          "root.sg.d3.s3",
          "root.sg.d3.s4"
        };
    final String[] valueArray =
        new String[] {"3", "4", "5", "3", "5", "4", "4", "6", "5", "4", "4", "6"};
    final String[] dataTypeArray =
        new String[] {
          "INT32", "INT32", "INT32", "INT32", "INT32", "INT32", "INT32", "INT32", "INT32", "INT32",
          "INT32", "INT32"
        };

    int count = timeArray.length - 1;
    while (!lastQueryMergeOperator.isFinished()) {
      assertTrue(lastQueryMergeOperator.isBlocked().isDone());
      TsBlock result = lastQueryMergeOperator.next();
      if (result == null) {
        continue;
      }
      assertEquals(3, result.getValueColumnCount());

      for (int i = 0; i < result.getPositionCount(); i++) {
        assertEquals(timeArray[count], result.getTimeByIndex(i));
        assertEquals(timeSeriesArray[count], result.getColumn(0).getBinary(i).toString());
        assertEquals(valueArray[count], result.getColumn(1).getBinary(i).toString());
        assertEquals(dataTypeArray[count], result.getColumn(2).getBinary(i).toString());
        count--;
      }
    }
    assertEquals(-1, count);
  }

  @Test
  public void testLastQueryMergeOperatorAsc() {

    QueryId queryId = new QueryId("stub_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNodeId1 = new PlanNodeId("1");
    driverContext.addOperatorContext(1, planNodeId1, LastQueryMergeOperator.class.getSimpleName());

    driverContext
        .getOperatorContexts()
        .forEach(operatorContext -> operatorContext.setMaxRunTime(TEST_TIME_SLICE));

    Operator operator1 =
        new Operator() {

          private final long[][] timeArray = new long[][] {{3, 4, 5, 3}, {5, 4, 4, 6}};
          private final String[][] timeSeriesArray =
              new String[][] {
                {"root.sg.d1.s1", "root.sg.d1.s2", "root.sg.d1.s3", "root.sg.d1.s4"},
                {"root.sg.d2.s1", "root.sg.d2.s2", "root.sg.d2.s3", "root.sg.d2.s4"}
              };
          private final String[][] valueArray =
              new String[][] {{"3", "4", "5", "3"}, {"5", "4", "4", "6"}};
          private final String[][] dataTypeArray =
              new String[][] {
                {"INT32", "INT32", "INT32", "INT32"}, {"INT32", "INT32", "INT32", "INT32"}
              };

          private int index = 0;

          @Override
          public OperatorContext getOperatorContext() {
            return driverContext.getOperatorContexts().get(0);
          }

          @Override
          public TsBlock next() {
            TsBlockBuilder builder = LastQueryUtil.createTsBlockBuilder(4);
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              LastQueryUtil.appendLastValue(
                  builder,
                  timeArray[index][i],
                  timeSeriesArray[index][i],
                  valueArray[index][i],
                  dataTypeArray[index][i]);
            }
            index++;
            return builder.build();
          }

          @Override
          public boolean hasNext() {
            return index < 2;
          }

          @Override
          public boolean isFinished() {
            return !hasNext();
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
        };

    Operator operator2 =
        new Operator() {

          private final long[][] timeArray = new long[][] {{2, 3, 3, 2}, {5, 4, 4, 6}};
          private final String[][] timeSeriesArray =
              new String[][] {
                {"root.sg.d2.s1", "root.sg.d2.s2", "root.sg.d2.s3", "root.sg.d2.s4"},
                {"root.sg.d3.s1", "root.sg.d3.s2", "root.sg.d3.s3", "root.sg.d3.s4"}
              };
          private final String[][] valueArray =
              new String[][] {{"2", "3", "3", "2"}, {"5", "4", "4", "6"}};
          private final String[][] dataTypeArray =
              new String[][] {
                {"INT32", "INT32", "INT32", "INT32"}, {"INT32", "INT32", "INT32", "INT32"}
              };

          private int index = 0;

          @Override
          public OperatorContext getOperatorContext() {
            return driverContext.getOperatorContexts().get(0);
          }

          @Override
          public TsBlock next() {
            TsBlockBuilder builder = LastQueryUtil.createTsBlockBuilder(4);
            for (int i = 0, size = timeArray[index].length; i < size; i++) {
              LastQueryUtil.appendLastValue(
                  builder,
                  timeArray[index][i],
                  timeSeriesArray[index][i],
                  valueArray[index][i],
                  dataTypeArray[index][i]);
            }
            index++;
            return builder.build();
          }

          @Override
          public boolean hasNext() {
            return index < 2;
          }

          @Override
          public boolean isFinished() {
            return !hasNext();
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
        };

    LastQueryMergeOperator lastQueryMergeOperator =
        new LastQueryMergeOperator(
            driverContext.getOperatorContexts().get(0),
            ImmutableList.of(operator1, operator2),
            Comparator.naturalOrder());

    final long[] timeArray = new long[] {3, 4, 5, 3, 5, 4, 4, 6, 5, 4, 4, 6};
    final String[] timeSeriesArray =
        new String[] {
          "root.sg.d1.s1",
          "root.sg.d1.s2",
          "root.sg.d1.s3",
          "root.sg.d1.s4",
          "root.sg.d2.s1",
          "root.sg.d2.s2",
          "root.sg.d2.s3",
          "root.sg.d2.s4",
          "root.sg.d3.s1",
          "root.sg.d3.s2",
          "root.sg.d3.s3",
          "root.sg.d3.s4"
        };
    final String[] valueArray =
        new String[] {"3", "4", "5", "3", "5", "4", "4", "6", "5", "4", "4", "6"};
    final String[] dataTypeArray =
        new String[] {
          "INT32", "INT32", "INT32", "INT32", "INT32", "INT32", "INT32", "INT32", "INT32", "INT32",
          "INT32", "INT32"
        };

    int count = 0;
    while (!lastQueryMergeOperator.isFinished()) {
      assertTrue(lastQueryMergeOperator.isBlocked().isDone());
      TsBlock result = lastQueryMergeOperator.next();
      if (result == null) {
        continue;
      }
      assertEquals(3, result.getValueColumnCount());

      for (int i = 0; i < result.getPositionCount(); i++) {
        assertEquals(timeArray[count], result.getTimeByIndex(i));
        assertEquals(timeSeriesArray[count], result.getColumn(0).getBinary(i).toString());
        assertEquals(valueArray[count], result.getColumn(1).getBinary(i).toString());
        assertEquals(dataTypeArray[count], result.getColumn(2).getBinary(i).toString());
        count++;
      }
    }

    assertEquals(timeArray.length, count);
  }
}
