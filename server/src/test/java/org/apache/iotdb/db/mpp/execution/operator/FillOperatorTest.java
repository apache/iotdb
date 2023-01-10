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
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.driver.DriverContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.process.FillOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.IFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.constant.DoubleConstantFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.previous.IntPreviousFill;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FillOperatorTest {

  @Test
  public void batchConstantFillTest() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
      PlanNodeId planNodeId1 = new PlanNodeId("1");
      driverContext.addOperatorContext(1, planNodeId1, FillOperator.class.getSimpleName());

      IFill[] fillArray =
          new IFill[] {
            new DoubleConstantFill(520.0),
            new DoubleConstantFill(520.0),
            new DoubleConstantFill(520.0)
          };
      FillOperator fillOperator =
          new FillOperator(
              driverContext.getOperatorContexts().get(0),
              fillArray,
              new Operator() {
                private int index = 0;

                @Override
                public OperatorContext getOperatorContext() {
                  return driverContext.getOperatorContexts().get(0);
                }

                @Override
                public TsBlock next() {
                  int delta = index * 10000;
                  TsBlockBuilder builder =
                      new TsBlockBuilder(
                          ImmutableList.of(
                              TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE));
                  // 1  1.0, null, 100.0
                  builder.getTimeColumnBuilder().writeLong(1 + delta);
                  builder.getColumnBuilder(0).writeDouble(1 + delta);
                  builder.getColumnBuilder(1).appendNull();
                  builder.getColumnBuilder(2).writeDouble(100 + delta);
                  builder.declarePosition();
                  // 2  2.0, 20.0, 200.0
                  builder.getTimeColumnBuilder().writeLong(2 + delta);
                  builder.getColumnBuilder(0).writeDouble(2 + delta);
                  builder.getColumnBuilder(1).writeDouble(20 + delta);
                  builder.getColumnBuilder(2).writeDouble(200 + delta);
                  builder.declarePosition();
                  // 3  3.0, 30.0, null
                  builder.getTimeColumnBuilder().writeLong(3 + delta);
                  builder.getColumnBuilder(0).writeDouble(3 + delta);
                  builder.getColumnBuilder(1).writeDouble(30 + delta);
                  builder.getColumnBuilder(2).appendNull();
                  builder.declarePosition();
                  // 4  null, 40.0, null
                  builder.getTimeColumnBuilder().writeLong(4 + delta);
                  builder.getColumnBuilder(0).appendNull();
                  builder.getColumnBuilder(1).writeDouble(40 + delta);
                  builder.getColumnBuilder(2).appendNull();
                  builder.declarePosition();
                  // 5  null, null, 500.0
                  builder.getTimeColumnBuilder().writeLong(5 + delta);
                  builder.getColumnBuilder(0).appendNull();
                  builder.getColumnBuilder(1).appendNull();
                  builder.getColumnBuilder(2).writeDouble(500 + delta);
                  builder.declarePosition();

                  index++;
                  return builder.build();
                }

                @Override
                public boolean hasNext() {
                  return index < 3;
                }

                @Override
                public boolean isFinished() {
                  return index >= 3;
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
              });

      int count = 0;
      double[][][] res =
          new double[][][] {
            {
              {1.0, 520.0, 100.0},
              {2, 20, 200},
              {3, 30, 520.0},
              {520.0, 40, 520.0},
              {520.0, 520.0, 500}
            },
            {
              {10001, 520.0, 10100},
              {10002, 10020, 10200},
              {10003, 10030, 520.0},
              {520.0, 10040, 520.0},
              {520.0, 520.0, 10500}
            },
            {
              {20001, 520.0, 20100},
              {20002, 20020, 20200},
              {20003, 20030, 520.0},
              {520.0, 20040, 520.0},
              {520.0, 520.0, 20500}
            }
          };
      boolean[][][] isNull =
          new boolean[][][] {
            {
              {false, false, false},
              {false, false, false},
              {false, false, false},
              {false, false, false},
              {false, false, false}
            },
            {
              {false, false, false},
              {false, false, false},
              {false, false, false},
              {false, false, false},
              {false, false, false}
            },
            {
              {false, false, false},
              {false, false, false},
              {false, false, false},
              {false, false, false},
              {false, false, false}
            }
          };
      while (fillOperator.hasNext()) {
        TsBlock block = fillOperator.next();
        for (int i = 0; i < block.getPositionCount(); i++) {
          long expectedTime = i + 1 + count * 10000L;
          assertEquals(expectedTime, block.getTimeByIndex(i));
          for (int j = 0; j < 3; j++) {
            assertEquals(isNull[count][i][j], block.getColumn(j).isNull(i));
            if (!isNull[count][i][j]) {
              assertEquals(res[count][i][j], block.getColumn(j).getDouble(i), 0.00001);
            }
          }
        }
        count++;
      }

      assertTrue(fillOperator.isFinished());
      assertEquals(3, count);

    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void batchPreviousFillTest() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
      PlanNodeId planNodeId1 = new PlanNodeId("1");
      driverContext.addOperatorContext(1, planNodeId1, FillOperator.class.getSimpleName());

      IFill[] fillArray =
          new IFill[] {new IntPreviousFill(), new IntPreviousFill(), new IntPreviousFill()};
      FillOperator fillOperator =
          new FillOperator(
              driverContext.getOperatorContexts().get(0),
              fillArray,
              new Operator() {
                private int index = 0;

                @Override
                public OperatorContext getOperatorContext() {
                  return driverContext.getOperatorContexts().get(0);
                }

                @Override
                public TsBlock next() {
                  int delta = index * 10000;
                  TsBlockBuilder builder =
                      new TsBlockBuilder(
                          ImmutableList.of(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32));
                  // 1  1, null, 100
                  builder.getTimeColumnBuilder().writeLong(1 + delta);
                  builder.getColumnBuilder(0).writeInt(1 + delta);
                  builder.getColumnBuilder(1).appendNull();
                  builder.getColumnBuilder(2).writeInt(100 + delta);
                  builder.declarePosition();
                  // 2  2, 20, 200
                  builder.getTimeColumnBuilder().writeLong(2 + delta);
                  builder.getColumnBuilder(0).writeInt(2 + delta);
                  builder.getColumnBuilder(1).writeInt(20 + delta);
                  builder.getColumnBuilder(2).writeInt(200 + delta);
                  builder.declarePosition();
                  // 3  3, 30, null
                  builder.getTimeColumnBuilder().writeLong(3 + delta);
                  builder.getColumnBuilder(0).writeInt(3 + delta);
                  builder.getColumnBuilder(1).writeInt(30 + delta);
                  builder.getColumnBuilder(2).appendNull();
                  builder.declarePosition();
                  // 4  null, 40, null
                  builder.getTimeColumnBuilder().writeLong(4 + delta);
                  builder.getColumnBuilder(0).appendNull();
                  builder.getColumnBuilder(1).writeInt(40 + delta);
                  builder.getColumnBuilder(2).appendNull();
                  builder.declarePosition();
                  // 5  null, null, 500
                  builder.getTimeColumnBuilder().writeLong(5 + delta);
                  builder.getColumnBuilder(0).appendNull();
                  builder.getColumnBuilder(1).appendNull();
                  builder.getColumnBuilder(2).writeInt(500 + delta);
                  builder.declarePosition();

                  index++;
                  return builder.build();
                }

                @Override
                public boolean hasNext() {
                  return index < 3;
                }

                @Override
                public boolean isFinished() {
                  return index >= 3;
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
              });

      int count = 0;
      int[][][] res =
          new int[][][] {
            {{1, 0, 100}, {2, 20, 200}, {3, 30, 200}, {3, 40, 200}, {3, 40, 500}},
            {
              {10001, 40, 10100},
              {10002, 10020, 10200},
              {10003, 10030, 10200},
              {10003, 10040, 10200},
              {10003, 10040, 10500}
            },
            {
              {20001, 10040, 20100},
              {20002, 20020, 20200},
              {20003, 20030, 20200},
              {20003, 20040, 20200},
              {20003, 20040, 20500}
            }
          };
      boolean[][][] isNull =
          new boolean[][][] {
            {
              {false, true, false},
              {false, false, false},
              {false, false, false},
              {false, false, false},
              {false, false, false}
            },
            {
              {false, false, false},
              {false, false, false},
              {false, false, false},
              {false, false, false},
              {false, false, false}
            },
            {
              {false, false, false},
              {false, false, false},
              {false, false, false},
              {false, false, false},
              {false, false, false}
            }
          };
      while (fillOperator.hasNext()) {
        TsBlock block = fillOperator.next();
        for (int i = 0; i < block.getPositionCount(); i++) {
          long expectedTime = i + 1 + count * 10000L;
          assertEquals(expectedTime, block.getTimeByIndex(i));
          for (int j = 0; j < 3; j++) {
            assertEquals(isNull[count][i][j], block.getColumn(j).isNull(i));
            if (!isNull[count][i][j]) {
              assertEquals(res[count][i][j], block.getColumn(j).getInt(i));
            }
          }
        }
        count++;
      }

      assertTrue(fillOperator.isFinished());
      assertEquals(3, count);

    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }
}
