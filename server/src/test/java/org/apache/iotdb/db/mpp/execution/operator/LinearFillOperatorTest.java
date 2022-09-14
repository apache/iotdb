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
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.process.LinearFillOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.ILinearFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.identity.IdentityLinearFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.linear.FloatLinearFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.linear.LinearFill;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class LinearFillOperatorTest {

  @Test
  public void batchLinearFillTest1() {
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
      PlanNodeId planNodeId1 = new PlanNodeId("1");
      fragmentInstanceContext.addOperatorContext(
          1, planNodeId1, LinearFillOperator.class.getSimpleName());

      LinearFill[] fillArray =
          new LinearFill[] {
            new FloatLinearFill(),
            new FloatLinearFill(),
            new FloatLinearFill(),
            new FloatLinearFill()
          };
      LinearFillOperator fillOperator =
          new LinearFillOperator(
              fragmentInstanceContext.getOperatorContexts().get(0),
              fillArray,
              new Operator() {
                private int index = 0;
                private final float[][][] value =
                    new float[][][] {
                      {
                        {1.0f, 0.0f, 3.0f, 4.0f},
                        {11.0f, 12.0f, 13.0f, 0.0f},
                        {21.0f, 22.0f, 0.0f, 0.0f},
                        {0.0f, 32.0f, 0.0f, 0.0f},
                        {0.0f, 0.0f, 43.0f, 0.0f}
                      },
                      {
                        {51.0f, 0.0f, 53.0f, 0.0f},
                        {61.0f, 62.0f, 63.0f, 0.0f},
                        {71.0f, 72.0f, 0.0f, 74.0f},
                        {0.0f, 82.0f, 0.0f, 0.0f},
                        {0.0f, 0.0f, 93.0f, 0.0f}
                      },
                      {
                        {101.0f, 0.0f, 103.0f, 0.0f},
                        {111.0f, 112.0f, 113.0f, 114.0f},
                        {121.0f, 122.0f, 0.0f, 124.0f},
                        {0.0f, 132.0f, 0.0f, 0.0f},
                        {0.0f, 0.0f, 143.0f, 0.0f}
                      }
                    };
                final boolean[][][] isNull =
                    new boolean[][][] {
                      {
                        {false, true, false, false},
                        {false, false, false, true},
                        {false, false, true, true},
                        {true, false, true, true},
                        {true, true, false, true}
                      },
                      {
                        {false, true, false, true},
                        {false, false, false, true},
                        {false, false, true, false},
                        {true, false, true, true},
                        {true, true, false, true}
                      },
                      {
                        {false, true, false, true},
                        {false, false, false, false},
                        {false, false, true, false},
                        {true, false, true, true},
                        {true, true, false, true}
                      }
                    };

                @Override
                public OperatorContext getOperatorContext() {
                  return null;
                }

                @Override
                public TsBlock next() {
                  TsBlockBuilder builder =
                      new TsBlockBuilder(
                          ImmutableList.of(
                              TSDataType.FLOAT,
                              TSDataType.FLOAT,
                              TSDataType.FLOAT,
                              TSDataType.FLOAT));
                  for (int i = 0; i < 5; i++) {
                    builder.getTimeColumnBuilder().writeLong(i + index * 5L);
                    for (int j = 0; j < 4; j++) {
                      if (isNull[index][i][j]) {
                        builder.getColumnBuilder(j).appendNull();
                      } else {
                        builder.getColumnBuilder(j).writeFloat(value[index][i][j]);
                      }
                    }
                    builder.declarePosition();
                  }
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
      float[][][] res =
          new float[][][] {
            {
              {1.0f, 0.0f, 3.0f, 4.0f},
              {11.0f, 12.0f, 13.0f, 39.0f},
              {21.0f, 22.0f, 28.0f, 39.0f},
              {36.0f, 32.0f, 28.0f, 39.0f},
              {36.0f, 47.0f, 43.0f, 39.0f}
            },
            {
              {51.0f, 47.0f, 53.0f, 39.0f},
              {61.0f, 62.0f, 63.0f, 39.0f},
              {71.0f, 72.0f, 78.0f, 74.0f},
              {86.0f, 82.0f, 78.0f, 94.0f},
              {86.0f, 97.0f, 93.0f, 94.0f}
            },
            {
              {101.0f, 97.0f, 103.0f, 94.0f},
              {111.0f, 112.0f, 113.0f, 114.0f},
              {121.0f, 122.0f, 128.0f, 124.0f},
              {0.0f, 132.0f, 128.0f, 0.0f},
              {0.0f, 0.0f, 143.0f, 0.0f}
            }
          };
      boolean[][][] isNull =
          new boolean[][][] {
            {
              {false, true, false, false},
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false}
            },
            {
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false}
            },
            {
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false},
              {true, false, false, true},
              {true, true, false, true}
            }
          };

      boolean[] nullBlock = new boolean[] {true, false, false, false};
      int nullBlockIndex = 0;
      while (fillOperator.hasNext()) {
        TsBlock block = fillOperator.next();
        assertEquals(nullBlock[nullBlockIndex++], block == null);
        if (block == null) {
          continue;
        }
        for (int i = 0; i < block.getPositionCount(); i++) {
          long expectedTime = i + count * 5L;
          assertEquals(expectedTime, block.getTimeByIndex(i));
          for (int j = 0; j < 4; j++) {
            assertEquals(isNull[count][i][j], block.getColumn(j).isNull(i));
            if (!isNull[count][i][j]) {
              assertEquals(res[count][i][j], block.getColumn(j).getFloat(i), 0.00001f);
            }
          }
        }
        count++;
      }

      assertTrue(fillOperator.isFinished());
      assertEquals(res.length, count);
      assertEquals(nullBlock.length, nullBlockIndex);

    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void batchLinearFillTest1OrderByDesc() {
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
      PlanNodeId planNodeId1 = new PlanNodeId("1");
      fragmentInstanceContext.addOperatorContext(
          1, planNodeId1, LinearFillOperator.class.getSimpleName());

      LinearFill[] fillArray =
          new LinearFill[] {
            new FloatLinearFill(),
            new FloatLinearFill(),
            new FloatLinearFill(),
            new FloatLinearFill()
          };
      LinearFillOperator fillOperator =
          new LinearFillOperator(
              fragmentInstanceContext.getOperatorContexts().get(0),
              fillArray,
              new Operator() {
                private int index = 0;
                private final float[][][] value =
                    new float[][][] {
                      {
                        {1.0f, 0.0f, 3.0f, 4.0f},
                        {11.0f, 12.0f, 13.0f, 0.0f},
                        {21.0f, 22.0f, 0.0f, 0.0f},
                        {0.0f, 32.0f, 0.0f, 0.0f},
                        {0.0f, 0.0f, 43.0f, 0.0f}
                      },
                      {
                        {51.0f, 0.0f, 53.0f, 0.0f},
                        {61.0f, 62.0f, 63.0f, 0.0f},
                        {71.0f, 72.0f, 0.0f, 74.0f},
                        {0.0f, 82.0f, 0.0f, 0.0f},
                        {0.0f, 0.0f, 93.0f, 0.0f}
                      },
                      {
                        {101.0f, 0.0f, 103.0f, 0.0f},
                        {111.0f, 112.0f, 113.0f, 114.0f},
                        {121.0f, 122.0f, 0.0f, 124.0f},
                        {0.0f, 132.0f, 0.0f, 0.0f},
                        {0.0f, 0.0f, 143.0f, 0.0f}
                      }
                    };
                final boolean[][][] isNull =
                    new boolean[][][] {
                      {
                        {false, true, false, false},
                        {false, false, false, true},
                        {false, false, true, true},
                        {true, false, true, true},
                        {true, true, false, true}
                      },
                      {
                        {false, true, false, true},
                        {false, false, false, true},
                        {false, false, true, false},
                        {true, false, true, true},
                        {true, true, false, true}
                      },
                      {
                        {false, true, false, true},
                        {false, false, false, false},
                        {false, false, true, false},
                        {true, false, true, true},
                        {true, true, false, true}
                      }
                    };

                @Override
                public OperatorContext getOperatorContext() {
                  return null;
                }

                @Override
                public TsBlock next() {
                  TsBlockBuilder builder =
                      new TsBlockBuilder(
                          ImmutableList.of(
                              TSDataType.FLOAT,
                              TSDataType.FLOAT,
                              TSDataType.FLOAT,
                              TSDataType.FLOAT));
                  for (int i = 0; i < 5; i++) {
                    builder.getTimeColumnBuilder().writeLong((4 - i) + (2 - index) * 5L);
                    for (int j = 0; j < 4; j++) {
                      if (isNull[index][i][j]) {
                        builder.getColumnBuilder(j).appendNull();
                      } else {
                        builder.getColumnBuilder(j).writeFloat(value[index][i][j]);
                      }
                    }
                    builder.declarePosition();
                  }
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

      float[][][] res =
          new float[][][] {
            {
              {1.0f, 0.0f, 3.0f, 4.0f},
              {11.0f, 12.0f, 13.0f, 39.0f},
              {21.0f, 22.0f, 28.0f, 39.0f},
              {36.0f, 32.0f, 28.0f, 39.0f},
              {36.0f, 47.0f, 43.0f, 39.0f}
            },
            {
              {51.0f, 47.0f, 53.0f, 39.0f},
              {61.0f, 62.0f, 63.0f, 39.0f},
              {71.0f, 72.0f, 78.0f, 74.0f},
              {86.0f, 82.0f, 78.0f, 94.0f},
              {86.0f, 97.0f, 93.0f, 94.0f}
            },
            {
              {101.0f, 97.0f, 103.0f, 94.0f},
              {111.0f, 112.0f, 113.0f, 114.0f},
              {121.0f, 122.0f, 128.0f, 124.0f},
              {0.0f, 132.0f, 128.0f, 0.0f},
              {0.0f, 0.0f, 143.0f, 0.0f}
            }
          };
      boolean[][][] isNull =
          new boolean[][][] {
            {
              {false, true, false, false},
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false}
            },
            {
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false}
            },
            {
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false},
              {true, false, false, true},
              {true, true, false, true}
            }
          };

      boolean[] nullBlock = new boolean[] {true, false, false, false};
      int nullBlockIndex = 0;
      int count = 0;
      while (fillOperator.hasNext()) {
        TsBlock block = fillOperator.next();
        assertEquals(nullBlock[nullBlockIndex++], block == null);
        if (block == null) {
          continue;
        }
        for (int i = 0; i < block.getPositionCount(); i++) {
          long expectedTime = (block.getPositionCount() - i - 1) + (res.length - count - 1) * 5L;
          assertEquals(expectedTime, block.getTimeByIndex(i));
          for (int j = 0; j < 4; j++) {
            assertEquals(isNull[count][i][j], block.getColumn(j).isNull(i));
            if (!isNull[count][i][j]) {
              assertEquals(res[count][i][j], block.getColumn(j).getFloat(i), 0.00001f);
            }
          }
        }
        count++;
      }

      assertTrue(fillOperator.isFinished());
      assertEquals(res.length, count);
      assertEquals(nullBlock.length, nullBlockIndex);

    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void batchLinearFillTest2() {
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
      PlanNodeId planNodeId1 = new PlanNodeId("1");
      fragmentInstanceContext.addOperatorContext(
          1, planNodeId1, LinearFillOperator.class.getSimpleName());

      LinearFill[] fillArray =
          new LinearFill[] {
            new FloatLinearFill(),
            new FloatLinearFill(),
            new FloatLinearFill(),
            new FloatLinearFill()
          };
      LinearFillOperator fillOperator =
          new LinearFillOperator(
              fragmentInstanceContext.getOperatorContexts().get(0),
              fillArray,
              new Operator() {
                private int index = 0;
                private final float[][][] value =
                    new float[][][] {
                      {
                        {1.0f, 0.0f, 3.0f, 4.0f},
                        {11.0f, 12.0f, 13.0f, 0.0f},
                        {21.0f, 22.0f, 0.0f, 0.0f},
                        {0.0f, 32.0f, 0.0f, 0.0f},
                        {0.0f, 0.0f, 0.0f, 0.0f}
                      },
                      {
                        {51.0f, 0.0f, 0.0f, 0.0f},
                        {61.0f, 62.0f, 0.0f, 0.0f},
                        {71.0f, 72.0f, 0.0f, 74.0f},
                        {0.0f, 82.0f, 0.0f, 0.0f},
                        {0.0f, 0.0f, 0.0f, 0.0f}
                      },
                      {
                        {101.0f, 0.0f, 103.0f, 0.0f},
                        {111.0f, 112.0f, 0.0f, 114.0f},
                        {121.0f, 122.0f, 0.0f, 124.0f},
                        {0.0f, 132.0f, 0.0f, 0.0f},
                        {0.0f, 0.0f, 0.0f, 0.0f}
                      }
                    };
                final boolean[][][] isNull =
                    new boolean[][][] {
                      {
                        {false, true, false, false},
                        {false, false, false, true},
                        {false, false, true, true},
                        {true, false, true, true},
                        {true, true, true, true}
                      },
                      {
                        {false, true, true, true},
                        {false, false, true, true},
                        {false, false, true, false},
                        {true, false, true, true},
                        {true, true, true, true}
                      },
                      {
                        {false, true, false, true},
                        {false, false, true, false},
                        {false, false, true, false},
                        {true, false, true, true},
                        {true, true, true, true}
                      }
                    };

                @Override
                public OperatorContext getOperatorContext() {
                  return null;
                }

                @Override
                public TsBlock next() {
                  TsBlockBuilder builder =
                      new TsBlockBuilder(
                          ImmutableList.of(
                              TSDataType.FLOAT,
                              TSDataType.FLOAT,
                              TSDataType.FLOAT,
                              TSDataType.FLOAT));
                  for (int i = 0; i < 5; i++) {
                    builder.getTimeColumnBuilder().writeLong(i + index * 5L);
                    for (int j = 0; j < 4; j++) {
                      if (isNull[index][i][j]) {
                        builder.getColumnBuilder(j).appendNull();
                      } else {
                        builder.getColumnBuilder(j).writeFloat(value[index][i][j]);
                      }
                    }
                    builder.declarePosition();
                  }
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
      float[][][] res =
          new float[][][] {
            {
              {1.0f, 0.0f, 3.0f, 4.0f},
              {11.0f, 12.0f, 13.0f, 39.0f},
              {21.0f, 22.0f, 58.0f, 39.0f},
              {36.0f, 32.0f, 58.0f, 39.0f},
              {36.0f, 47.0f, 58.0f, 39.0f}
            },
            {
              {51.0f, 47.0f, 58.0f, 39.0f},
              {61.0f, 62.0f, 58.0f, 39.0f},
              {71.0f, 72.0f, 58.0f, 74.0f},
              {86.0f, 82.0f, 58.0f, 94.0f},
              {86.0f, 97.0f, 58.0f, 94.0f}
            },
            {
              {101.0f, 97.0f, 103.0f, 94.0f},
              {111.0f, 112.0f, 0.0f, 114.0f},
              {121.0f, 122.0f, 0.0f, 124.0f},
              {0.0f, 132.0f, 0.0f, 0.0f},
              {0.0f, 0.0f, 0.0f, 0.0f}
            }
          };
      boolean[][][] isNull =
          new boolean[][][] {
            {
              {false, true, false, false},
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false}
            },
            {
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false}
            },
            {
              {false, false, false, false},
              {false, false, true, false},
              {false, false, true, false},
              {true, false, true, true},
              {true, true, true, true}
            }
          };

      boolean[] nullBlock = new boolean[] {true, true, false, false, false};
      int nullBlockIndex = 0;
      while (fillOperator.hasNext()) {
        TsBlock block = fillOperator.next();
        assertEquals(nullBlock[nullBlockIndex++], block == null);
        if (block == null) {
          continue;
        }
        for (int i = 0; i < block.getPositionCount(); i++) {
          long expectedTime = i + count * 5L;
          assertEquals(expectedTime, block.getTimeByIndex(i));
          for (int j = 0; j < 4; j++) {
            assertEquals(isNull[count][i][j], block.getColumn(j).isNull(i));
            if (!isNull[count][i][j]) {
              assertEquals(res[count][i][j], block.getColumn(j).getFloat(i), 0.00001f);
            }
          }
        }
        count++;
      }

      assertTrue(fillOperator.isFinished());
      assertEquals(res.length, count);
      assertEquals(nullBlock.length, nullBlockIndex);

    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void batchLinearFillTest2OrderByDesc() {
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
      PlanNodeId planNodeId1 = new PlanNodeId("1");
      fragmentInstanceContext.addOperatorContext(
          1, planNodeId1, LinearFillOperator.class.getSimpleName());

      LinearFill[] fillArray =
          new LinearFill[] {
            new FloatLinearFill(),
            new FloatLinearFill(),
            new FloatLinearFill(),
            new FloatLinearFill()
          };
      LinearFillOperator fillOperator =
          new LinearFillOperator(
              fragmentInstanceContext.getOperatorContexts().get(0),
              fillArray,
              new Operator() {
                private int index = 0;
                private final float[][][] value =
                    new float[][][] {
                      {
                        {1.0f, 0.0f, 3.0f, 4.0f},
                        {11.0f, 12.0f, 13.0f, 0.0f},
                        {21.0f, 22.0f, 0.0f, 0.0f},
                        {0.0f, 32.0f, 0.0f, 0.0f},
                        {0.0f, 0.0f, 0.0f, 0.0f}
                      },
                      {
                        {51.0f, 0.0f, 0.0f, 0.0f},
                        {61.0f, 62.0f, 0.0f, 0.0f},
                        {71.0f, 72.0f, 0.0f, 74.0f},
                        {0.0f, 82.0f, 0.0f, 0.0f},
                        {0.0f, 0.0f, 0.0f, 0.0f}
                      },
                      {
                        {101.0f, 0.0f, 103.0f, 0.0f},
                        {111.0f, 112.0f, 0.0f, 114.0f},
                        {121.0f, 122.0f, 0.0f, 124.0f},
                        {0.0f, 132.0f, 0.0f, 0.0f},
                        {0.0f, 0.0f, 0.0f, 0.0f}
                      }
                    };
                final boolean[][][] isNull =
                    new boolean[][][] {
                      {
                        {false, true, false, false},
                        {false, false, false, true},
                        {false, false, true, true},
                        {true, false, true, true},
                        {true, true, true, true}
                      },
                      {
                        {false, true, true, true},
                        {false, false, true, true},
                        {false, false, true, false},
                        {true, false, true, true},
                        {true, true, true, true}
                      },
                      {
                        {false, true, false, true},
                        {false, false, true, false},
                        {false, false, true, false},
                        {true, false, true, true},
                        {true, true, true, true}
                      }
                    };

                @Override
                public OperatorContext getOperatorContext() {
                  return null;
                }

                @Override
                public TsBlock next() {
                  TsBlockBuilder builder =
                      new TsBlockBuilder(
                          ImmutableList.of(
                              TSDataType.FLOAT,
                              TSDataType.FLOAT,
                              TSDataType.FLOAT,
                              TSDataType.FLOAT));
                  for (int i = 0; i < 5; i++) {
                    builder.getTimeColumnBuilder().writeLong((4 - i) + (2 - index) * 5L);
                    for (int j = 0; j < 4; j++) {
                      if (isNull[index][i][j]) {
                        builder.getColumnBuilder(j).appendNull();
                      } else {
                        builder.getColumnBuilder(j).writeFloat(value[index][i][j]);
                      }
                    }
                    builder.declarePosition();
                  }
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
      float[][][] res =
          new float[][][] {
            {
              {1.0f, 0.0f, 3.0f, 4.0f},
              {11.0f, 12.0f, 13.0f, 39.0f},
              {21.0f, 22.0f, 58.0f, 39.0f},
              {36.0f, 32.0f, 58.0f, 39.0f},
              {36.0f, 47.0f, 58.0f, 39.0f}
            },
            {
              {51.0f, 47.0f, 58.0f, 39.0f},
              {61.0f, 62.0f, 58.0f, 39.0f},
              {71.0f, 72.0f, 58.0f, 74.0f},
              {86.0f, 82.0f, 58.0f, 94.0f},
              {86.0f, 97.0f, 58.0f, 94.0f}
            },
            {
              {101.0f, 97.0f, 103.0f, 94.0f},
              {111.0f, 112.0f, 0.0f, 114.0f},
              {121.0f, 122.0f, 0.0f, 124.0f},
              {0.0f, 132.0f, 0.0f, 0.0f},
              {0.0f, 0.0f, 0.0f, 0.0f}
            }
          };
      boolean[][][] isNull =
          new boolean[][][] {
            {
              {false, true, false, false},
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false}
            },
            {
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false},
              {false, false, false, false}
            },
            {
              {false, false, false, false},
              {false, false, true, false},
              {false, false, true, false},
              {true, false, true, true},
              {true, true, true, true}
            }
          };

      boolean[] nullBlock = new boolean[] {true, true, false, false, false};
      int nullBlockIndex = 0;
      while (fillOperator.hasNext()) {
        TsBlock block = fillOperator.next();
        assertEquals(nullBlock[nullBlockIndex++], block == null);
        if (block == null) {
          continue;
        }
        for (int i = 0; i < block.getPositionCount(); i++) {
          long expectedTime = (block.getPositionCount() - i - 1) + (res.length - count - 1) * 5L;
          assertEquals(expectedTime, block.getTimeByIndex(i));
          for (int j = 0; j < 4; j++) {
            assertEquals(isNull[count][i][j], block.getColumn(j).isNull(i));
            if (!isNull[count][i][j]) {
              assertEquals(res[count][i][j], block.getColumn(j).getFloat(i), 0.00001f);
            }
          }
        }
        count++;
      }

      assertTrue(fillOperator.isFinished());
      assertEquals(res.length, count);
      assertEquals(nullBlock.length, nullBlockIndex);

    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void batchLinearFillTest3() {
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
      PlanNodeId planNodeId1 = new PlanNodeId("1");
      fragmentInstanceContext.addOperatorContext(
          1, planNodeId1, LinearFillOperator.class.getSimpleName());

      LinearFill[] fillArray = new LinearFill[] {new FloatLinearFill()};
      LinearFillOperator fillOperator =
          new LinearFillOperator(
              fragmentInstanceContext.getOperatorContexts().get(0),
              fillArray,
              new Operator() {
                private int index = 0;
                private final float[][][] value =
                    new float[][][] {
                      {{0.0f}}, {{2.0f}}, {{3.0f}}, {{4.0f}}, {{0.0f}}, {{0.0f}}, {{0.0f}}
                    };
                final boolean[][][] isNull =
                    new boolean[][][] {
                      {{true}}, {{false}}, {{false}}, {{false}}, {{true}}, {{true}}, {{true}}
                    };

                @Override
                public OperatorContext getOperatorContext() {
                  return null;
                }

                @Override
                public TsBlock next() {
                  TsBlockBuilder builder = new TsBlockBuilder(ImmutableList.of(TSDataType.FLOAT));
                  for (int i = 0; i < 1; i++) {
                    builder.getTimeColumnBuilder().writeLong(i + index);
                    for (int j = 0; j < 1; j++) {
                      if (isNull[index][i][j]) {
                        builder.getColumnBuilder(j).appendNull();
                      } else {
                        builder.getColumnBuilder(j).writeFloat(value[index][i][j]);
                      }
                    }
                    builder.declarePosition();
                  }
                  index++;
                  return builder.build();
                }

                @Override
                public boolean hasNext() {
                  return index < 7;
                }

                @Override
                public boolean isFinished() {
                  return index >= 7;
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
      float[][][] res =
          new float[][][] {{{0.0f}}, {{2.0f}}, {{3.0f}}, {{4.0f}}, {{0.0f}}, {{0.0f}}, {{0.0f}}};
      boolean[][][] isNull =
          new boolean[][][] {
            {{true}}, {{false}}, {{false}}, {{false}}, {{true}}, {{true}}, {{true}}
          };

      boolean[] nullBlock =
          new boolean[] {true, false, false, false, false, true, true, true, false, false, false};
      int nullBlockIndex = 0;
      while (fillOperator.hasNext()) {
        TsBlock block = fillOperator.next();
        assertEquals(nullBlock[nullBlockIndex++], block == null);
        if (block == null) {
          continue;
        }
        for (int i = 0; i < block.getPositionCount(); i++) {
          long expectedTime = i + count;
          assertEquals(expectedTime, block.getTimeByIndex(i));
          for (int j = 0; j < 1; j++) {
            assertEquals(isNull[count][i][j], block.getColumn(j).isNull(i));
            if (!isNull[count][i][j]) {
              assertEquals(res[count][i][j], block.getColumn(j).getFloat(i), 0.00001f);
            }
          }
        }
        count++;
      }

      assertTrue(fillOperator.isFinished());
      assertEquals(res.length, count);
      assertEquals(nullBlock.length, nullBlockIndex);

    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void batchLinearFillTest3OrderByDesc() {
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
      PlanNodeId planNodeId1 = new PlanNodeId("1");
      fragmentInstanceContext.addOperatorContext(
          1, planNodeId1, LinearFillOperator.class.getSimpleName());

      LinearFill[] fillArray = new LinearFill[] {new FloatLinearFill()};
      LinearFillOperator fillOperator =
          new LinearFillOperator(
              fragmentInstanceContext.getOperatorContexts().get(0),
              fillArray,
              new Operator() {
                private int index = 0;
                private final float[][][] value =
                    new float[][][] {
                      {{0.0f}}, {{2.0f}}, {{3.0f}}, {{4.0f}}, {{0.0f}}, {{0.0f}}, {{0.0f}}
                    };
                final boolean[][][] isNull =
                    new boolean[][][] {
                      {{true}}, {{false}}, {{false}}, {{false}}, {{true}}, {{true}}, {{true}}
                    };

                @Override
                public OperatorContext getOperatorContext() {
                  return null;
                }

                @Override
                public TsBlock next() {
                  TsBlockBuilder builder = new TsBlockBuilder(ImmutableList.of(TSDataType.FLOAT));
                  for (int i = 0; i < 1; i++) {
                    builder.getTimeColumnBuilder().writeLong(i + (6 - index));
                    for (int j = 0; j < 1; j++) {
                      if (isNull[index][i][j]) {
                        builder.getColumnBuilder(j).appendNull();
                      } else {
                        builder.getColumnBuilder(j).writeFloat(value[index][i][j]);
                      }
                    }
                    builder.declarePosition();
                  }
                  index++;
                  return builder.build();
                }

                @Override
                public boolean hasNext() {
                  return index < 7;
                }

                @Override
                public boolean isFinished() {
                  return index >= 7;
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
      float[][][] res =
          new float[][][] {{{0.0f}}, {{2.0f}}, {{3.0f}}, {{4.0f}}, {{0.0f}}, {{0.0f}}, {{0.0f}}};
      boolean[][][] isNull =
          new boolean[][][] {
            {{true}}, {{false}}, {{false}}, {{false}}, {{true}}, {{true}}, {{true}}
          };

      boolean[] nullBlock =
          new boolean[] {true, false, false, false, false, true, true, true, false, false, false};
      int nullBlockIndex = 0;
      while (fillOperator.hasNext()) {
        TsBlock block = fillOperator.next();
        assertEquals(nullBlock[nullBlockIndex++], block == null);
        if (block == null) {
          continue;
        }
        for (int i = 0; i < block.getPositionCount(); i++) {
          long expectedTime = (block.getPositionCount() - i - 1) + (res.length - count - 1);
          assertEquals(expectedTime, block.getTimeByIndex(i));
          for (int j = 0; j < 1; j++) {
            assertEquals(isNull[count][i][j], block.getColumn(j).isNull(i));
            if (!isNull[count][i][j]) {
              assertEquals(res[count][i][j], block.getColumn(j).getFloat(i), 0.00001f);
            }
          }
        }
        count++;
      }

      assertTrue(fillOperator.isFinished());
      assertEquals(res.length, count);
      assertEquals(nullBlock.length, nullBlockIndex);

    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void batchLinearFillBooleanTest() {
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
      PlanNodeId planNodeId1 = new PlanNodeId("1");
      fragmentInstanceContext.addOperatorContext(
          1, planNodeId1, LinearFillOperator.class.getSimpleName());

      ILinearFill[] fillArray = new ILinearFill[] {new IdentityLinearFill()};
      LinearFillOperator fillOperator =
          new LinearFillOperator(
              fragmentInstanceContext.getOperatorContexts().get(0),
              fillArray,
              new Operator() {
                private int index = 0;
                private final boolean[][][] value =
                    new boolean[][][] {
                      {{true}}, {{true}}, {{false}}, {{false}}, {{true}}, {{false}}, {{true}}
                    };
                final boolean[][][] isNull =
                    new boolean[][][] {
                      {{true}}, {{false}}, {{false}}, {{false}}, {{true}}, {{true}}, {{true}}
                    };

                @Override
                public OperatorContext getOperatorContext() {
                  return null;
                }

                @Override
                public TsBlock next() {
                  TsBlockBuilder builder = new TsBlockBuilder(ImmutableList.of(TSDataType.BOOLEAN));
                  for (int i = 0; i < 1; i++) {
                    builder.getTimeColumnBuilder().writeLong(i + index);
                    for (int j = 0; j < 1; j++) {
                      if (isNull[index][i][j]) {
                        builder.getColumnBuilder(j).appendNull();
                      } else {
                        builder.getColumnBuilder(j).writeBoolean(value[index][i][j]);
                      }
                    }
                    builder.declarePosition();
                  }
                  index++;
                  return builder.build();
                }

                @Override
                public boolean hasNext() {
                  return index < 7;
                }

                @Override
                public boolean isFinished() {
                  return index >= 7;
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
      boolean[][][] res =
          new boolean[][][] {
            {{true}}, {{true}}, {{false}}, {{false}}, {{true}}, {{false}}, {{true}}
          };
      boolean[][][] isNull =
          new boolean[][][] {
            {{true}}, {{false}}, {{false}}, {{false}}, {{true}}, {{true}}, {{true}}
          };

      while (fillOperator.hasNext()) {
        TsBlock block = fillOperator.next();
        assertNotNull(block);
        for (int i = 0; i < block.getPositionCount(); i++) {
          long expectedTime = i + count;
          assertEquals(expectedTime, block.getTimeByIndex(i));
          for (int j = 0; j < 1; j++) {
            assertEquals(isNull[count][i][j], block.getColumn(j).isNull(i));
            if (!isNull[count][i][j]) {
              assertEquals(res[count][i][j], block.getColumn(j).getBoolean(i));
            }
          }
        }
        count++;
      }

      assertTrue(fillOperator.isFinished());
      assertEquals(res.length, count);

    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }
}
