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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ExchangeOperatorTest {

  private ExecutorService instanceNotificationExecutor;

  @Before
  public void setUp() {
    instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-exchange-node-notification");
  }

  @After
  public void tearDown() {
    instanceNotificationExecutor.shutdown();
  }

  private OperatorContext createOperatorContext() {
    QueryId queryId = new QueryId("test_exchange_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "test-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    driverContext.addOperatorContext(
        1, new PlanNodeId("1"), ExchangeOperator.class.getSimpleName());
    return driverContext.getOperatorContexts().get(0);
  }

  /**
   * When sourceHandle.receive() returns a non-null TsBlock, receivedSizeInBytes should increase.
   */
  @Test
  public void testNextAccumulatesReceivedSizeWhenBlockIsNotNull() throws Exception {
    ISourceHandle sourceHandle = Mockito.mock(ISourceHandle.class);
    TsBlock tsBlock = buildTsBlock(10);
    Mockito.when(sourceHandle.receive()).thenReturn(tsBlock);

    try (ExchangeOperator operator =
        new ExchangeOperator(createOperatorContext(), sourceHandle, new PlanNodeId("source1")); ) {

      TsBlock result = operator.next();
      assertEquals(tsBlock, result);
      long expectedSize = tsBlock.getSizeInBytes();
      assertEquals(
          expectedSize,
          ((java.util.concurrent.atomic.AtomicLong)
                  operator
                      .getOperatorContext()
                      .getSpecifiedInfo()
                      .get(ExchangeOperator.SIZE_IN_BYTES))
              .get());
    }
  }

  /** When sourceHandle.receive() returns null, receivedSizeInBytes should remain 0. */
  @Test
  public void testNextDoesNotAccumulateWhenBlockIsNull() throws Exception {
    ISourceHandle sourceHandle = Mockito.mock(ISourceHandle.class);
    Mockito.when(sourceHandle.receive()).thenReturn(null);

    try (ExchangeOperator operator =
        new ExchangeOperator(createOperatorContext(), sourceHandle, new PlanNodeId("source2"))) {

      TsBlock result = operator.next();

      assertNull(result);
      assertEquals(
          0L,
          ((java.util.concurrent.atomic.AtomicLong)
                  operator
                      .getOperatorContext()
                      .getSpecifiedInfo()
                      .get(ExchangeOperator.SIZE_IN_BYTES))
              .get());
    }
  }

  /** Multiple calls to next() should accumulate sizes correctly. */
  @Test
  public void testNextAccumulatesAcrossMultipleCalls() throws Exception {
    ISourceHandle sourceHandle = Mockito.mock(ISourceHandle.class);
    TsBlock block1 = buildTsBlock(5);
    TsBlock block2 = buildTsBlock(8);
    Mockito.when(sourceHandle.receive()).thenReturn(block1).thenReturn(null).thenReturn(block2);

    ExchangeOperator operator =
        new ExchangeOperator(createOperatorContext(), sourceHandle, new PlanNodeId("source3"));

    operator.next(); // block1 — should accumulate
    operator.next(); // null   — should not accumulate
    operator.next(); // block2 — should accumulate

    long expectedSize = block1.getSizeInBytes() + block2.getSizeInBytes();
    assertEquals(
        expectedSize,
        ((java.util.concurrent.atomic.AtomicLong)
                operator
                    .getOperatorContext()
                    .getSpecifiedInfo()
                    .get(ExchangeOperator.SIZE_IN_BYTES))
            .get());
  }

  private TsBlock buildTsBlock(int rowCount) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    for (int i = 0; i < rowCount; i++) {
      builder.getTimeColumnBuilder().writeLong(i);
      builder.getColumnBuilder(0).writeInt(i);
      builder.declarePosition();
    }
    return builder.build();
  }
}
