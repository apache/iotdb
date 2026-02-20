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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ValuesOperatorTest {
  private static final ExecutorService instanceNotificationExecutor =
      IoTDBThreadPoolFactory.newFixedThreadPool(
          1, "valuesOperator-test-instance-notification");

  @Test
  public void testEmptyValues() {
    try (ValuesOperator operator = genValuesOperator(ImmutableList.of())) {
      assertTrue(operator.isFinished());
      assertFalse(operator.hasNext());
      assertNull(operator.next());
      assertEquals(0, operator.calculateMaxPeekMemory());
      assertEquals(0, operator.calculateMaxReturnSize());
      assertEquals(0, operator.calculateRetainedSizeAfterCallingNext());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testSingleTsBlock() {
    int[] values = {10, 20, 30};
    TsBlock tsBlock = createIntTsBlock(values);

    try (ValuesOperator operator = genValuesOperator(Collections.singletonList(tsBlock))) {
      assertFalse(operator.isFinished());
      assertTrue(operator.hasNext());

      TsBlock result = operator.next();
      assertNotNull(result);
      assertEquals(3, result.getPositionCount());
      for (int i = 0; i < values.length; i++) {
        assertEquals(values[i], result.getColumn(0).getInt(i));
      }

      assertTrue(operator.isFinished());
      assertFalse(operator.hasNext());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testMultipleTsBlocks() {
    int[] values1 = {1, 2, 3};
    int[] values2 = {4, 5};
    int[] values3 = {6, 7, 8, 9};

    TsBlock block1 = createIntTsBlock(values1);
    TsBlock block2 = createIntTsBlock(values2);
    TsBlock block3 = createIntTsBlock(values3);

    try (ValuesOperator operator =
        genValuesOperator(Arrays.asList(block1, block2, block3))) {
      assertFalse(operator.isFinished());
      assertTrue(operator.hasNext());

      // First block
      TsBlock result1 = operator.next();
      assertNotNull(result1);
      assertEquals(3, result1.getPositionCount());
      for (int i = 0; i < values1.length; i++) {
        assertEquals(values1[i], result1.getColumn(0).getInt(i));
      }

      // Second block
      assertFalse(operator.isFinished());
      assertTrue(operator.hasNext());
      TsBlock result2 = operator.next();
      assertNotNull(result2);
      assertEquals(2, result2.getPositionCount());
      for (int i = 0; i < values2.length; i++) {
        assertEquals(values2[i], result2.getColumn(0).getInt(i));
      }

      // Third block
      assertFalse(operator.isFinished());
      assertTrue(operator.hasNext());
      TsBlock result3 = operator.next();
      assertNotNull(result3);
      assertEquals(4, result3.getPositionCount());
      for (int i = 0; i < values3.length; i++) {
        assertEquals(values3[i], result3.getColumn(0).getInt(i));
      }

      assertTrue(operator.isFinished());
      assertFalse(operator.hasNext());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testRetainedSizeDecreases() {
    int[] values1 = {1, 2, 3};
    int[] values2 = {4, 5, 6};

    TsBlock block1 = createIntTsBlock(values1);
    TsBlock block2 = createIntTsBlock(values2);

    try (ValuesOperator operator = genValuesOperator(Arrays.asList(block1, block2))) {
      long initialRetained = operator.calculateRetainedSizeAfterCallingNext();

      operator.next();
      long afterFirstRetained = operator.calculateRetainedSizeAfterCallingNext();
      assertTrue(
          "Retained size should decrease after consuming a block",
          afterFirstRetained < initialRetained);

      operator.next();
      long afterSecondRetained = operator.calculateRetainedSizeAfterCallingNext();
      assertEquals(0, afterSecondRetained);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testIsBlockedReturnsNotBlocked() {
    try (ValuesOperator operator = genValuesOperator(ImmutableList.of())) {
      assertTrue(operator.isBlocked().isDone());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private TsBlock createIntTsBlock(int[] values) {
    TsBlockBuilder builder =
        new TsBlockBuilder(values.length, Collections.singletonList(TSDataType.INT32));
    ColumnBuilder columnBuilder = builder.getColumnBuilder(0);
    for (int value : values) {
      columnBuilder.writeInt(value);
    }
    builder.declarePositions(values.length);
    return builder.build(
        new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, builder.getPositionCount()));
  }

  private ValuesOperator genValuesOperator(java.util.List<TsBlock> tsBlocks) {
    QueryId queryId = new QueryId("stub_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNode = new PlanNodeId("1");
    driverContext.addOperatorContext(
        1, planNode, TreeLinearFillOperator.class.getSimpleName());

    return new ValuesOperator(driverContext.getOperatorContexts().get(0), tsBlocks);
  }
}
