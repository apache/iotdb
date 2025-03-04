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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.HashAggregationOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.TimestampType;
import org.junit.Test;

import java.util.Collections;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.GroupByHash.DEFAULT_GROUP_NUMBER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GroupByLargeDataTest {

  @Test
  public void test() {
    try (HashAggregationOperator aggregationOperator = genHashAggregationOperator()) {
      ListenableFuture<?> listenableFuture = aggregationOperator.isBlocked();
      listenableFuture.get();
      while (!aggregationOperator.isFinished() && aggregationOperator.hasNext()) {
        TsBlock tsBlock = aggregationOperator.next();
        if (tsBlock != null && !tsBlock.isEmpty()) {
          assertEquals(2, tsBlock.getPositionCount());
          Column column = tsBlock.getColumn(0);
          if (column.getLong(0) == 1) {
            assertEquals(2, column.getLong(1));
          } else {
            assertEquals(2, column.getLong(0));
            assertEquals(1, column.getLong(1));
          }
        }
        listenableFuture = aggregationOperator.isBlocked();
        listenableFuture.get();
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // construct a AggregationHashOperator has more than 1024 lines in input TsBlock
  private HashAggregationOperator genHashAggregationOperator() {

    // Construct operator tree
    QueryId queryId = new QueryId("stub_query");

    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(
            instanceId,
            IoTDBThreadPoolFactory.newFixedThreadPool(
                1, "aggregationHashOperator-test-instance-notification"));
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNodeId1 = new PlanNodeId("1");
    driverContext.addOperatorContext(1, planNodeId1, TableScanOperator.class.getSimpleName());
    PlanNodeId planNodeId2 = new PlanNodeId("2");
    driverContext.addOperatorContext(2, planNodeId2, HashAggregationOperator.class.getSimpleName());
    Operator childOperator =
        new Operator() {
          boolean finished = false;

          @Override
          public OperatorContext getOperatorContext() {
            return driverContext.getOperatorContexts().get(0);
          }

          @Override
          public TsBlock next() {
            TsBlockBuilder builder =
                new TsBlockBuilder(Collections.singletonList(TSDataType.TIMESTAMP));
            ColumnBuilder columnBuilder = builder.getValueColumnBuilders()[0];
            for (int i = 0; i < 1000; i++) {
              columnBuilder.writeLong(1);
            }
            for (int i = 1000; i < 1025; i++) {
              columnBuilder.writeLong(2);
            }
            builder.declarePositions(1025);
            TsBlock result =
                builder.build(
                    new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, builder.getPositionCount()));
            finished = true;
            return result;
          }

          @Override
          public boolean hasNext() throws Exception {
            return !finished;
          }

          @Override
          public void close() throws Exception {}

          @Override
          public boolean isFinished() throws Exception {
            return finished;
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

    return new HashAggregationOperator(
        operatorContext,
        childOperator,
        Collections.singletonList(TimestampType.TIMESTAMP),
        Collections.singletonList(0),
        Collections.emptyList(),
        AggregationNode.Step.SINGLE,
        DEFAULT_GROUP_NUMBER,
        Long.MAX_VALUE,
        false,
        Long.MAX_VALUE);
  }
}
