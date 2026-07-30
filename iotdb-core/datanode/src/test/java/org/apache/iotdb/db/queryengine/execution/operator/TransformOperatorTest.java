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

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.common.NodeRef;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.commons.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.process.TransformOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.dag.input.QueryDataSetInputLayer;
import org.apache.iotdb.db.queryengine.transformation.dag.input.TsBlockInputDataSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;

public class TransformOperatorTest {

  @Test
  public void testInputLayerEmptyBlockProcess() throws Exception {
    QueryId queryId = new QueryId("stub_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(
            instanceId, IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification"));
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNodeId1 = new PlanNodeId("1");
    driverContext.addOperatorContext(1, planNodeId1, SeriesScanOperator.class.getSimpleName());
    PlanNodeId planNodeId2 = new PlanNodeId("2");
    driverContext.addOperatorContext(2, planNodeId2, TransformOperator.class.getSimpleName());

    // Construct child Operator
    TsBlock[] data = new TsBlock[3];
    TimeColumn timeColumn1 = new TimeColumn(1, new long[] {1});
    data[0] = new TsBlock(timeColumn1, new LongColumn(1, Optional.empty(), new long[] {1}));
    TimeColumn timeColumn2 = new TimeColumn(0, new long[] {});
    data[1] = new TsBlock(timeColumn2, new LongColumn(0, Optional.empty(), new long[] {}));
    TimeColumn timeColumn3 = new TimeColumn(1, new long[] {2});
    data[2] = new TsBlock(timeColumn3, new LongColumn(1, Optional.empty(), new long[] {1}));
    Operator childOperator =
        new Operator() {
          boolean finished = false;
          int count = 0;

          @Override
          public OperatorContext getOperatorContext() {
            return driverContext.getOperatorContexts().get(0);
          }

          @Override
          public TsBlock next() {
            TsBlock tsBlock = data[count];
            count++;
            if (count == 3) {
              finished = true;
            }
            return tsBlock;
          }

          @Override
          public boolean hasNext() {
            return !finished;
          }

          @Override
          public void close() {}

          @Override
          public boolean isFinished() {
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

    // Construct LayerReader for TransformOperator
    QueryDataSetInputLayer inputLayer =
        new QueryDataSetInputLayer(
            queryId.toString(),
            1,
            new TsBlockInputDataSet(childOperator, ImmutableList.of(TSDataType.INT64)));
    LayerReader reader = inputLayer.constructValueReader(0);
    reader.yield();
    reader.consumedAll();
    // process empty TsBlock
    reader.yield();
    reader.consumedAll();
    reader.yield();
    reader.consumedAll();
  }

  @Test
  public void testTransformResultLimit() throws Exception {
    UDFClassLoaderManager.setupAndGetInstance();
    int savedMaxLine = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();
    try {
      int rowCount = 2001;
      int maxLine = 200;
      TSFileDescriptor.getInstance().getConfig().setMaxTsBlockLineNumber(200);
      QueryId queryId = new QueryId("stub_query_chunk");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(
              instanceId,
              IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification"));
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
      PlanNodeId scanNodeId = new PlanNodeId("scan");
      driverContext.addOperatorContext(1, scanNodeId, SeriesScanOperator.class.getSimpleName());
      PlanNodeId transformNodeId = new PlanNodeId("transform");
      driverContext.addOperatorContext(2, transformNodeId, TransformOperator.class.getSimpleName());

      long[] times = new long[rowCount];
      long[] values = new long[rowCount];
      for (int i = 0; i < rowCount; i++) {
        times[i] = i;
        values[i] = i * 10L;
      }
      TsBlock oneBatch =
          new TsBlock(
              new TimeColumn(rowCount, times), new LongColumn(rowCount, Optional.empty(), values));

      Operator childOperator =
          new Operator() {
            boolean consumed = false;

            @Override
            public OperatorContext getOperatorContext() {
              return driverContext.getOperatorContexts().get(0);
            }

            @Override
            public TsBlock next() {
              if (!consumed) {
                consumed = true;
                return oneBatch;
              }
              return null;
            }

            @Override
            public boolean hasNext() {
              return !consumed;
            }

            @Override
            public void close() {}

            @Override
            public boolean isFinished() {
              return consumed;
            }

            @Override
            public long calculateMaxPeekMemory() {
              return oneBatch.getSizeInBytes();
            }

            @Override
            public long calculateMaxReturnSize() {
              return oneBatch.getSizeInBytes();
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

      TimeSeriesOperand s1 =
          new TimeSeriesOperand(new PartialPath("root.sg.d1.s1"), TSDataType.INT64);
      Map<String, List<InputLocation>> inputLocations =
          ImmutableMap.of(s1.getExpressionString(), ImmutableList.of(new InputLocation(0, 0)));
      Map<NodeRef<Expression>, TSDataType> expressionTypes = new HashMap<>();
      expressionTypes.put(NodeRef.of(s1), TSDataType.INT64);

      TransformOperator transform =
          new TransformOperator(
              driverContext.getOperatorContexts().get(1),
              childOperator,
              ImmutableList.of(TSDataType.INT64),
              inputLocations,
              new Expression[] {s1},
              true,
              ZoneId.systemDefault(),
              expressionTypes,
              true);

      int totalOutRows = 0;
      int nonNullNextCount = 0;
      while (transform.hasNext()) {
        TsBlock out = transform.next();
        if (out != null) {
          nonNullNextCount++;
          Assert.assertTrue(
              "Each batch must be at most " + maxLine + " rows", out.getPositionCount() <= maxLine);
          totalOutRows += out.getPositionCount();
        }
      }
      Assert.assertEquals(rowCount, totalOutRows);
      System.out.println(nonNullNextCount);
      Assert.assertTrue(nonNullNextCount >= 11);
    } finally {
      TSFileDescriptor.getInstance().getConfig().setMaxTsBlockLineNumber(savedMaxLine);
    }
  }
}
