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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.process.SingleDeviceViewOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.FullOuterTimeJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.planner.memory.ConsumeAllChildrenPipelineMemoryEstimator;
import org.apache.iotdb.db.queryengine.plan.planner.memory.ConsumeChildrenOneByOnePipelineMemoryEstimator;
import org.apache.iotdb.db.queryengine.plan.planner.memory.PipelineMemoryEstimator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.LeftOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByKey;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.plan.statement.component.OrderByKey.DEVICE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PipelineBuilderTest {

  OperatorTreeGenerator operatorTreeGenerator = new OperatorTreeGenerator();

  /**
   * The operator structure is [TimeJoin1 - [SeriesScan0,SeriesScan1,SeriesScan2,SeriesScan3]].
   *
   * <p>The next six tests, I will test this TimeJoinOperator with different dop.
   *
   * <p>The first test will test dop = 1. Expected result is that no child pipelines will be
   * divided.
   */
  @Test
  public void testConsumeAllChildrenPipelineBuilder1() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    FullOuterTimeJoinNode fullOuterTimeJoinNode = initFullOuterTimeJoinNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(1);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeAllChildrenPipelineBreaker(
            fullOuterTimeJoinNode, context);
    assertEquals(0, context.getPipelineNumber());
    assertEquals(4, childrenOperator.size());
    assertEquals(4, fullOuterTimeJoinNode.getChildren().size());
    for (int i = 0; i < 4; i++) {
      assertEquals(SeriesScanOperator.class, childrenOperator.get(i).getClass());
      assertEquals(SeriesScanNode.class, fullOuterTimeJoinNode.getChildren().get(i).getClass());
      assertEquals(
          String.format("root.sg.d%d.s1", i),
          fullOuterTimeJoinNode.getChildren().get(i).getOutputColumnNames().get(0));
    }

    // Validate the number exchange operator
    assertEquals(0, context.getExchangeSumNum());

    // Validate PipelineMemoryEstimator
    try (Operator root = fullOuterTimeJoinNode.accept(operatorTreeGenerator, context)) {
      PipelineMemoryEstimator pipelineMemoryEstimator =
          context.constructPipelineMemoryEstimator(root, null, fullOuterTimeJoinNode, -1);
      assertEquals(
          root.calculateMaxPeekMemoryWithCounter() + root.ramBytesUsed(),
          pipelineMemoryEstimator.getEstimatedMemoryUsageInBytes());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * This test will test dop = 2. Expected result is two pipelines:
   *
   * <p>The first is: TimeJoin1 - [SeriesScan1, SeriesScan0, ExchangeOperator];
   *
   * <p>The second is: ExchangeOperator - TimeJoin1-1[SeriesScan2, SeriesScan3].
   */
  @Test
  public void testConsumeAllChildrenPipelineBuilder2() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    FullOuterTimeJoinNode fullOuterTimeJoinNode = initFullOuterTimeJoinNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(2);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeAllChildrenPipelineBreaker(
            fullOuterTimeJoinNode, context);
    // The number of pipeline is 1, since parent pipeline hasn't joined
    assertEquals(1, context.getPipelineNumber());

    // Validate the first pipeline
    assertEquals(3, childrenOperator.size());
    assertEquals(3, fullOuterTimeJoinNode.getChildren().size());
    for (int i = 0; i < 2; i++) {
      assertEquals(SeriesScanOperator.class, childrenOperator.get(i).getClass());
      assertEquals(SeriesScanNode.class, fullOuterTimeJoinNode.getChildren().get(i).getClass());
    }
    assertEquals(ExchangeOperator.class, childrenOperator.get(2).getClass());

    // Validate the changes of node structure
    assertEquals(
        "root.sg.d0.s1", fullOuterTimeJoinNode.getChildren().get(0).getOutputColumnNames().get(0));
    assertEquals(
        "root.sg.d1.s1", fullOuterTimeJoinNode.getChildren().get(1).getOutputColumnNames().get(0));
    assertEquals(
        FullOuterTimeJoinNode.class, fullOuterTimeJoinNode.getChildren().get(2).getClass());

    // Validate the second pipeline
    FullOuterTimeJoinNode subFullOuterTimeJoinNode =
        (FullOuterTimeJoinNode) fullOuterTimeJoinNode.getChildren().get(2);
    assertEquals(2, subFullOuterTimeJoinNode.getChildren().size());
    assertEquals(
        "root.sg.d2.s1",
        subFullOuterTimeJoinNode.getChildren().get(0).getOutputColumnNames().get(0));
    assertEquals(
        "root.sg.d3.s1",
        subFullOuterTimeJoinNode.getChildren().get(1).getOutputColumnNames().get(0));

    // Validate the number exchange operator
    assertEquals(1, context.getExchangeSumNum());

    // Validate PipelineMemoryEstimator
    try (Operator root =
        fullOuterTimeJoinNode.accept(
            operatorTreeGenerator, createLocalExecutionPlanContext(typeProvider))) {
      PipelineMemoryEstimator rootPipelineMemoryEstimator =
          context.constructPipelineMemoryEstimator(root, null, fullOuterTimeJoinNode, -1);
      assertEquals(
          ConsumeAllChildrenPipelineMemoryEstimator.class, rootPipelineMemoryEstimator.getClass());
      // test calculateEstimatedMemory
      assertEquals(
          root.calculateMaxPeekMemoryWithCounter()
              + rootPipelineMemoryEstimator.getChildren().stream()
                  .map(PipelineMemoryEstimator::calculateEstimatedRunningMemorySize)
                  .reduce(0L, Long::sum),
          rootPipelineMemoryEstimator.calculateEstimatedRunningMemorySize());

      List<PipelineMemoryEstimator> childrenPipelineMemoryEstimators =
          rootPipelineMemoryEstimator.getChildren();
      assertEquals(1, childrenPipelineMemoryEstimators.size());
      for (int i = 0; i < 1; i++) {
        assertEquals(
            ConsumeAllChildrenPipelineMemoryEstimator.class,
            childrenPipelineMemoryEstimators.get(i).getClass());
        assertEquals(
            FullOuterTimeJoinOperator.class,
            childrenPipelineMemoryEstimators.get(i).getRoot().getClass());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * This test will test dop = 3. Expected result is three pipelines:
   *
   * <p>The first is: TimeJoin1 - [SeriesScan0, ExchangeOperator, ExchangeOperator];
   *
   * <p>The second is: ExchangeOperator - SeriesScan1.
   *
   * <p>The third is: ExchangeOperator - TimeJoin1-1[SeriesScan2, SeriesScan3].
   */
  @Test
  public void testConsumeAllChildrenPipelineBuilder3() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    FullOuterTimeJoinNode fullOuterTimeJoinNode = initFullOuterTimeJoinNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(3);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeAllChildrenPipelineBreaker(
            fullOuterTimeJoinNode, context);
    // The number of pipeline is 2, since parent pipeline hasn't joined
    assertEquals(2, context.getPipelineNumber());

    // Validate the first pipeline
    assertEquals(3, childrenOperator.size());
    assertEquals(SeriesScanOperator.class, childrenOperator.get(0).getClass());
    assertEquals(ExchangeOperator.class, childrenOperator.get(1).getClass());
    assertEquals(ExchangeOperator.class, childrenOperator.get(2).getClass());

    // Validate the changes of node structure
    assertEquals(3, fullOuterTimeJoinNode.getChildren().size());
    assertEquals(
        "root.sg.d0.s1", fullOuterTimeJoinNode.getChildren().get(0).getOutputColumnNames().get(0));
    assertEquals(
        "root.sg.d1.s1", fullOuterTimeJoinNode.getChildren().get(1).getOutputColumnNames().get(0));
    assertEquals(
        FullOuterTimeJoinNode.class, fullOuterTimeJoinNode.getChildren().get(2).getClass());

    // Validate the second pipeline
    ExchangeOperator exchangeOperator1 = (ExchangeOperator) childrenOperator.get(1);
    assertEquals("SeriesScanNode1", exchangeOperator1.getSourceId().getId());

    // Validate the third pipeline
    FullOuterTimeJoinNode subFullOuterTimeJoinNode =
        (FullOuterTimeJoinNode) fullOuterTimeJoinNode.getChildren().get(2);
    assertEquals(2, subFullOuterTimeJoinNode.getChildren().size());
    assertEquals(
        "root.sg.d2.s1",
        subFullOuterTimeJoinNode.getChildren().get(0).getOutputColumnNames().get(0));
    assertEquals(
        "root.sg.d3.s1",
        subFullOuterTimeJoinNode.getChildren().get(1).getOutputColumnNames().get(0));
    ExchangeOperator exchangeOperator2 = (ExchangeOperator) childrenOperator.get(2);
    assertEquals(exchangeOperator2.getSourceId(), subFullOuterTimeJoinNode.getPlanNodeId());

    // Validate the number exchange operator
    assertEquals(2, context.getExchangeSumNum());

    // Validate PipelineMemoryEstimator
    try (Operator root =
        fullOuterTimeJoinNode.accept(
            operatorTreeGenerator, createLocalExecutionPlanContext(typeProvider))) {
      PipelineMemoryEstimator rootPipelineMemoryEstimator =
          context.constructPipelineMemoryEstimator(root, null, fullOuterTimeJoinNode, -1);
      assertEquals(
          ConsumeAllChildrenPipelineMemoryEstimator.class, rootPipelineMemoryEstimator.getClass());
      List<PipelineMemoryEstimator> childrenPipelineMemoryEstimators =
          rootPipelineMemoryEstimator.getChildren();
      assertEquals(2, childrenPipelineMemoryEstimators.size());
      for (int i = 0; i < 2; i++) {
        assertEquals(
            ConsumeAllChildrenPipelineMemoryEstimator.class,
            childrenPipelineMemoryEstimators.get(i).getClass());
        if (i == 0) {
          assertEquals(
              SeriesScanOperator.class,
              childrenPipelineMemoryEstimators.get(i).getRoot().getClass());
        } else {
          assertEquals(
              FullOuterTimeJoinOperator.class,
              childrenPipelineMemoryEstimators.get(i).getRoot().getClass());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * This test will test dop = 4. Expected result is four pipelines:
   *
   * <p>The first is: TimeJoin1 - [SeriesScan0, ExchangeOperator, ExchangeOperator,
   * ExchangeOperator];
   *
   * <p>The second is: ExchangeOperator - SeriesScan1.
   *
   * <p>The third is: ExchangeOperator - SeriesScan2.
   *
   * <p>The forth is: ExchangeOperator - SeriesScan3.
   */
  @Test
  public void testConsumeAllChildrenPipelineBuilder4() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    FullOuterTimeJoinNode fullOuterTimeJoinNode = initFullOuterTimeJoinNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(4);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeAllChildrenPipelineBreaker(
            fullOuterTimeJoinNode, context);
    // The number of pipeline is 3, since parent pipeline hasn't joined
    assertEquals(3, context.getPipelineNumber());

    // Validate the first pipeline
    assertEquals(4, childrenOperator.size());
    assertEquals(SeriesScanOperator.class, childrenOperator.get(0).getClass());
    assertEquals(ExchangeOperator.class, childrenOperator.get(1).getClass());
    assertEquals(ExchangeOperator.class, childrenOperator.get(2).getClass());
    assertEquals(ExchangeOperator.class, childrenOperator.get(3).getClass());

    // Validate the changes of node structure
    assertEquals(4, fullOuterTimeJoinNode.getChildren().size());
    for (int i = 0; i < 4; i++) {
      assertEquals(SeriesScanNode.class, fullOuterTimeJoinNode.getChildren().get(i).getClass());
      assertEquals(
          String.format("root.sg.d%d.s1", i),
          fullOuterTimeJoinNode.getChildren().get(i).getOutputColumnNames().get(0));
    }

    // Validate the second pipeline
    ExchangeOperator exchangeOperator1 = (ExchangeOperator) childrenOperator.get(1);
    assertEquals("SeriesScanNode1", exchangeOperator1.getSourceId().getId());

    // Validate the third pipeline
    ExchangeOperator exchangeOperator2 = (ExchangeOperator) childrenOperator.get(2);
    assertEquals("SeriesScanNode2", exchangeOperator2.getSourceId().getId());

    // Validate the forth pipeline
    ExchangeOperator exchangeOperator3 = (ExchangeOperator) childrenOperator.get(3);
    assertEquals("SeriesScanNode3", exchangeOperator3.getSourceId().getId());

    // Validate the number exchange operator
    assertEquals(3, context.getExchangeSumNum());

    // Validate PipelineMemoryEstimator
    try (Operator root =
        fullOuterTimeJoinNode.accept(
            operatorTreeGenerator, createLocalExecutionPlanContext(typeProvider))) {
      PipelineMemoryEstimator rootPipelineMemoryEstimator =
          context.constructPipelineMemoryEstimator(root, null, fullOuterTimeJoinNode, -1);
      assertEquals(
          ConsumeAllChildrenPipelineMemoryEstimator.class, rootPipelineMemoryEstimator.getClass());
      List<PipelineMemoryEstimator> childrenPipelineMemoryEstimators =
          rootPipelineMemoryEstimator.getChildren();
      assertEquals(3, childrenPipelineMemoryEstimators.size());
      for (int i = 0; i < 3; i++) {
        assertEquals(
            ConsumeAllChildrenPipelineMemoryEstimator.class,
            childrenPipelineMemoryEstimators.get(i).getClass());
        assertEquals(
            SeriesScanOperator.class, childrenPipelineMemoryEstimators.get(i).getRoot().getClass());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * This test will test dop = 5. Expected result is five pipelines:
   *
   * <p>The first is: TimeJoin1 - [ExchangeOperator, ExchangeOperator, ExchangeOperator,
   * ExchangeOperator];
   *
   * <p>The second is: ExchangeOperator - SeriesScan0.
   *
   * <p>The third is: ExchangeOperator - SeriesScan1.
   *
   * <p>The forth is: ExchangeOperator - SeriesScan2.
   *
   * <p>The fifth is: ExchangeOperator - SeriesScan3.
   */
  @Test
  public void testConsumeAllChildrenPipelineBuilder5() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    FullOuterTimeJoinNode fullOuterTimeJoinNode = initFullOuterTimeJoinNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(5);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeAllChildrenPipelineBreaker(
            fullOuterTimeJoinNode, context);
    // The number of pipeline is 4, since parent pipeline hasn't joined
    assertEquals(4, context.getPipelineNumber());

    // Validate the first pipeline
    assertEquals(4, childrenOperator.size());
    for (int i = 0; i < 4; i++) {
      assertEquals(ExchangeOperator.class, childrenOperator.get(i).getClass());
    }

    // Validate the changes of node structure
    assertEquals(4, fullOuterTimeJoinNode.getChildren().size());
    for (int i = 0; i < 4; i++) {
      assertEquals(SeriesScanNode.class, fullOuterTimeJoinNode.getChildren().get(i).getClass());
      assertEquals(
          String.format("root.sg.d%d.s1", i),
          fullOuterTimeJoinNode.getChildren().get(i).getOutputColumnNames().get(0));
    }

    // Validate the second pipeline
    ExchangeOperator exchangeOperator1 = (ExchangeOperator) childrenOperator.get(0);
    assertEquals("SeriesScanNode0", exchangeOperator1.getSourceId().getId());

    // Validate the third pipeline
    ExchangeOperator exchangeOperator2 = (ExchangeOperator) childrenOperator.get(1);
    assertEquals("SeriesScanNode1", exchangeOperator2.getSourceId().getId());

    // Validate the forth pipeline
    ExchangeOperator exchangeOperator3 = (ExchangeOperator) childrenOperator.get(2);
    assertEquals("SeriesScanNode2", exchangeOperator3.getSourceId().getId());

    // Validate the fifth pipeline
    ExchangeOperator exchangeOperator4 = (ExchangeOperator) childrenOperator.get(3);
    assertEquals("SeriesScanNode3", exchangeOperator4.getSourceId().getId());

    // Validate the number exchange operator
    assertEquals(4, context.getExchangeSumNum());

    // Validate PipelineMemoryEstimator
    try (Operator root =
        fullOuterTimeJoinNode.accept(
            operatorTreeGenerator, createLocalExecutionPlanContext(typeProvider))) {
      PipelineMemoryEstimator rootPipelineMemoryEstimator =
          context.constructPipelineMemoryEstimator(root, null, fullOuterTimeJoinNode, -1);
      assertEquals(
          ConsumeAllChildrenPipelineMemoryEstimator.class, rootPipelineMemoryEstimator.getClass());
      List<PipelineMemoryEstimator> childrenPipelineMemoryEstimators =
          rootPipelineMemoryEstimator.getChildren();
      assertEquals(4, childrenPipelineMemoryEstimators.size());
      for (int i = 0; i < 4; i++) {
        assertEquals(
            ConsumeAllChildrenPipelineMemoryEstimator.class,
            childrenPipelineMemoryEstimators.get(i).getClass());
        assertEquals(
            SeriesScanOperator.class, childrenPipelineMemoryEstimators.get(i).getRoot().getClass());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * This test will test dop = 6. Expected result is still five pipelines:
   *
   * <p>The first is: TimeJoin1 - [ExchangeOperator, ExchangeOperator, ExchangeOperator,
   * ExchangeOperator];
   *
   * <p>The second is: ExchangeOperator - SeriesScan0.
   *
   * <p>The third is: ExchangeOperator - SeriesScan1.
   *
   * <p>The forth is: ExchangeOperator - SeriesScan2.
   *
   * <p>The fifth is: ExchangeOperator - SeriesScan3.
   */
  @Test
  public void testConsumeAllChildrenPipelineBuilder6() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    FullOuterTimeJoinNode fullOuterTimeJoinNode = initFullOuterTimeJoinNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(6);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeAllChildrenPipelineBreaker(
            fullOuterTimeJoinNode, context);
    // The number of pipeline is 4, since parent pipeline hasn't joined
    assertEquals(4, context.getPipelineNumber());

    // Validate the first pipeline
    assertEquals(4, childrenOperator.size());
    for (int i = 0; i < 4; i++) {
      assertEquals(ExchangeOperator.class, childrenOperator.get(i).getClass());
    }

    // Validate the changes of node structure
    assertEquals(4, fullOuterTimeJoinNode.getChildren().size());
    for (int i = 0; i < 4; i++) {
      assertEquals(SeriesScanNode.class, fullOuterTimeJoinNode.getChildren().get(i).getClass());
      assertEquals(
          String.format("root.sg.d%d.s1", i),
          fullOuterTimeJoinNode.getChildren().get(i).getOutputColumnNames().get(0));
    }

    // Validate the second pipeline
    ExchangeOperator exchangeOperator1 = (ExchangeOperator) childrenOperator.get(0);
    assertEquals("SeriesScanNode0", exchangeOperator1.getSourceId().getId());

    // Validate the third pipeline
    ExchangeOperator exchangeOperator2 = (ExchangeOperator) childrenOperator.get(1);
    assertEquals("SeriesScanNode1", exchangeOperator2.getSourceId().getId());

    // Validate the forth pipeline
    ExchangeOperator exchangeOperator3 = (ExchangeOperator) childrenOperator.get(2);
    assertEquals("SeriesScanNode2", exchangeOperator3.getSourceId().getId());

    // Validate the fifth pipeline
    ExchangeOperator exchangeOperator4 = (ExchangeOperator) childrenOperator.get(3);
    assertEquals("SeriesScanNode3", exchangeOperator4.getSourceId().getId());

    // Validate the number exchange operator
    assertEquals(4, context.getExchangeSumNum());

    // Validate PipelineMemoryEstimator
    try (Operator root =
        fullOuterTimeJoinNode.accept(
            operatorTreeGenerator, createLocalExecutionPlanContext(typeProvider))) {
      PipelineMemoryEstimator rootPipelineMemoryEstimator =
          context.constructPipelineMemoryEstimator(root, null, fullOuterTimeJoinNode, -1);
      assertEquals(
          ConsumeAllChildrenPipelineMemoryEstimator.class, rootPipelineMemoryEstimator.getClass());
      List<PipelineMemoryEstimator> childrenPipelineMemoryEstimators =
          rootPipelineMemoryEstimator.getChildren();
      assertEquals(4, childrenPipelineMemoryEstimators.size());
      for (int i = 0; i < 4; i++) {
        assertEquals(
            ConsumeAllChildrenPipelineMemoryEstimator.class,
            childrenPipelineMemoryEstimators.get(i).getClass());
        assertEquals(
            SeriesScanOperator.class, childrenPipelineMemoryEstimators.get(i).getRoot().getClass());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * This test will test dop = 3. Expected result is three pipelines:
   *
   * <p>The first is: TopKNode1 - [SingleDeviceViewNode0, ExchangeOperator, ExchangeOperator];
   *
   * <p>The second is: ExchangeOperator - SingleDeviceViewNode1.
   *
   * <p>The third is: ExchangeOperator - TopKNode1-1[SingleDeviceViewNode2, SingleDeviceViewNode3].
   */
  @Test
  public void testTopKConsumeAllChildrenPipelineBuilder3() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    TopKNode topKNode = initTopKNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(3);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeAllChildrenPipelineBreaker(topKNode, context);
    // The number of pipeline is 2, since parent pipeline hasn't joined
    assertEquals(2, context.getPipelineNumber());

    // Validate the first pipeline
    assertEquals(3, childrenOperator.size());
    assertEquals(SingleDeviceViewOperator.class, childrenOperator.get(0).getClass());
    assertEquals(ExchangeOperator.class, childrenOperator.get(1).getClass());
    assertEquals(ExchangeOperator.class, childrenOperator.get(2).getClass());

    // Validate the changes of node structure
    assertEquals(3, topKNode.getChildren().size());
    assertEquals("Time", topKNode.getChildren().get(0).getOutputColumnNames().get(0));
    assertEquals("Time", topKNode.getChildren().get(1).getOutputColumnNames().get(0));
    assertEquals("Time", topKNode.getChildren().get(2).getOutputColumnNames().get(0));
    assertEquals(TopKNode.class, topKNode.getChildren().get(2).getClass());

    // Validate the second pipeline
    ExchangeOperator exchangeOperator1 = (ExchangeOperator) childrenOperator.get(1);
    assertEquals("SingleDeviceViewNode1", exchangeOperator1.getSourceId().getId());

    // Validate the third pipeline
    TopKNode subTimeJoinNode = (TopKNode) topKNode.getChildren().get(2);
    assertEquals(2, subTimeJoinNode.getChildren().size());
    assertEquals("Time", subTimeJoinNode.getChildren().get(0).getOutputColumnNames().get(0));
    assertEquals("Time", subTimeJoinNode.getChildren().get(1).getOutputColumnNames().get(0));
    ExchangeOperator exchangeOperator2 = (ExchangeOperator) childrenOperator.get(2);
    assertEquals(exchangeOperator2.getSourceId(), subTimeJoinNode.getPlanNodeId());

    // Validate the number exchange operator
    assertEquals(2, context.getExchangeSumNum());
  }

  /**
   * The operator structure is [DeviceView - [SeriesScan0,SeriesScan1,SeriesScan2,SeriesScan3]].
   *
   * <p>The next six tests, I will test this DeviceViewOperator with different dop.
   *
   * <p>The first test will test dop = 1. Expected result is that no child pipelines will be
   * divided.
   */
  @Test
  public void testConsumeOneByOneChildrenPipelineBuilder1() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    DeviceViewNode deviceViewNode = initDeviceViewNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(1);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeChildrenOneByOneNode(deviceViewNode, context);
    assertEquals(0, context.getPipelineNumber());
    assertEquals(4, childrenOperator.size());
    for (int i = 0; i < 4; i++) {
      assertEquals(AlignedSeriesScanOperator.class, childrenOperator.get(i).getClass());
      assertEquals(
          String.format("root.sg.d%d.s1", i),
          deviceViewNode.getChildren().get(i).getOutputColumnNames().get(0));
    }

    // Validate the number exchange operator
    assertEquals(0, context.getExchangeSumNum());

    // Validate PipelineMemoryEstimator
    try {
      PipelineMemoryEstimator rootPipelineMemoryEstimator =
          context.constructPipelineMemoryEstimator(null, null, deviceViewNode, -1);
      assertEquals(
          ConsumeChildrenOneByOnePipelineMemoryEstimator.class,
          rootPipelineMemoryEstimator.getClass());
      List<PipelineMemoryEstimator> childrenPipelineMemoryEstimators =
          rootPipelineMemoryEstimator.getChildren();
      assertEquals(0, childrenPipelineMemoryEstimators.size());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * This test will test dop = 2. Expected result is five pipelines with dependency:
   *
   * <p>The first is: DeviceView - [ExchangeOperator, ExchangeOperator, ExchangeOperator,
   * ExchangeOperator];
   *
   * <p>The second is: ExchangeOperator - SeriesScan0.
   *
   * <p>The third is: ExchangeOperator - SeriesScan1, which has dependency second pipeline.
   *
   * <p>The forth is: ExchangeOperator - SeriesScan2, which has dependency third pipeline.
   *
   * <p>The fifth is: ExchangeOperator - SeriesScan3, which has dependency forth pipeline.
   */
  @Test
  public void testConsumeOneByOneChildrenPipelineBuilder2() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    DeviceViewNode deviceViewNode = initDeviceViewNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(2);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeChildrenOneByOneNode(deviceViewNode, context);
    // The number of pipeline is 4, since parent pipeline hasn't joined
    assertEquals(4, context.getPipelineNumber());

    // Validate the first pipeline
    assertEquals(4, childrenOperator.size());
    for (int i = 0; i < 4; i++) {
      assertEquals(ExchangeOperator.class, childrenOperator.get(i).getClass());
    }

    // Validate the second pipeline
    ExchangeOperator exchangeOperator1 = (ExchangeOperator) childrenOperator.get(0);
    assertEquals("AlignedSeriesScanNode0", exchangeOperator1.getSourceId().getId());
    assertEquals(-1, context.getPipelineDriverFactories().get(0).getDependencyPipelineIndex());

    // Validate the third pipeline
    ExchangeOperator exchangeOperator2 = (ExchangeOperator) childrenOperator.get(1);
    assertEquals("AlignedSeriesScanNode1", exchangeOperator2.getSourceId().getId());
    assertEquals(0, context.getPipelineDriverFactories().get(1).getDependencyPipelineIndex());

    // Validate the forth pipeline
    ExchangeOperator exchangeOperator3 = (ExchangeOperator) childrenOperator.get(2);
    assertEquals("AlignedSeriesScanNode2", exchangeOperator3.getSourceId().getId());
    assertEquals(1, context.getPipelineDriverFactories().get(2).getDependencyPipelineIndex());

    // Validate the fifth pipeline
    ExchangeOperator exchangeOperator4 = (ExchangeOperator) childrenOperator.get(3);
    assertEquals("AlignedSeriesScanNode3", exchangeOperator4.getSourceId().getId());
    assertEquals(2, context.getPipelineDriverFactories().get(3).getDependencyPipelineIndex());

    // Validate the number exchange operator
    assertEquals(1, context.getExchangeSumNum());

    // Validate PipelineMemoryEstimator
    try {
      PipelineMemoryEstimator rootPipelineMemoryEstimator =
          context.constructPipelineMemoryEstimator(null, null, deviceViewNode, -1);
      assertEquals(
          ConsumeChildrenOneByOnePipelineMemoryEstimator.class,
          rootPipelineMemoryEstimator.getClass());
      List<PipelineMemoryEstimator> childrenPipelineMemoryEstimators =
          rootPipelineMemoryEstimator.getChildren();
      assertEquals(4, childrenPipelineMemoryEstimators.size());
      for (int i = 0; i < 4; i++) {
        assertEquals(
            ConsumeAllChildrenPipelineMemoryEstimator.class,
            childrenPipelineMemoryEstimators.get(i).getClass());
        assertEquals(
            AlignedSeriesScanOperator.class,
            childrenPipelineMemoryEstimators.get(i).getRoot().getClass());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * This test will test dop = 3. Expected result is five pipelines with dependency:
   *
   * <p>The first is: DeviceView - [ExchangeOperator, ExchangeOperator, ExchangeOperator,
   * ExchangeOperator];
   *
   * <p>The second is: ExchangeOperator - SeriesScan0.
   *
   * <p>The third is: ExchangeOperator - SeriesScan1.
   *
   * <p>The forth is: ExchangeOperator - SeriesScan2, which has dependency second pipeline.
   *
   * <p>The fifth is: ExchangeOperator - SeriesScan3, which has dependency third pipeline.
   */
  @Test
  public void testConsumeOneByOneChildrenPipelineBuilder3() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    DeviceViewNode deviceViewNode = initDeviceViewNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(3);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeChildrenOneByOneNode(deviceViewNode, context);
    // The number of pipeline is 4, since parent pipeline hasn't joined
    assertEquals(4, context.getPipelineNumber());

    // Validate the first pipeline
    assertEquals(4, childrenOperator.size());
    for (int i = 0; i < 4; i++) {
      assertEquals(ExchangeOperator.class, childrenOperator.get(i).getClass());
    }

    // Validate the second pipeline
    ExchangeOperator exchangeOperator1 = (ExchangeOperator) childrenOperator.get(0);
    assertEquals("AlignedSeriesScanNode0", exchangeOperator1.getSourceId().getId());
    assertEquals(-1, context.getPipelineDriverFactories().get(0).getDependencyPipelineIndex());

    // Validate the third pipeline
    ExchangeOperator exchangeOperator2 = (ExchangeOperator) childrenOperator.get(1);
    assertEquals("AlignedSeriesScanNode1", exchangeOperator2.getSourceId().getId());
    assertEquals(-1, context.getPipelineDriverFactories().get(1).getDependencyPipelineIndex());

    // Validate the forth pipeline
    ExchangeOperator exchangeOperator3 = (ExchangeOperator) childrenOperator.get(2);
    assertEquals("AlignedSeriesScanNode2", exchangeOperator3.getSourceId().getId());
    assertEquals(0, context.getPipelineDriverFactories().get(2).getDependencyPipelineIndex());

    // Validate the fifth pipeline
    ExchangeOperator exchangeOperator4 = (ExchangeOperator) childrenOperator.get(3);
    assertEquals("AlignedSeriesScanNode3", exchangeOperator4.getSourceId().getId());
    assertEquals(1, context.getPipelineDriverFactories().get(3).getDependencyPipelineIndex());

    // Validate the number exchange operator
    assertEquals(2, context.getExchangeSumNum());

    // Validate PipelineMemoryEstimator
    try {
      PipelineMemoryEstimator rootPipelineMemoryEstimator =
          context.constructPipelineMemoryEstimator(null, null, deviceViewNode, -1);
      assertEquals(
          ConsumeChildrenOneByOnePipelineMemoryEstimator.class,
          rootPipelineMemoryEstimator.getClass());
      List<PipelineMemoryEstimator> childrenPipelineMemoryEstimators =
          rootPipelineMemoryEstimator.getChildren();
      assertEquals(4, childrenPipelineMemoryEstimators.size());
      for (int i = 0; i < 4; i++) {
        assertEquals(
            ConsumeAllChildrenPipelineMemoryEstimator.class,
            childrenPipelineMemoryEstimators.get(i).getClass());
        assertEquals(
            AlignedSeriesScanOperator.class,
            childrenPipelineMemoryEstimators.get(i).getRoot().getClass());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * This test will test dop = 4. Expected result is five pipelines with dependency:
   *
   * <p>The first is: DeviceView - [ExchangeOperator, ExchangeOperator, ExchangeOperator,
   * ExchangeOperator];
   *
   * <p>The second is: ExchangeOperator - SeriesScan0.
   *
   * <p>The third is: ExchangeOperator - SeriesScan1.
   *
   * <p>The forth is: ExchangeOperator - SeriesScan2.
   *
   * <p>The fifth is: ExchangeOperator - SeriesScan3, which has dependency second pipeline.
   */
  @Test
  public void testConsumeOneByOneChildrenPipelineBuilder4() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    DeviceViewNode deviceViewNode = initDeviceViewNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(4);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeChildrenOneByOneNode(deviceViewNode, context);
    // The number of pipeline is 4, since parent pipeline hasn't joined
    assertEquals(4, context.getPipelineNumber());

    // Validate the first pipeline
    assertEquals(4, childrenOperator.size());
    for (int i = 0; i < 4; i++) {
      assertEquals(ExchangeOperator.class, childrenOperator.get(i).getClass());
    }

    // Validate the second pipeline
    ExchangeOperator exchangeOperator1 = (ExchangeOperator) childrenOperator.get(0);
    assertEquals("AlignedSeriesScanNode0", exchangeOperator1.getSourceId().getId());
    assertEquals(-1, context.getPipelineDriverFactories().get(0).getDependencyPipelineIndex());

    // Validate the third pipeline
    ExchangeOperator exchangeOperator2 = (ExchangeOperator) childrenOperator.get(1);
    assertEquals("AlignedSeriesScanNode1", exchangeOperator2.getSourceId().getId());
    assertEquals(-1, context.getPipelineDriverFactories().get(1).getDependencyPipelineIndex());

    // Validate the forth pipeline
    ExchangeOperator exchangeOperator3 = (ExchangeOperator) childrenOperator.get(2);
    assertEquals("AlignedSeriesScanNode2", exchangeOperator3.getSourceId().getId());
    assertEquals(-1, context.getPipelineDriverFactories().get(2).getDependencyPipelineIndex());

    // Validate the fifth pipeline
    ExchangeOperator exchangeOperator4 = (ExchangeOperator) childrenOperator.get(3);
    assertEquals("AlignedSeriesScanNode3", exchangeOperator4.getSourceId().getId());
    assertEquals(0, context.getPipelineDriverFactories().get(3).getDependencyPipelineIndex());

    // Validate the number exchange operator
    assertEquals(3, context.getExchangeSumNum());

    // Validate PipelineMemoryEstimator
    try {
      PipelineMemoryEstimator rootPipelineMemoryEstimator =
          context.constructPipelineMemoryEstimator(null, null, deviceViewNode, -1);
      assertEquals(
          ConsumeChildrenOneByOnePipelineMemoryEstimator.class,
          rootPipelineMemoryEstimator.getClass());

      List<PipelineMemoryEstimator> childrenPipelineMemoryEstimators =
          rootPipelineMemoryEstimator.getChildren();
      assertEquals(4, childrenPipelineMemoryEstimators.size());
      for (int i = 0; i < 4; i++) {
        assertEquals(
            ConsumeAllChildrenPipelineMemoryEstimator.class,
            childrenPipelineMemoryEstimators.get(i).getClass());
        assertEquals(
            AlignedSeriesScanOperator.class,
            childrenPipelineMemoryEstimators.get(i).getRoot().getClass());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * This test will test dop = 5. Expected result is five pipelines without dependency:
   *
   * <p>The first is: DeviceView - [ExchangeOperator, ExchangeOperator, ExchangeOperator,
   * ExchangeOperator];
   *
   * <p>The second is: ExchangeOperator - SeriesScan0.
   *
   * <p>The third is: ExchangeOperator - SeriesScan1.
   *
   * <p>The forth is: ExchangeOperator - SeriesScan2.
   *
   * <p>The fifth is: ExchangeOperator - SeriesScan3.
   */
  @Test
  public void testConsumeOneByOneChildrenPipelineBuilder5() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    DeviceViewNode deviceViewNode = initDeviceViewNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(5);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeChildrenOneByOneNode(deviceViewNode, context);
    // The number of pipeline is 4, since parent pipeline hasn't joined
    assertEquals(4, context.getPipelineNumber());

    // Validate the first pipeline
    assertEquals(4, childrenOperator.size());
    for (int i = 0; i < 4; i++) {
      assertEquals(ExchangeOperator.class, childrenOperator.get(i).getClass());
    }

    // Validate the second pipeline
    ExchangeOperator exchangeOperator1 = (ExchangeOperator) childrenOperator.get(0);
    assertEquals("AlignedSeriesScanNode0", exchangeOperator1.getSourceId().getId());
    assertEquals(-1, context.getPipelineDriverFactories().get(0).getDependencyPipelineIndex());

    // Validate the third pipeline
    ExchangeOperator exchangeOperator2 = (ExchangeOperator) childrenOperator.get(1);
    assertEquals("AlignedSeriesScanNode1", exchangeOperator2.getSourceId().getId());
    assertEquals(-1, context.getPipelineDriverFactories().get(1).getDependencyPipelineIndex());

    // Validate the forth pipeline
    ExchangeOperator exchangeOperator3 = (ExchangeOperator) childrenOperator.get(2);
    assertEquals("AlignedSeriesScanNode2", exchangeOperator3.getSourceId().getId());
    assertEquals(-1, context.getPipelineDriverFactories().get(2).getDependencyPipelineIndex());

    // Validate the fifth pipeline
    ExchangeOperator exchangeOperator4 = (ExchangeOperator) childrenOperator.get(3);
    assertEquals("AlignedSeriesScanNode3", exchangeOperator4.getSourceId().getId());
    assertEquals(-1, context.getPipelineDriverFactories().get(3).getDependencyPipelineIndex());

    // Validate the number exchange operator
    assertEquals(4, context.getExchangeSumNum());

    // Validate PipelineMemoryEstimator
    try {
      PipelineMemoryEstimator rootPipelineMemoryEstimator =
          context.constructPipelineMemoryEstimator(null, null, deviceViewNode, -1);
      assertEquals(
          ConsumeChildrenOneByOnePipelineMemoryEstimator.class,
          rootPipelineMemoryEstimator.getClass());

      List<PipelineMemoryEstimator> childrenPipelineMemoryEstimators =
          rootPipelineMemoryEstimator.getChildren();
      assertEquals(4, childrenPipelineMemoryEstimators.size());
      for (int i = 0; i < 4; i++) {
        assertEquals(
            ConsumeAllChildrenPipelineMemoryEstimator.class,
            childrenPipelineMemoryEstimators.get(i).getClass());
        assertEquals(
            AlignedSeriesScanOperator.class,
            childrenPipelineMemoryEstimators.get(i).getRoot().getClass());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * This test will test dop = 6. Expected result is five pipelines without dependency:
   *
   * <p>The first is: DeviceView - [ExchangeOperator, ExchangeOperator, ExchangeOperator,
   * ExchangeOperator];
   *
   * <p>The second is: ExchangeOperator - SeriesScan0.
   *
   * <p>The third is: ExchangeOperator - SeriesScan1.
   *
   * <p>The forth is: ExchangeOperator - SeriesScan2.
   *
   * <p>The fifth is: ExchangeOperator - SeriesScan3.
   */
  @Test
  public void testConsumeOneByOneChildrenPipelineBuilder6() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    DeviceViewNode deviceViewNode = initDeviceViewNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(5);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeChildrenOneByOneNode(deviceViewNode, context);
    // The number of pipeline is 4, since parent pipeline hasn't joined
    assertEquals(4, context.getPipelineNumber());

    // Validate the first pipeline
    assertEquals(4, childrenOperator.size());
    for (int i = 0; i < 4; i++) {
      assertEquals(ExchangeOperator.class, childrenOperator.get(i).getClass());
    }

    // Validate the second pipeline
    ExchangeOperator exchangeOperator1 = (ExchangeOperator) childrenOperator.get(0);
    assertEquals("AlignedSeriesScanNode0", exchangeOperator1.getSourceId().getId());
    assertEquals(-1, context.getPipelineDriverFactories().get(0).getDependencyPipelineIndex());

    // Validate the third pipeline
    ExchangeOperator exchangeOperator2 = (ExchangeOperator) childrenOperator.get(1);
    assertEquals("AlignedSeriesScanNode1", exchangeOperator2.getSourceId().getId());
    assertEquals(-1, context.getPipelineDriverFactories().get(1).getDependencyPipelineIndex());

    // Validate the forth pipeline
    ExchangeOperator exchangeOperator3 = (ExchangeOperator) childrenOperator.get(2);
    assertEquals("AlignedSeriesScanNode2", exchangeOperator3.getSourceId().getId());
    assertEquals(-1, context.getPipelineDriverFactories().get(2).getDependencyPipelineIndex());

    // Validate the fifth pipeline
    ExchangeOperator exchangeOperator4 = (ExchangeOperator) childrenOperator.get(3);
    assertEquals("AlignedSeriesScanNode3", exchangeOperator4.getSourceId().getId());
    assertEquals(-1, context.getPipelineDriverFactories().get(3).getDependencyPipelineIndex());

    // Validate the number exchange operator
    assertEquals(4, context.getExchangeSumNum());

    // Validate PipelineMemoryEstimator
    try {
      PipelineMemoryEstimator rootPipelineMemoryEstimator =
          context.constructPipelineMemoryEstimator(null, null, deviceViewNode, -1);
      assertEquals(
          ConsumeChildrenOneByOnePipelineMemoryEstimator.class,
          rootPipelineMemoryEstimator.getClass());

      List<PipelineMemoryEstimator> childrenPipelineMemoryEstimators =
          rootPipelineMemoryEstimator.getChildren();
      assertEquals(4, childrenPipelineMemoryEstimators.size());
      for (int i = 0; i < 4; i++) {
        assertEquals(
            ConsumeAllChildrenPipelineMemoryEstimator.class,
            childrenPipelineMemoryEstimators.get(i).getClass());
        assertEquals(
            AlignedSeriesScanOperator.class,
            childrenPipelineMemoryEstimators.get(i).getRoot().getClass());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * The operator structure is:
   *
   * <p>DeviceViewOperator - [AggregationOperator1 - [SeriesAggregationScanOperator1,
   * ExchangeOperator1]], [AggregationOperator2 - [SeriesAggregationScanOperator2,
   * ExchangeOperator2]].
   *
   * <p>This test will test dop = 3. Expected result is five pipelines with dependency:
   *
   * <p>The pipeline0 is: ExchangeOperator - SeriesAggregationScanOperator1.
   *
   * <p>The pipeline1 is: ExchangeOperator - AggregationOperator1.
   *
   * <p>The pipeline2 is: ExchangeOperator - SeriesAggregationScanOperator2, which has dependency 1.
   *
   * <p>The pipeline3 is: ExchangeOperator - AggregationOperator2, which has dependency1.
   *
   * <p>The pipeline4 is: DeviceView - [ExchangeOperator, ExchangeOperator]
   */
  @Test
  public void testConsumeOneByOneChildrenPipelineBuilderDependency() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    typeProvider.setTreeModelType("root.sg.d0.s1", TSDataType.INT64);
    typeProvider.setTreeModelType("root.sg.d1.s1", TSDataType.INT64);
    typeProvider.setTreeModelType("count(root.sg.d0.s1)", TSDataType.INT64);
    typeProvider.setTreeModelType("count(root.sg.d1.s1)", TSDataType.INT64);
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(new PlanNodeId("DeviceViewNode"), null, null, null);
    for (int i = 0; i < 2; i++) {
      PartialPath path = new MeasurementPath(String.format("root.sg.d%d.s1", i), TSDataType.INT64);
      AggregationNode aggregationNode =
          new AggregationNode(
              new PlanNodeId(String.format("AggregationOperator%d", i)),
              Collections.singletonList(
                  new AggregationDescriptor(
                      TAggregationType.COUNT.name().toLowerCase(),
                      AggregationStep.FINAL,
                      Collections.singletonList(new TimeSeriesOperand(path)))),
              null,
              Ordering.ASC);
      SeriesAggregationScanNode seriesAggregationScanNode =
          new SeriesAggregationScanNode(
              new PlanNodeId(String.format("seriesAggregationScanNode%d", i)),
              (MeasurementPath) path,
              Collections.singletonList(
                  new AggregationDescriptor(
                      TAggregationType.COUNT.name().toLowerCase(),
                      AggregationStep.PARTIAL,
                      Collections.singletonList(new TimeSeriesOperand(path)))));
      ExchangeNode exchangeNode =
          new ExchangeNode(new PlanNodeId(String.format("ExchangeNode%d", i)));
      exchangeNode.setUpstream(
          new TEndPoint("127.0.0.1", 6667),
          new FragmentInstanceId(new PlanFragmentId("q", 1), "ds"),
          new PlanNodeId("test"));
      aggregationNode.addChild(seriesAggregationScanNode);
      aggregationNode.addChild(exchangeNode);
      deviceViewNode.addChild(aggregationNode);
    }
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(3);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeChildrenOneByOneNode(deviceViewNode, context);
    // The number of pipeline is 4, since parent pipeline hasn't joined
    assertEquals(4, context.getPipelineNumber());

    assertEquals(2, childrenOperator.size());
    for (int i = 0; i < 2; i++) {
      assertEquals(ExchangeOperator.class, childrenOperator.get(i).getClass());
    }

    // Validate the first pipeline
    assertEquals(-1, context.getPipelineDriverFactories().get(0).getDependencyPipelineIndex());

    // Validate the second pipeline
    assertEquals(-1, context.getPipelineDriverFactories().get(1).getDependencyPipelineIndex());

    // Validate the third pipeline
    assertEquals(1, context.getPipelineDriverFactories().get(2).getDependencyPipelineIndex());

    // Validate the forth pipeline
    assertEquals(1, context.getPipelineDriverFactories().get(2).getDependencyPipelineIndex());
  }

  @Test
  public void testGetChildNumInEachPipeline() {
    List<PlanNode> allChildren = new ArrayList<>();
    allChildren.add(new ExchangeNode(new PlanNodeId("remoteNode1")));
    allChildren.add(new SeriesScanNode(new PlanNodeId("localNode1"), null));
    allChildren.add(new ExchangeNode(new PlanNodeId("remoteNode2")));
    allChildren.add(new SeriesScanNode(new PlanNodeId("localNode2"), null));

    int[] childNumInEachPipeline =
        operatorTreeGenerator.getChildNumInEachPipeline(allChildren, 2, 2);
    assertEquals(2, childNumInEachPipeline.length);
    assertEquals(2, childNumInEachPipeline[0]);
    assertEquals(1, childNumInEachPipeline[1]);

    allChildren.add(new SeriesScanNode(new PlanNodeId("localNode3"), null));
    allChildren.add(new SeriesScanNode(new PlanNodeId("localNode4"), null));
    allChildren.add(new ExchangeNode(new PlanNodeId("remoteNode3")));
    allChildren.add(new ExchangeNode(new PlanNodeId("remoteNode4")));
    allChildren.add(new SeriesScanNode(new PlanNodeId("localNode5"), null));
    allChildren.add(new ExchangeNode(new PlanNodeId("remoteNode5")));
    childNumInEachPipeline = operatorTreeGenerator.getChildNumInEachPipeline(allChildren, 5, 3);
    assertEquals(3, childNumInEachPipeline.length);
    assertEquals(2, childNumInEachPipeline[0]);
    assertEquals(2, childNumInEachPipeline[1]);
    assertEquals(5, childNumInEachPipeline[2]);
  }

  /**
   * The operator structure is [LeftOuterTimeJoin - [SeriesScan0,SeriesScan1]].
   *
   * <p>The next three tests, I will test this LeftOuterTimeJoinOperator with different dop.
   *
   * <p>The first test will test dop = 1. Expected result is that no child pipelines will be
   * divided.
   */
  @Test
  public void testLeftOuterTimeJoinPipelineBuilder1() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    LeftOuterTimeJoinNode leftOuterTimeJoinNode = initLeftOuterTimeJoinNode(typeProvider);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(1);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeAllChildrenPipelineBreaker(
            leftOuterTimeJoinNode, context);
    assertEquals(0, context.getPipelineNumber());
    assertEquals(2, childrenOperator.size());
    assertEquals(2, leftOuterTimeJoinNode.getChildren().size());

    assertEquals(SeriesScanOperator.class, childrenOperator.get(0).getClass());
    assertEquals(SeriesScanNode.class, leftOuterTimeJoinNode.getLeftChild().getClass());
    assertEquals(
        "root.sg.d0.s1", leftOuterTimeJoinNode.getLeftChild().getOutputColumnNames().get(0));

    assertEquals(SeriesScanOperator.class, childrenOperator.get(1).getClass());
    assertEquals(SeriesScanNode.class, leftOuterTimeJoinNode.getRightChild().getClass());
    assertEquals(
        "root.sg.d1.s1", leftOuterTimeJoinNode.getRightChild().getOutputColumnNames().get(0));

    // Validate the number exchange operator
    assertEquals(0, context.getExchangeSumNum());
  }

  /**
   * This test will test dop = 2. Expected result is two pipelines:
   *
   * <p>The first is: LeftOuterTimeJoin1 - [SeriesScan0, ExchangeOperator];
   *
   * <p>The second is: ExchangeOperator - SeriesScan1.
   */
  @Test
  public void testLeftOuterTimeJoinPipelineBuilder2() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    LeftOuterTimeJoinNode leftOuterTimeJoinNode = initLeftOuterTimeJoinNode(typeProvider);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(2);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeAllChildrenPipelineBreaker(
            leftOuterTimeJoinNode, context);
    assertEquals(1, context.getPipelineNumber());
    assertEquals(2, childrenOperator.size());

    // Validate the first pipeline
    assertEquals(SeriesScanOperator.class, childrenOperator.get(0).getClass());
    assertEquals(
        "root.sg.d0.s1", leftOuterTimeJoinNode.getLeftChild().getOutputColumnNames().get(0));

    // Validate the second pipeline
    ExchangeOperator exchangeOperator = (ExchangeOperator) childrenOperator.get(1);
    assertEquals("SeriesScanNode1", exchangeOperator.getSourceId().getId());
    assertEquals(-1, context.getPipelineDriverFactories().get(0).getDependencyPipelineIndex());

    // Validate the number exchange operator
    assertEquals(1, context.getExchangeSumNum());
  }

  /**
   * This test will test dop = 3. Expected result is two pipelines:
   *
   * <p>The first is: LeftOuterTimeJoin1 - [ExchangeOperator1, ExchangeOperator2];
   *
   * <p>The second is: ExchangeOperator1 - SeriesScan0.
   *
   * <p>The third is: ExchangeOperator2 - SeriesScan1.
   */
  @Test
  public void testLeftOuterTimeJoinPipelineBuilder3() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    LeftOuterTimeJoinNode leftOuterTimeJoinNode = initLeftOuterTimeJoinNode(typeProvider);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(3);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeAllChildrenPipelineBreaker(
            leftOuterTimeJoinNode, context);
    assertEquals(2, context.getPipelineNumber());
    assertEquals(2, childrenOperator.size());

    // Validate the first pipeline
    ExchangeOperator exchangeOperator1 = (ExchangeOperator) childrenOperator.get(0);
    assertEquals("SeriesScanNode0", exchangeOperator1.getSourceId().getId());
    assertEquals(-1, context.getPipelineDriverFactories().get(0).getDependencyPipelineIndex());

    // Validate the second pipeline
    ExchangeOperator exchangeOperator2 = (ExchangeOperator) childrenOperator.get(1);
    assertEquals("SeriesScanNode1", exchangeOperator2.getSourceId().getId());
    assertEquals(-1, context.getPipelineDriverFactories().get(1).getDependencyPipelineIndex());

    // Validate the number exchange operator
    assertEquals(2, context.getExchangeSumNum());
  }

  /** This test will test dop > 3. Expected result is same as dop = 3. */
  @Test
  public void testLeftOuterTimeJoinPipelineBuilder4() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    LeftOuterTimeJoinNode leftOuterTimeJoinNode = initLeftOuterTimeJoinNode(typeProvider);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(4);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeAllChildrenPipelineBreaker(
            leftOuterTimeJoinNode, context);
    assertEquals(2, context.getPipelineNumber());
    assertEquals(2, childrenOperator.size());

    // Validate the first pipeline
    ExchangeOperator exchangeOperator1 = (ExchangeOperator) childrenOperator.get(0);
    assertEquals("SeriesScanNode0", exchangeOperator1.getSourceId().getId());
    assertEquals(-1, context.getPipelineDriverFactories().get(0).getDependencyPipelineIndex());

    // Validate the second pipeline
    ExchangeOperator exchangeOperator2 = (ExchangeOperator) childrenOperator.get(1);
    assertEquals("SeriesScanNode1", exchangeOperator2.getSourceId().getId());
    assertEquals(-1, context.getPipelineDriverFactories().get(1).getDependencyPipelineIndex());

    // Validate the number exchange operator
    assertEquals(2, context.getExchangeSumNum());
  }

  @Test
  public void testConsumeAllChildrenPipelineBuilderWithExchange() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    FullOuterTimeJoinNode fullOuterTimeJoinNode =
        initFullOuterTimeJoinNodeWithExchangeNode(typeProvider, 3, 3);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(1);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeAllChildrenPipelineBreaker(
            fullOuterTimeJoinNode, context);
    assertEquals(0, context.getPipelineNumber());
    assertEquals(6, childrenOperator.size());
    assertEquals(6, fullOuterTimeJoinNode.getChildren().size());
    for (int i = 0; i < 3; i++) {
      assertEquals(ExchangeOperator.class, childrenOperator.get(i).getClass());
      assertEquals(ExchangeNode.class, fullOuterTimeJoinNode.getChildren().get(i).getClass());
    }

    for (int i = 3; i < 6; i++) {
      assertEquals(SeriesScanOperator.class, childrenOperator.get(i).getClass());
      assertEquals(SeriesScanNode.class, fullOuterTimeJoinNode.getChildren().get(i).getClass());
      assertEquals(
          String.format("root.sg.d%d.s1", i - 3),
          fullOuterTimeJoinNode.getChildren().get(i).getOutputColumnNames().get(0));
    }

    // Validate the number exchange operator
    assertEquals(3, context.getExchangeSumNum());
  }

  @Test
  public void testIdentitySinkNodeMemoryEstimatorWithDop1() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    IdentitySinkNode identitySinkNode = initIdentitySinkNode(typeProvider);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(1);

    // Validate PipelineMemoryEstimator
    try (Operator root = identitySinkNode.accept(operatorTreeGenerator, context)) {
      PipelineMemoryEstimator rootPipelineMemoryEstimator =
          context.constructPipelineMemoryEstimator(root, null, identitySinkNode, -1);
      assertEquals(
          ConsumeChildrenOneByOnePipelineMemoryEstimator.class,
          rootPipelineMemoryEstimator.getClass());
      assertEquals(
          root.calculateMaxPeekMemoryWithCounter(),
          rootPipelineMemoryEstimator.calculateEstimatedRunningMemorySize());

      List<PipelineMemoryEstimator> childrenPipelineMemoryEstimators =
          rootPipelineMemoryEstimator.getChildren();
      assertEquals(0, childrenPipelineMemoryEstimators.size());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * The operator structure is:
   *
   * <p>IdentitySinkOperator - [ExchangeOperator1, ExchangeOperator2, ExchangeOperator3]
   *
   * <p>This test will test dop = 2. Expected result is four pipelines with dependency:
   *
   * <p>The pipeline0 is: ExchangeOperator1 - FullOuterTimeJoinOperator1.
   *
   * <p>The pipeline1 is: ExchangeOperator2 - FullOuterTimeJoinOperator2, which has dependency 0.
   *
   * <p>The pipeline2 is: ExchangeOperator2 - FullOuterTimeJoinOperator2, which has dependency 1.
   *
   * <p>The pipeline3 is: IdentitySinkOperator - [ExchangeOperator1, ExchangeOperator2,
   * ExchangeOperator3]
   */
  @Test
  public void testIdentitySinkNodeMemoryEstimatorWithDop2() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    IdentitySinkNode identitySinkNode = initIdentitySinkNode(typeProvider);
    LocalExecutionPlanContext context =
        createLocalExecutionPlanContextWithQueryId(typeProvider, new QueryId("test_dop2"));
    context.setDegreeOfParallelism(2);

    // Validate PipelineMemoryEstimator
    try (Operator root = identitySinkNode.accept(operatorTreeGenerator, context)) {
      PipelineMemoryEstimator rootPipelineMemoryEstimator =
          context.constructPipelineMemoryEstimator(root, null, identitySinkNode, -1);
      assertEquals(
          ConsumeChildrenOneByOnePipelineMemoryEstimator.class,
          rootPipelineMemoryEstimator.getClass());
      assertEquals(
          root.calculateMaxPeekMemoryWithCounter()
              + rootPipelineMemoryEstimator.getChildren().stream()
                      .map(PipelineMemoryEstimator::calculateEstimatedRunningMemorySize)
                      .reduce(0L, Long::sum)
                  / 3,
          rootPipelineMemoryEstimator.calculateEstimatedRunningMemorySize());

      List<PipelineMemoryEstimator> childrenPipelineMemoryEstimators =
          rootPipelineMemoryEstimator.getChildren();
      assertEquals(3, childrenPipelineMemoryEstimators.size());
      for (int i = 0; i < 3; i++) {
        assertEquals(
            ConsumeAllChildrenPipelineMemoryEstimator.class,
            childrenPipelineMemoryEstimators.get(i).getClass());
        assertEquals(
            FullOuterTimeJoinOperator.class,
            childrenPipelineMemoryEstimators.get(i).getRoot().getClass());
        assertEquals(0, childrenPipelineMemoryEstimators.get(i).getChildren().size());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * The operator structure is:
   *
   * <p>IdentitySinkOperator - [ExchangeOperator - FullOuterTimeJoinOperator1 -
   * [SeriesScanOperator1, ExchangeOperator, ExchangeOperator], ExchangeOperator -
   * FullOuterTimeJoinOperator2 - [SeriesScanOperator4, ExchangeOperator, ExchangeOperator],
   * ExchangeOperator - FullOuterTimeJoinOperator3 - [SeriesScanOperator7, ExchangeOperator,
   * ExchangeOperator]]
   *
   * <p>This test will test dop = 4. Expected result is ten pipelines with dependency:
   *
   * <p>The pipeline0 is: ExchangeOperator - SeriesOperator2.
   *
   * <p>The pipeline1 is: ExchangeOperator - SeriesOperator3.
   *
   * <p>The pipeline2 is: ExchangeOperator - FullOuterTimeJoinOperator1 - [SeriesScanOperator1,
   * ExchangeOperator, ExchangeOperator]
   *
   * <p>The pipeline3 is: ExchangeOperator - SeriesOperator5, which has dependency 2.
   *
   * <p>The pipeline4 is: ExchangeOperator - SeriesOperator6, which has dependency 2.
   *
   * <p>The pipeline5 is: ExchangeOperator - FullOuterTimeJoinOperator2 - [SeriesScanOperator4,
   * ExchangeOperator, ExchangeOperator], which has dependency 2.
   *
   * <p>The pipeline7 is: ExchangeOperator - SeriesOperator5, which has dependency 5.
   *
   * <p>The pipeline8 is: ExchangeOperator - SeriesOperator6, which has dependency 5.
   *
   * <p>The pipeline9 is: ExchangeOperator - FullOuterTimeJoinOperator2 - [SeriesScanOperator4,
   * ExchangeOperator, ExchangeOperator], which has dependency 5.
   *
   * <p>The pipeline10 is: IdentitySinkOperator - [ExchangeOperator1, ExchangeOperator2,
   * ExchangeOperator3]
   */
  @Test
  public void testIdentitySinkNodeMemoryEstimatorWithDop4() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    IdentitySinkNode identitySinkNode = initIdentitySinkNode(typeProvider);
    LocalExecutionPlanContext context =
        createLocalExecutionPlanContextWithQueryId(typeProvider, new QueryId("test_dop4"));
    context.setDegreeOfParallelism(4);

    // Validate PipelineMemoryEstimator
    try (Operator root = identitySinkNode.accept(operatorTreeGenerator, context)) {
      PipelineMemoryEstimator rootPipelineMemoryEstimator =
          context.constructPipelineMemoryEstimator(root, null, identitySinkNode, -1);
      assertEquals(
          ConsumeChildrenOneByOnePipelineMemoryEstimator.class,
          rootPipelineMemoryEstimator.getClass());
      assertEquals(
          1,
          ((ConsumeChildrenOneByOnePipelineMemoryEstimator) rootPipelineMemoryEstimator)
              .getConcurrentRunningChildrenNumForTest());

      List<PipelineMemoryEstimator> childrenPipelineMemoryEstimators =
          rootPipelineMemoryEstimator.getChildren();
      assertEquals(3, childrenPipelineMemoryEstimators.size());
      for (int i = 0; i < 3; i++) {
        assertEquals(
            ConsumeAllChildrenPipelineMemoryEstimator.class,
            childrenPipelineMemoryEstimators.get(i).getClass());
        assertEquals(
            FullOuterTimeJoinOperator.class,
            childrenPipelineMemoryEstimators.get(i).getRoot().getClass());
        assertEquals(2, childrenPipelineMemoryEstimators.get(i).getChildren().size());
        for (int j = 0; j < 2; j++) {
          assertEquals(
              ConsumeAllChildrenPipelineMemoryEstimator.class,
              childrenPipelineMemoryEstimators.get(i).getChildren().get(j).getClass());
          assertEquals(
              SeriesScanOperator.class,
              childrenPipelineMemoryEstimators.get(i).getChildren().get(j).getRoot().getClass());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testIdentitySinkNodeMemoryEstimatorWithDop8() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    IdentitySinkNode identitySinkNode = initIdentitySinkNode(typeProvider);
    LocalExecutionPlanContext context =
        createLocalExecutionPlanContextWithQueryId(typeProvider, new QueryId("test_dop16"));
    context.setDegreeOfParallelism(8);

    // Validate PipelineMemoryEstimator
    try (Operator root = identitySinkNode.accept(operatorTreeGenerator, context)) {
      PipelineMemoryEstimator rootPipelineMemoryEstimator =
          context.constructPipelineMemoryEstimator(root, null, identitySinkNode, -1);
      assertEquals(
          ConsumeChildrenOneByOnePipelineMemoryEstimator.class,
          rootPipelineMemoryEstimator.getClass());
      assertEquals(
          1,
          ((ConsumeChildrenOneByOnePipelineMemoryEstimator) rootPipelineMemoryEstimator)
              .getConcurrentRunningChildrenNumForTest());

      List<PipelineMemoryEstimator> childrenPipelineMemoryEstimators =
          rootPipelineMemoryEstimator.getChildren();
      assertEquals(3, childrenPipelineMemoryEstimators.size());
      for (int i = 0; i < 3; i++) {
        assertEquals(
            ConsumeAllChildrenPipelineMemoryEstimator.class,
            childrenPipelineMemoryEstimators.get(i).getClass());
        assertEquals(
            FullOuterTimeJoinOperator.class,
            childrenPipelineMemoryEstimators.get(i).getRoot().getClass());
        for (int j = 0; j < 3; j++) {
          assertEquals(
              ConsumeAllChildrenPipelineMemoryEstimator.class,
              childrenPipelineMemoryEstimators.get(i).getChildren().get(j).getClass());
          assertEquals(
              SeriesScanOperator.class,
              childrenPipelineMemoryEstimators.get(i).getChildren().get(j).getRoot().getClass());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testIdentitySinkNodeMemoryEstimatorWithDop16() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    IdentitySinkNode identitySinkNode = initIdentitySinkNode(typeProvider);
    LocalExecutionPlanContext context =
        createLocalExecutionPlanContextWithQueryId(typeProvider, new QueryId("test_dop8"));
    context.setDegreeOfParallelism(16);

    // Validate PipelineMemoryEstimator
    try (Operator root = identitySinkNode.accept(operatorTreeGenerator, context)) {
      PipelineMemoryEstimator rootPipelineMemoryEstimator =
          context.constructPipelineMemoryEstimator(root, null, identitySinkNode, -1);
      assertEquals(
          ConsumeChildrenOneByOnePipelineMemoryEstimator.class,
          rootPipelineMemoryEstimator.getClass());
      // all the pipeline under this node will be executed concurrently
      assertEquals(
          3,
          ((ConsumeChildrenOneByOnePipelineMemoryEstimator) rootPipelineMemoryEstimator)
              .getConcurrentRunningChildrenNumForTest());

      List<PipelineMemoryEstimator> childrenPipelineMemoryEstimators =
          rootPipelineMemoryEstimator.getChildren();
      assertEquals(3, childrenPipelineMemoryEstimators.size());
      for (int i = 0; i < 3; i++) {
        assertEquals(
            ConsumeAllChildrenPipelineMemoryEstimator.class,
            childrenPipelineMemoryEstimators.get(i).getClass());
        assertEquals(
            FullOuterTimeJoinOperator.class,
            childrenPipelineMemoryEstimators.get(i).getRoot().getClass());
        for (int j = 0; j < 3; j++) {
          assertEquals(
              ConsumeAllChildrenPipelineMemoryEstimator.class,
              childrenPipelineMemoryEstimators.get(i).getChildren().get(j).getClass());
          assertEquals(
              SeriesScanOperator.class,
              childrenPipelineMemoryEstimators.get(i).getChildren().get(j).getRoot().getClass());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  private LocalExecutionPlanContext createLocalExecutionPlanContext(TypeProvider typeProvider) {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");

    QueryId queryId = new QueryId("stub_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    DataRegion dataRegion = Mockito.mock(DataRegion.class);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    fragmentInstanceContext.setDataRegion(dataRegion);

    return new LocalExecutionPlanContext(
        typeProvider, fragmentInstanceContext, new DataNodeQueryContext(1));
  }

  private LocalExecutionPlanContext createLocalExecutionPlanContextWithQueryId(
      TypeProvider typeProvider, QueryId queryId) {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");

    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    DataRegion dataRegion = Mockito.mock(DataRegion.class);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    fragmentInstanceContext.setDataRegion(dataRegion);

    return new LocalExecutionPlanContext(
        typeProvider, fragmentInstanceContext, new DataNodeQueryContext(1));
  }

  /**
   * This method will init a timeJoinNode with @childNum seriesScanNode as children.
   *
   * @param childNum the number of children
   * @return a timeJoinNode with @childNum seriesScanNode as children
   */
  private FullOuterTimeJoinNode initFullOuterTimeJoinNode(TypeProvider typeProvider, int childNum)
      throws IllegalPathException {
    FullOuterTimeJoinNode fullOuterTimeJoinNode =
        new FullOuterTimeJoinNode(new PlanNodeId("TimeJoinNode"), Ordering.ASC);
    for (int i = 0; i < childNum; i++) {
      SeriesScanNode seriesScanNode =
          new SeriesScanNode(
              new PlanNodeId(String.format("SeriesScanNode%d", i)),
              new MeasurementPath(String.format("root.sg.d%d.s1", i), TSDataType.INT32));
      typeProvider.setTreeModelType(seriesScanNode.getSeriesPath().toString(), TSDataType.INT32);
      fullOuterTimeJoinNode.addChild(seriesScanNode);
    }
    return fullOuterTimeJoinNode;
  }

  private FullOuterTimeJoinNode initFullOuterTimeJoinNodeWithExchangeNode(
      TypeProvider typeProvider, int exchangeNum, int scanNum) throws IllegalPathException {
    FullOuterTimeJoinNode fullOuterTimeJoinNode =
        new FullOuterTimeJoinNode(new PlanNodeId("TimeJoinNode"), Ordering.ASC);
    for (int i = 0; i < exchangeNum; i++) {
      ExchangeNode exchangeNode =
          new ExchangeNode(new PlanNodeId(String.format("FullOuterTimeJoinWithExchangeNode%d", i)));
      exchangeNode.setUpstream(
          new TEndPoint("127.0.0.2", 6667),
          new FragmentInstanceId(new PlanFragmentId("q", i), "ds"),
          new PlanNodeId("test"));
      fullOuterTimeJoinNode.addChild(exchangeNode);
    }
    for (int i = 0; i < scanNum; i++) {
      SeriesScanNode seriesScanNode =
          new SeriesScanNode(
              new PlanNodeId(String.format("SeriesScanNode%d", i)),
              new MeasurementPath(String.format("root.sg.d%d.s1", i), TSDataType.INT32));
      typeProvider.setTreeModelType(seriesScanNode.getSeriesPath().toString(), TSDataType.INT32);
      fullOuterTimeJoinNode.addChild(seriesScanNode);
    }
    return fullOuterTimeJoinNode;
  }

  private LeftOuterTimeJoinNode initLeftOuterTimeJoinNode(TypeProvider typeProvider)
      throws IllegalPathException {
    LeftOuterTimeJoinNode leftOuterTimeJoinNode =
        new LeftOuterTimeJoinNode(new PlanNodeId("TimeJoinNode"), Ordering.ASC);
    for (int i = 0; i < 2; i++) {
      SeriesScanNode seriesScanNode =
          new SeriesScanNode(
              new PlanNodeId(String.format("SeriesScanNode%d", i)),
              new MeasurementPath(String.format("root.sg.d%d.s1", i), TSDataType.INT32));
      typeProvider.setTreeModelType(seriesScanNode.getSeriesPath().toString(), TSDataType.INT32);
      leftOuterTimeJoinNode.addChild(seriesScanNode);
    }
    return leftOuterTimeJoinNode;
  }

  /**
   * This method will init a DeviceViewNode with @childNum alignedSeriesScanNode as children.
   *
   * @param childNum the number of children
   * @return a DeviceViewNode with @childNum alignedSeriesScanNode as children
   */
  private DeviceViewNode initDeviceViewNode(TypeProvider typeProvider, int childNum)
      throws IllegalPathException {
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(new PlanNodeId("DeviceViewNode"), null, null, null);
    for (int i = 0; i < childNum; i++) {
      AlignedSeriesScanNode alignedSeriesScanNode =
          new AlignedSeriesScanNode(
              new PlanNodeId(String.format("AlignedSeriesScanNode%d", i)),
              new AlignedPath(String.format("root.sg.d%d", i), "s1"));
      deviceViewNode.addChild(alignedSeriesScanNode);
    }
    return deviceViewNode;
  }

  private TopKNode initTopKNode(TypeProvider typeProvider, int childNum)
      throws IllegalPathException {
    TopKNode topKNode =
        new TopKNode(
            new PlanNodeId("TopKNode"),
            10,
            new OrderByParameter(
                Arrays.asList(
                    new SortItem(OrderByKey.TIME, Ordering.ASC),
                    new SortItem(DEVICE, Ordering.ASC))),
            Arrays.asList("Time", "Device", "s1"));
    for (int i = 0; i < childNum; i++) {
      SingleDeviceViewNode singleDeviceViewNode =
          new SingleDeviceViewNode(
              new PlanNodeId(String.format("SingleDeviceViewNode%d", i)),
              Arrays.asList("Time", "Device", "s1"),
              IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d" + i),
              Arrays.asList(0, 1, 2));
      singleDeviceViewNode.setCacheOutputColumnNames(true);
      SeriesScanNode seriesScanNode =
          new SeriesScanNode(
              new PlanNodeId(String.format("SeriesScanNode%d", i)),
              new MeasurementPath(String.format("root.sg.d%d.s1", i), TSDataType.INT32));
      typeProvider.setTreeModelType(seriesScanNode.getSeriesPath().toString(), TSDataType.INT32);
      singleDeviceViewNode.addChild(seriesScanNode);
      typeProvider.setTreeModelType("Time", TSDataType.INT64);
      typeProvider.setTreeModelType("Device", TSDataType.TEXT);
      typeProvider.setTreeModelType("s1", TSDataType.INT32);
      topKNode.addChild(singleDeviceViewNode);
    }
    return topKNode;
  }

  private IdentitySinkNode initIdentitySinkNode(TypeProvider typeProvider)
      throws IllegalPathException {
    FullOuterTimeJoinNode fullOuterTimeJoinNode1 = initFullOuterTimeJoinNode(typeProvider, 3);
    FullOuterTimeJoinNode fullOuterTimeJoinNode2 = initFullOuterTimeJoinNode(typeProvider, 3);
    FullOuterTimeJoinNode fullOuterTimeJoinNode3 = initFullOuterTimeJoinNode(typeProvider, 3);
    fullOuterTimeJoinNode1.setPlanNodeId(new PlanNodeId("FullOuterTimeJoinNode1"));
    fullOuterTimeJoinNode2.setPlanNodeId(new PlanNodeId("FullOuterTimeJoinNode2"));
    fullOuterTimeJoinNode3.setPlanNodeId(new PlanNodeId("FullOuterTimeJoinNode3"));

    IdentitySinkNode identitySinkNode = new IdentitySinkNode(new PlanNodeId("IdentitySinkNode"));
    identitySinkNode.addChild(fullOuterTimeJoinNode1);
    identitySinkNode.addChild(fullOuterTimeJoinNode2);
    identitySinkNode.addChild(fullOuterTimeJoinNode3);
    return identitySinkNode;
  }
}
