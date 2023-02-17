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

package org.apache.iotdb.db.mpp.plan.plan;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.source.AlignedSeriesScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.planner.LocalExecutionPlanContext;
import org.apache.iotdb.db.mpp.plan.planner.OperatorTreeGenerator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;

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
    TimeJoinNode timeJoinNode = initTimeJoinNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(1);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeAllChildrenPipelineBreaker(timeJoinNode, context);
    assertEquals(0, context.getPipelineNumber());
    assertEquals(4, childrenOperator.size());
    assertEquals(4, timeJoinNode.getChildren().size());
    for (int i = 0; i < 4; i++) {
      assertEquals(SeriesScanOperator.class, childrenOperator.get(i).getClass());
      assertEquals(SeriesScanNode.class, timeJoinNode.getChildren().get(i).getClass());
      assertEquals(
          String.format("root.sg.d%d.s1", i),
          timeJoinNode.getChildren().get(i).getOutputColumnNames().get(0));
    }

    // Validate the number exchange operator
    assertEquals(0, context.getExchangeSumNum());
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
    TimeJoinNode timeJoinNode = initTimeJoinNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(2);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeAllChildrenPipelineBreaker(timeJoinNode, context);
    // The number of pipeline is 1, since parent pipeline hasn't joined
    assertEquals(1, context.getPipelineNumber());

    // Validate the first pipeline
    assertEquals(3, childrenOperator.size());
    assertEquals(3, timeJoinNode.getChildren().size());
    for (int i = 0; i < 2; i++) {
      assertEquals(SeriesScanOperator.class, childrenOperator.get(i).getClass());
      assertEquals(SeriesScanNode.class, timeJoinNode.getChildren().get(i).getClass());
    }
    assertEquals(ExchangeOperator.class, childrenOperator.get(2).getClass());

    // Validate the changes of node structure
    assertEquals("root.sg.d0.s1", timeJoinNode.getChildren().get(0).getOutputColumnNames().get(0));
    assertEquals("root.sg.d1.s1", timeJoinNode.getChildren().get(1).getOutputColumnNames().get(0));
    assertEquals(TimeJoinNode.class, timeJoinNode.getChildren().get(2).getClass());

    // Validate the second pipeline
    TimeJoinNode subTimeJoinNode = (TimeJoinNode) timeJoinNode.getChildren().get(2);
    assertEquals(2, subTimeJoinNode.getChildren().size());
    assertEquals(
        "root.sg.d2.s1", subTimeJoinNode.getChildren().get(0).getOutputColumnNames().get(0));
    assertEquals(
        "root.sg.d3.s1", subTimeJoinNode.getChildren().get(1).getOutputColumnNames().get(0));

    // Validate the number exchange operator
    assertEquals(1, context.getExchangeSumNum());
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
    TimeJoinNode timeJoinNode = initTimeJoinNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(3);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeAllChildrenPipelineBreaker(timeJoinNode, context);
    // The number of pipeline is 2, since parent pipeline hasn't joined
    assertEquals(2, context.getPipelineNumber());

    // Validate the first pipeline
    assertEquals(3, childrenOperator.size());
    assertEquals(SeriesScanOperator.class, childrenOperator.get(0).getClass());
    assertEquals(ExchangeOperator.class, childrenOperator.get(1).getClass());
    assertEquals(ExchangeOperator.class, childrenOperator.get(2).getClass());

    // Validate the changes of node structure
    assertEquals(3, timeJoinNode.getChildren().size());
    assertEquals("root.sg.d0.s1", timeJoinNode.getChildren().get(0).getOutputColumnNames().get(0));
    assertEquals("root.sg.d1.s1", timeJoinNode.getChildren().get(1).getOutputColumnNames().get(0));
    assertEquals(TimeJoinNode.class, timeJoinNode.getChildren().get(2).getClass());

    // Validate the second pipeline
    ExchangeOperator exchangeOperator1 = (ExchangeOperator) childrenOperator.get(1);
    assertEquals("SeriesScanNode1", exchangeOperator1.getSourceId().getId());

    // Validate the third pipeline
    TimeJoinNode subTimeJoinNode = (TimeJoinNode) timeJoinNode.getChildren().get(2);
    assertEquals(2, subTimeJoinNode.getChildren().size());
    assertEquals(
        "root.sg.d2.s1", subTimeJoinNode.getChildren().get(0).getOutputColumnNames().get(0));
    assertEquals(
        "root.sg.d3.s1", subTimeJoinNode.getChildren().get(1).getOutputColumnNames().get(0));
    ExchangeOperator exchangeOperator2 = (ExchangeOperator) childrenOperator.get(2);
    assertEquals(exchangeOperator2.getSourceId(), subTimeJoinNode.getPlanNodeId());

    // Validate the number exchange operator
    assertEquals(2, context.getExchangeSumNum());
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
    TimeJoinNode timeJoinNode = initTimeJoinNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(4);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeAllChildrenPipelineBreaker(timeJoinNode, context);
    // The number of pipeline is 3, since parent pipeline hasn't joined
    assertEquals(3, context.getPipelineNumber());

    // Validate the first pipeline
    assertEquals(4, childrenOperator.size());
    assertEquals(SeriesScanOperator.class, childrenOperator.get(0).getClass());
    assertEquals(ExchangeOperator.class, childrenOperator.get(1).getClass());
    assertEquals(ExchangeOperator.class, childrenOperator.get(2).getClass());
    assertEquals(ExchangeOperator.class, childrenOperator.get(3).getClass());

    // Validate the changes of node structure
    assertEquals(4, timeJoinNode.getChildren().size());
    for (int i = 0; i < 4; i++) {
      assertEquals(SeriesScanNode.class, timeJoinNode.getChildren().get(i).getClass());
      assertEquals(
          String.format("root.sg.d%d.s1", i),
          timeJoinNode.getChildren().get(i).getOutputColumnNames().get(0));
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
    TimeJoinNode timeJoinNode = initTimeJoinNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(5);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeAllChildrenPipelineBreaker(timeJoinNode, context);
    // The number of pipeline is 4, since parent pipeline hasn't joined
    assertEquals(4, context.getPipelineNumber());

    // Validate the first pipeline
    assertEquals(4, childrenOperator.size());
    for (int i = 0; i < 4; i++) {
      assertEquals(ExchangeOperator.class, childrenOperator.get(i).getClass());
    }

    // Validate the changes of node structure
    assertEquals(4, timeJoinNode.getChildren().size());
    for (int i = 0; i < 4; i++) {
      assertEquals(SeriesScanNode.class, timeJoinNode.getChildren().get(i).getClass());
      assertEquals(
          String.format("root.sg.d%d.s1", i),
          timeJoinNode.getChildren().get(i).getOutputColumnNames().get(0));
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
    TimeJoinNode timeJoinNode = initTimeJoinNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(6);

    List<Operator> childrenOperator =
        operatorTreeGenerator.dealWithConsumeAllChildrenPipelineBreaker(timeJoinNode, context);
    // The number of pipeline is 4, since parent pipeline hasn't joined
    assertEquals(4, context.getPipelineNumber());

    // Validate the first pipeline
    assertEquals(4, childrenOperator.size());
    for (int i = 0; i < 4; i++) {
      assertEquals(ExchangeOperator.class, childrenOperator.get(i).getClass());
    }

    // Validate the changes of node structure
    assertEquals(4, timeJoinNode.getChildren().size());
    for (int i = 0; i < 4; i++) {
      assertEquals(SeriesScanNode.class, timeJoinNode.getChildren().get(i).getClass());
      assertEquals(
          String.format("root.sg.d%d.s1", i),
          timeJoinNode.getChildren().get(i).getOutputColumnNames().get(0));
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

    return new LocalExecutionPlanContext(typeProvider, fragmentInstanceContext);
  }

  /**
   * This method will init a timeJoinNode with @childNum seriesScanNode as children.
   *
   * @param childNum the number of children
   * @return a timeJoinNode with @childNum seriesScanNode as children
   */
  private TimeJoinNode initTimeJoinNode(TypeProvider typeProvider, int childNum)
      throws IllegalPathException {
    TimeJoinNode timeJoinNode = new TimeJoinNode(new PlanNodeId("TimeJoinNode"), Ordering.ASC);
    for (int i = 0; i < childNum; i++) {
      SeriesScanNode seriesScanNode =
          new SeriesScanNode(
              new PlanNodeId(String.format("SeriesScanNode%d", i)),
              new MeasurementPath(String.format("root.sg.d%d.s1", i), TSDataType.INT32));
      typeProvider.setType(seriesScanNode.getSeriesPath().toString(), TSDataType.INT32);
      timeJoinNode.addChild(seriesScanNode);
    }
    return timeJoinNode;
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
}
