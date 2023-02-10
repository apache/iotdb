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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.planner.LocalExecutionPlanContext;
import org.apache.iotdb.db.mpp.plan.planner.OperatorTreeGenerator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Test;
import org.mockito.Mockito;

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
    assertEquals("root.sg.d1.s1", timeJoinNode.getChildren().get(0).getOutputColumnNames().get(0));
    assertEquals("root.sg.d0.s1", timeJoinNode.getChildren().get(1).getOutputColumnNames().get(0));
    assertEquals(TimeJoinNode.class, timeJoinNode.getChildren().get(2).getClass());

    // Validate the second pipeline
    TimeJoinNode subTimeJoinNode = (TimeJoinNode) timeJoinNode.getChildren().get(2);
    assertEquals(2, subTimeJoinNode.getChildren().size());
    assertEquals(
        "root.sg.d2.s1", subTimeJoinNode.getChildren().get(0).getOutputColumnNames().get(0));
    assertEquals(
        "root.sg.d3.s1", subTimeJoinNode.getChildren().get(1).getOutputColumnNames().get(0));
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
  }

  /**
   * This test will test dop = 4. Expected result is five pipelines:
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
}
