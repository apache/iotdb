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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.plan.planner.FEPlanUtil.createLocalExecutionPlanContext;
import static org.apache.iotdb.db.queryengine.plan.planner.FEPlanUtil.initDeviceViewNodeWithSeriesScanAsChildren;
import static org.apache.iotdb.db.queryengine.plan.planner.FEPlanUtil.initFullOuterTimeJoinNode;

public class LocalExecutionPlannerTest {

  private static final long ESTIMATED_FI_NUM = 8;

  private static final long QUERY_THREAD_COUNT = 8;

  private static final SeriesScanOperator MOCK_SERIES_SCAN = mockSeriesScanOperator();

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
  public void testCheckMemoryWithDeviceView() {
    try {
      TypeProvider typeProvider = new TypeProvider();
      DeviceViewNode deviceViewNode = initDeviceViewNodeWithSeriesScanAsChildren(typeProvider, 4);
      LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
      context.setDegreeOfParallelism(2);
      Operator root = deviceViewNode.accept(new OperatorTreeGenerator(), context);

      // all the four children pipelines are ScanOperator
      context
          .getPipelineDriverFactories()
          .forEach(
              pipelineDriverFactory ->
                  Assert.assertEquals(
                      MOCK_SERIES_SCAN.calculateMaxPeekMemory(),
                      pipelineDriverFactory.getEstimatedMemorySize()));

      context.addPipelineDriverFactory(
          root, context.getDriverContext(), root.calculateMaxPeekMemory());

      long expected =
          4 * MOCK_SERIES_SCAN.calculateRetainedSizeAfterCallingNext()
              + context.getPipelineDriverFactories().stream()
                      .map(PipelineDriverFactory::getEstimatedMemorySize)
                      .reduce(0L, Long::sum)
                  / 5
                  * Math.max((QUERY_THREAD_COUNT / ESTIMATED_FI_NUM), 1);

      Assert.assertEquals(
          expected, calculateEstimatedMemorySize(context.getPipelineDriverFactories()));
    } catch (Exception e) {
      Assert.fail();
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
  public void testCheckMemoryWithTimeJoin() throws IllegalPathException {
    TypeProvider typeProvider = new TypeProvider();
    FullOuterTimeJoinNode timeJoinNode = initFullOuterTimeJoinNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(2);

    Operator root = timeJoinNode.accept(new OperatorTreeGenerator(), context);

    context.addPipelineDriverFactory(
        root, context.getDriverContext(), root.calculateMaxPeekMemory());

    long expected =
        context
                .getPipelineDriverFactories()
                .get(0)
                .getOperation()
                .calculateRetainedSizeAfterCallingNext()
            + context
                .getPipelineDriverFactories()
                .get(1)
                .getOperation()
                .calculateRetainedSizeAfterCallingNext()
            + context.getPipelineDriverFactories().stream()
                    .map(PipelineDriverFactory::getEstimatedMemorySize)
                    .reduce(0L, Long::sum)
                / 2
                * Math.max((QUERY_THREAD_COUNT / ESTIMATED_FI_NUM), 1);

    Assert.assertEquals(
        expected, calculateEstimatedMemorySize(context.getPipelineDriverFactories()));
  }

  private long calculateEstimatedMemorySize(
      final List<PipelineDriverFactory> pipelineDriverFactories) {
    long result = 0;
    try {
      LocalExecutionPlanner localExecutionPlanner = new LocalExecutionPlanner();
      Method method =
          LocalExecutionPlanner.class.getDeclaredMethod("calculateEstimatedMemorySize", List.class);
      method.setAccessible(true);
      result = (long) method.invoke(localExecutionPlanner, pipelineDriverFactories); // 调用私有方法
    } catch (Exception e) {
      e.printStackTrace();
    }
    return result;
  }

  private static SeriesScanOperator mockSeriesScanOperator() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    MeasurementPath measurementPath;
    try {
      measurementPath =
          new MeasurementPath("root.SeriesScanOperatorTest" + ".device0.sensor0", TSDataType.INT32);
    } catch (IllegalPathException e) {
      throw new RuntimeException(e);
    }
    Set<String> allSensors = Sets.newHashSet("sensor0");
    QueryId queryId = new QueryId("stub_query");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNodeId = new PlanNodeId("1");
    driverContext.addOperatorContext(1, planNodeId, SeriesScanOperator.class.getSimpleName());

    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
    scanOptionsBuilder.withAllSensors(allSensors);
    return new SeriesScanOperator(
        driverContext.getOperatorContexts().get(0),
        planNodeId,
        measurementPath,
        Ordering.ASC,
        scanOptionsBuilder.build());
  }
}
