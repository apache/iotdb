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

package org.apache.iotdb.db.queryengine.plan.plan;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanContext;
import org.apache.iotdb.db.queryengine.plan.planner.OperatorTreeGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.PipelineDriverFactory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.plan.FEPlanUtil.createLocalExecutionPlanContext;
import static org.apache.iotdb.db.queryengine.plan.plan.FEPlanUtil.initDeviceViewNode;
import static org.apache.iotdb.db.queryengine.plan.plan.FEPlanUtil.initTimeJoinNode;

public class LocalExecutionPlannerTest {

  private static final long ESTIMATED_FI_NUM = 8;

  private static final long QUERY_THREAD_COUNT = 8;

  private static final long ALIGNED_MAX_SIZE =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

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
      DeviceViewNode deviceViewNode = initDeviceViewNode(typeProvider, 4);
      LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
      context.setDegreeOfParallelism(2);
      Operator root = deviceViewNode.accept(new OperatorTreeGenerator(), context);

      // all the four children pipelines are ScanOperator
      context
          .getPipelineDriverFactories()
          .forEach(
              pipelineDriverFactory ->
                  Assert.assertEquals(
                      ALIGNED_MAX_SIZE, pipelineDriverFactory.getEstimatedMemorySize()));

      context.addPipelineDriverFactory(
          root, context.getDriverContext(), root.calculateMaxPeekMemory());
      Assert.assertEquals(
          ALIGNED_MAX_SIZE, calculateEstimatedMemorySize(context.getPipelineDriverFactories()));
      Assert.assertEquals(
          calculateEstimatedMemorySizeFromOperation(context.getPipelineDriverFactories()),
          calculateEstimatedMemorySize(context.getPipelineDriverFactories()));
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
    TimeJoinNode timeJoinNode = initTimeJoinNode(typeProvider, 4);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext(typeProvider);
    context.setDegreeOfParallelism(2);

    Operator root = timeJoinNode.accept(new OperatorTreeGenerator(), context);

    // The second pipeline is a RowBasedTimeJoinOperator
    Assert.assertEquals(
        2 * ALIGNED_MAX_SIZE, context.getPipelineDriverFactories().get(0).getEstimatedMemorySize());

    context.addPipelineDriverFactory(
        root, context.getDriverContext(), root.calculateMaxPeekMemory());

    // ALIGNED_MAX_SIZE * 5 / 2 is calculated by directly applying the algorithm on the Operator
    // Tree.
    Assert.assertEquals(
        ALIGNED_MAX_SIZE * 5 / 2,
        calculateEstimatedMemorySize(context.getPipelineDriverFactories()));
    Assert.assertEquals(
        calculateEstimatedMemorySizeFromOperation(context.getPipelineDriverFactories()),
        calculateEstimatedMemorySize(context.getPipelineDriverFactories()));
  }

  private long calculateEstimatedMemorySizeFromOperation(
      final List<PipelineDriverFactory> pipelineDriverFactories) {
    long totalSizeOfDrivers =
        pipelineDriverFactories.stream()
            .map(
                pipelineDriverFactory ->
                    pipelineDriverFactory.getOperation().calculateMaxPeekMemory())
            .reduce(0L, Long::sum);
    return Math.max((QUERY_THREAD_COUNT / ESTIMATED_FI_NUM), 1)
        * (totalSizeOfDrivers / pipelineDriverFactories.size());
  }

  private long calculateEstimatedMemorySize(
      final List<PipelineDriverFactory> pipelineDriverFactories) {
    long totalSizeOfDrivers =
        pipelineDriverFactories.stream()
            .map(PipelineDriverFactory::getEstimatedMemorySize)
            .reduce(0L, Long::sum);
    return Math.max((QUERY_THREAD_COUNT / ESTIMATED_FI_NUM), 1)
        * (totalSizeOfDrivers / pipelineDriverFactories.size());
  }
}
