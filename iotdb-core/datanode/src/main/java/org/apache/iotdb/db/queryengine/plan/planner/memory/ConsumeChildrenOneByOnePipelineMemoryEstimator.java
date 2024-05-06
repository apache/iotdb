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

package org.apache.iotdb.db.queryengine.plan.planner.memory;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;

public class ConsumeChildrenOneByOnePipelineMemoryEstimator extends PipelineMemoryEstimator {

  private long concurrentRunningChildrenNum = -1;

  private boolean concurrentRunningChildrenNumInitialized = false;

  public ConsumeChildrenOneByOnePipelineMemoryEstimator(
      final Operator root, final int dependencyPipelineIndex) {
    super(root, dependencyPipelineIndex);
  }

  @Override
  public long calculateEstimatedRunningMemorySize() {
    // EstimatedSize = root.calculateMaxPeekMemoryWithCounter() + sum(children's estimated size) *
    // runningChildrenNum / children.size()
    return children.isEmpty()
        ? root.calculateMaxPeekMemoryWithCounter()
        : (long)
            (root.calculateMaxPeekMemoryWithCounter()
                + children.stream()
                        .map(PipelineMemoryEstimator::calculateEstimatedRunningMemorySize)
                        .reduce(0L, Long::sum)
                    / (double) (children.size())
                    * getConcurrentRunningChildrenNum());
  }

  private long getConcurrentRunningChildrenNum() {
    if (concurrentRunningChildrenNumInitialized) {
      return concurrentRunningChildrenNum;
    }
    concurrentRunningChildrenNum =
        children.stream()
            .filter(memoryEstimator -> memoryEstimator.getDependencyPipelineIndex() == -1)
            .count();
    concurrentRunningChildrenNumInitialized = true;
    return concurrentRunningChildrenNum;
  }

  @TestOnly
  public long getConcurrentRunningChildrenNumForTest() {
    return children.stream()
        .filter(memoryEstimator -> memoryEstimator.getDependencyPipelineIndex() == -1)
        .count();
  }
}
