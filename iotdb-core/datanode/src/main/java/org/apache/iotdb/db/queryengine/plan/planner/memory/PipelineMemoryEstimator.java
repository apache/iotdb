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

import java.util.LinkedList;
import java.util.List;

public abstract class PipelineMemoryEstimator {

  protected final List<PipelineMemoryEstimator> children;

  protected final Operator root;

  protected final int dependencyPipelineIndex;

  protected PipelineMemoryEstimator(final Operator root, final int dependencyPipelineIndex) {
    this.root = root;
    this.dependencyPipelineIndex = dependencyPipelineIndex;
    this.children = new LinkedList<>();
  }

  /** Calculate the estimated memory size of the pipeline. */
  public abstract long calculateEstimatedMemorySize();

  protected long calculateRetainedMemorySize() {
    return root.getEstimatedMemoryUsageInBytes()
        + children.stream()
            .map(PipelineMemoryEstimator::calculateRetainedMemorySize)
            .reduce(0L, Long::sum);
  }

  public void addChildren(final List<PipelineMemoryEstimator> child) {
    children.addAll(child);
  }

  protected int getDependencyPipelineIndex() {
    return dependencyPipelineIndex;
  }

  @TestOnly
  public List<PipelineMemoryEstimator> getChildren() {
    return children;
  }

  @TestOnly
  public Operator getRoot() {
    return root;
  }
}
