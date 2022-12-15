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
package org.apache.iotdb.db.mpp.execution.operator.schema;

import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public class SchemaQueryMergeOperator implements ProcessOperator {
  private final PlanNodeId planNodeId;
  private final OperatorContext operatorContext;

  private final List<Operator> children;

  private int currentIndex;

  public SchemaQueryMergeOperator(
      PlanNodeId planNodeId, OperatorContext operatorContext, List<Operator> children) {
    this.planNodeId = planNodeId;
    this.operatorContext = operatorContext;
    this.children = children;
    this.currentIndex = 0;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    if (children.get(currentIndex).hasNextWithTimer()) {
      return children.get(currentIndex).nextWithTimer();
    } else {
      currentIndex++;
      return null;
    }
  }

  @Override
  public boolean hasNext() {
    return currentIndex < children.size();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return currentIndex < children.size() ? children.get(currentIndex).isBlocked() : NOT_BLOCKED;
  }

  @Override
  public boolean isFinished() {
    return !hasNextWithTimer();
  }

  @Override
  public void close() throws Exception {
    for (Operator child : children) {
      child.close();
    }
  }

  @Override
  public long calculateMaxPeekMemory() {
    long childrenMaxPeekMemory = 0;
    for (Operator child : children) {
      childrenMaxPeekMemory = Math.max(childrenMaxPeekMemory, child.calculateMaxPeekMemory());
    }

    return childrenMaxPeekMemory;
  }

  @Override
  public long calculateMaxReturnSize() {
    long childrenMaxReturnSize = 0;
    for (Operator child : children) {
      childrenMaxReturnSize = Math.max(childrenMaxReturnSize, child.calculateMaxReturnSize());
    }

    return childrenMaxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long retainedSize = 0L;
    for (Operator child : children) {
      retainedSize += child.calculateRetainedSizeAfterCallingNext();
    }
    return retainedSize;
  }
}
