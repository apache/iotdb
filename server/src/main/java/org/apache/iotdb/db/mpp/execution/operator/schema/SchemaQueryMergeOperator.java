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
  private final boolean[] noMoreTsBlocks;

  private final List<Operator> children;

  public SchemaQueryMergeOperator(
      PlanNodeId planNodeId, OperatorContext operatorContext, List<Operator> children) {
    this.planNodeId = planNodeId;
    this.operatorContext = operatorContext;
    this.children = children;
    noMoreTsBlocks = new boolean[children.size()];
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    for (int i = 0; i < children.size(); i++) {
      if (!noMoreTsBlocks[i]) {
        TsBlock tsBlock = children.get(i).next();
        if (!children.get(i).hasNext()) {
          noMoreTsBlocks[i] = true;
        }
        return tsBlock;
      }
    }
    return null;
  }

  @Override
  public boolean hasNext() {
    for (int i = 0; i < children.size(); i++) {
      if (!noMoreTsBlocks[i] && children.get(i).hasNext()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public ListenableFuture<Void> isBlocked() {
    for (int i = 0; i < children.size(); i++) {
      if (!noMoreTsBlocks[i]) {
        ListenableFuture<Void> blocked = children.get(i).isBlocked();
        if (!blocked.isDone()) {
          return blocked;
        }
      }
    }
    return NOT_BLOCKED;
  }

  @Override
  public boolean isFinished() {
    return !hasNext();
  }
}
