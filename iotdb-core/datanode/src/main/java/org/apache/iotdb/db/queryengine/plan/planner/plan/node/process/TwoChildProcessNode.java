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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.process;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

public abstract class TwoChildProcessNode extends ProcessNode {

  protected PlanNode leftChild;
  protected PlanNode rightChild;

  protected TwoChildProcessNode(PlanNodeId id) {
    super(id);
  }

  protected TwoChildProcessNode(PlanNodeId id, PlanNode leftChild, PlanNode rightChild) {
    super(id);
    this.leftChild = leftChild;
    this.rightChild = rightChild;
  }

  public PlanNode getLeftChild() {
    return leftChild;
  }

  public void setLeftChild(PlanNode leftChild) {
    this.leftChild = leftChild;
  }

  public PlanNode getRightChild() {
    return rightChild;
  }

  public void setRightChild(PlanNode rightChild) {
    this.rightChild = rightChild;
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of(leftChild, rightChild);
  }

  @Override
  public void addChild(PlanNode child) {
    if (leftChild == null) {
      leftChild = child;
    } else if (rightChild == null) {
      rightChild = child;
    } else {
      throw new UnsupportedOperationException("This node doesn't support more than two children");
    }
  }

  @Override
  public int allowedChildCount() {
    return TWO_CHILDREN;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TwoChildProcessNode that = (TwoChildProcessNode) o;
    return Objects.equals(leftChild, that.leftChild) && Objects.equals(rightChild, that.rightChild);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), leftChild, rightChild);
  }
}
