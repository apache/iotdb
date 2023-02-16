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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.process;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

public abstract class SingleChildProcessNode extends ProcessNode {

  protected PlanNode child;

  protected SingleChildProcessNode(PlanNodeId id) {
    super(id);
  }

  protected SingleChildProcessNode(PlanNodeId id, PlanNode child) {
    super(id);
    this.child = child;
  }

  public PlanNode getChild() {
    return child;
  }

  public void setChild(PlanNode child) {
    this.child = child;
  }

  public void cleanChildren() {
    this.child = null;
  }

  @Override
  public List<PlanNode> getChildren() {
    if (this.child == null) {
      return ImmutableList.of();
    }
    return ImmutableList.of(child);
  }

  @Override
  public void addChild(PlanNode child) {
    this.child = child;
  }

  @Override
  public int allowedChildCount() {
    return ONE_CHILD;
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
    SingleChildProcessNode that = (SingleChildProcessNode) o;
    return Objects.equals(child, that.child);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), child);
  }
}
