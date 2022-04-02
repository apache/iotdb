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
package org.apache.iotdb.db.mpp.sql.planner.plan.node;

import org.apache.commons.lang.Validate;

import java.nio.ByteBuffer;
import java.util.List;

import static java.util.Objects.requireNonNull;

/** The base class of query executable operators, which is used to compose logical query plan. */
// TODO: consider how to restrict the children type for each type of ExecOperator
public abstract class PlanNode {
  protected static final int NO_CHILD_ALLOWED = 0;
  protected static final int ONE_CHILD = 1;
  protected static final int CHILD_COUNT_NO_LIMIT = -1;

  private final PlanNodeId id;

  protected PlanNode(PlanNodeId id) {
    requireNonNull(id, "id is null");
    this.id = id;
  }

  public PlanNodeId getId() {
    return id;
  }

  public abstract List<PlanNode> getChildren();

  public abstract void addChild(PlanNode child);

  public abstract PlanNode clone();

  public PlanNode cloneWithChildren(List<PlanNode> children) {
    Validate.isTrue(
        children == null
            || allowedChildCount() == CHILD_COUNT_NO_LIMIT
            || children.size() == allowedChildCount(),
        String.format(
            "Child count is not correct for PlanNode. Expected: %d, Value: %d",
            allowedChildCount(), getChildrenCount(children)));
    PlanNode node = clone();
    if (children != null) {
      children.forEach(node::addChild);
    }
    return node;
  }

  private int getChildrenCount(List<PlanNode> children) {
    return children == null ? 0 : children.size();
  }

  public abstract int allowedChildCount();

  public abstract List<String> getOutputColumnNames();

  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitPlan(this, context);
  }

  public abstract void serialize(ByteBuffer byteBuffer);
}
