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
package org.apache.iotdb.db.mpp.sql.planner.plan.node.process;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * In general, the parameter in sortNode should be pushed down to the upstream operators. In our
 * optimized logical query plan, the sortNode should not appear.
 */
public class SortNode extends ProcessNode {

  private PlanNode child;

  private final List<String> orderBy;

  private OrderBy sortOrder;

  public SortNode(PlanNodeId id, List<String> orderBy, OrderBy sortOrder) {
    super(id);
    this.orderBy = orderBy;
    this.sortOrder = sortOrder;
  }

  public SortNode(PlanNodeId id, PlanNode child, List<String> orderBy, OrderBy sortOrder) {
    this(id, orderBy, sortOrder);
    this.child = child;
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of(child);
  }

  @Override
  public void addChild(PlanNode child) {
    this.child = child;
  }

  @Override
  public PlanNode clone() {
    return new SortNode(getId(), orderBy, sortOrder);
  }

  @Override
  public int allowedChildCount() {
    return ONE_CHILD;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return child.getOutputColumnNames();
  }

  public OrderBy getSortOrder() {
    return sortOrder;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSort(this, context);
  }

  public static SortNode deserialize(ByteBuffer byteBuffer) {
    return null;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {}

  @TestOnly
  public Pair<String, List<String>> print() {
    String title = String.format("[SortNode (%s)]", this.getId());
    List<String> attributes = new ArrayList<>();
    attributes.add("SortOrder: " + (this.getSortOrder() == null ? "null" : this.getSortOrder()));
    return new Pair<>(title, attributes);
  }

  public List<String> getBoxString() {
    List<String> ret = new ArrayList<>();
    ret.add(String.format("SortNode-%s", getId().getId()));
    ret.add(String.format("Order: %s", sortOrder));
    return ret;
  }

  public String toString() {
    return String.format("SortNode-%s", getId().getId());
  }
}
