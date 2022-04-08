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
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullPolicy;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * TimeJoinOperator is responsible for join two or more TsBlock. The join algorithm is like outer
 * join by timestamp column. It will join two or more TsBlock by Timestamp column. The output result
 * of TimeJoinOperator is sorted by timestamp
 */
// TODO: define the TimeJoinMergeNode for distributed plan
public class TimeJoinNode extends ProcessNode {

  // This parameter indicates the order when executing multiway merge sort.
  private OrderBy mergeOrder;

  // The policy to decide whether a row should be discarded
  // The without policy is able to be push down to the TimeJoinOperator because we can know whether
  // a row contains
  // null or not.
  private FilterNullPolicy filterNullPolicy;

  private List<PlanNode> children;

  // output columns' data type
  private List<TSDataType> types;

  public TimeJoinNode(PlanNodeId id, OrderBy mergeOrder, FilterNullPolicy filterNullPolicy) {
    super(id);
    this.mergeOrder = mergeOrder;
    this.filterNullPolicy = filterNullPolicy;
    this.children = new ArrayList<>();
  }

  public TimeJoinNode(
      PlanNodeId id,
      OrderBy mergeOrder,
      FilterNullPolicy filterNullPolicy,
      List<PlanNode> children) {
    this(id, mergeOrder, filterNullPolicy);
    this.children = children;
  }

  @Override
  public List<PlanNode> getChildren() {
    return children;
  }

  @Override
  public PlanNode clone() {
    return new TimeJoinNode(getId(), this.mergeOrder, this.filterNullPolicy);
  }

  @Override
  public int allowedChildCount() {
    return CHILD_COUNT_NO_LIMIT;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return children.stream()
        .flatMap(child -> child.getOutputColumnNames().stream())
        .collect(Collectors.toList());
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitTimeJoin(this, context);
  }

  public static TimeJoinNode deserialize(ByteBuffer byteBuffer) {
    return null;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {}

  @Override
  public void addChild(PlanNode child) {
    this.children.add(child);
  }

  public void setChildren(List<PlanNode> children) {
    this.children = children;
  }

  public OrderBy getMergeOrder() {
    return mergeOrder;
  }

  public FilterNullPolicy getFilterNullPolicy() {
    return filterNullPolicy;
  }

  public void setMergeOrder(OrderBy mergeOrder) {
    this.mergeOrder = mergeOrder;
  }

  public void setWithoutPolicy(FilterNullPolicy filterNullPolicy) {
    this.filterNullPolicy = filterNullPolicy;
  }

  public String toString() {
    return "TimeJoinNode-" + this.getId();
  }

  public List<TSDataType> getTypes() {
    return types;
  }

  @TestOnly
  public Pair<String, List<String>> print() {
    String title = String.format("[TimeJoinNode (%s)]", this.getId());
    List<String> attributes = new ArrayList<>();
    attributes.add("MergeOrder: " + (this.getMergeOrder() == null ? "null" : this.getMergeOrder()));
    attributes.add(
        "FilterNullPolicy: "
            + (this.getFilterNullPolicy() == null ? "null" : this.getFilterNullPolicy()));
    return new Pair<>(title, attributes);
  }
}
