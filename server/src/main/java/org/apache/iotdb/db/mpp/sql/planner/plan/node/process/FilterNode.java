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
import org.apache.iotdb.db.mpp.common.filter.FilterDeserializeUtil;
import org.apache.iotdb.db.mpp.common.filter.QueryFilter;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** The FilterNode is responsible to filter the RowRecord from TsBlock. */
public class FilterNode extends ProcessNode {

  private PlanNode child;

  private final QueryFilter predicate;

  public FilterNode(PlanNodeId id, QueryFilter predicate) {
    super(id);
    this.predicate = predicate;
  }

  public FilterNode(PlanNodeId id, PlanNode child, QueryFilter predicate) {
    this(id, predicate);
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
    return new FilterNode(getId(), predicate);
  }

  @Override
  public int allowedChildCount() {
    return ONE_CHILD;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return child.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitFilter(this, context);
  }

  public static FilterNode deserialize(ByteBuffer byteBuffer) {
    QueryFilter predicate = FilterDeserializeUtil.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    FilterNode filterNode = new FilterNode(planNodeId, predicate);
    return filterNode;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.FILTER.serialize(byteBuffer);
    predicate.serialize(byteBuffer);
  }

  public QueryFilter getPredicate() {
    return predicate;
  }

  public PlanNode getChild() {
    return child;
  }

  @TestOnly
  public Pair<String, List<String>> print() {
    String title = String.format("[FilterNode (%s)]", this.getId());
    List<String> attributes = new ArrayList<>();
    attributes.add("QueryFilter: " + this.getPredicate());
    return new Pair<>(title, attributes);
  }
}
