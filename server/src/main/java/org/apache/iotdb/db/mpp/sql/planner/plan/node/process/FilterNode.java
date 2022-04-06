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
import org.apache.iotdb.db.mpp.common.filter.BasicFunctionFilter;
import org.apache.iotdb.db.mpp.common.filter.InFilter;
import org.apache.iotdb.db.mpp.common.filter.LikeFilter;
import org.apache.iotdb.db.mpp.common.filter.QueryFilter;
import org.apache.iotdb.db.mpp.common.filter.RegexpFilter;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

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
    byte filterType = ReadWriteIOUtils.readByte(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    QueryFilter predicate = null;
    if (filterType == 0) {
      predicate = BasicFunctionFilter.deserialize(byteBuffer);
    } else if (filterType == 1) {
      predicate = LikeFilter.deserialize(byteBuffer);
    } else if (filterType == 2) {
      predicate = InFilter.deserialize(byteBuffer);
    } else if (filterType == 3) {
      predicate = RegexpFilter.deserialize(byteBuffer);
    } else {
      predicate = QueryFilter.deserialize(byteBuffer);
    }
    FilterNode filterNode = new FilterNode(planNodeId, predicate);
    return filterNode;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.FILTER.serialize(byteBuffer);
    if (predicate instanceof BasicFunctionFilter) {
      ReadWriteIOUtils.write((byte)0, byteBuffer);
    } else if (predicate instanceof LikeFilter) {
      ReadWriteIOUtils.write((byte)1, byteBuffer);
    } else if (predicate instanceof InFilter) {
      ReadWriteIOUtils.write((byte)2, byteBuffer);
    } else if (predicate instanceof RegexpFilter) {
      ReadWriteIOUtils.write((byte)3, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte)4, byteBuffer);  // QueryFilter
    }
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
