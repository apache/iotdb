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
import org.apache.iotdb.db.mpp.sql.planner.plan.IOutputPlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.ColumnHeader;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * In general, the parameter in sortNode should be pushed down to the upstream operators. In our
 * optimized logical query plan, the sortNode should not appear.
 */
public class SortNode extends ProcessNode implements IOutputPlanNode {

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
    return new SortNode(getPlanNodeId(), orderBy, sortOrder);
  }

  @Override
  public int allowedChildCount() {
    return ONE_CHILD;
  }

  @Override
  public List<ColumnHeader> getOutputColumnHeaders() {
    return ((IOutputPlanNode) child).getOutputColumnHeaders();
  }

  @Override
  public List<String> getOutputColumnNames() {
    return ((IOutputPlanNode) child).getOutputColumnNames();
  }

  @Override
  public List<TSDataType> getOutputColumnTypes() {
    return ((IOutputPlanNode) child).getOutputColumnTypes();
  }

  public OrderBy getSortOrder() {
    return sortOrder;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSort(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SORT.serialize(byteBuffer);
    ReadWriteIOUtils.write(orderBy.size(), byteBuffer);
    for (int i = 0; i < orderBy.size(); i++) {
      ReadWriteIOUtils.write(orderBy.get(i), byteBuffer);
    }
    ReadWriteIOUtils.write(sortOrder.ordinal(), byteBuffer);
  }

  public static SortNode deserialize(ByteBuffer byteBuffer) {
    List<String> orderBys = new ArrayList<>();
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; i++) {
      orderBys.add(ReadWriteIOUtils.readString(byteBuffer));
    }
    OrderBy orderBy = OrderBy.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new SortNode(planNodeId, orderBys, orderBy);
  }

  @TestOnly
  public Pair<String, List<String>> print() {
    String title = String.format("[SortNode (%s)]", this.getPlanNodeId());
    List<String> attributes = new ArrayList<>();
    attributes.add("SortOrder: " + (this.getSortOrder() == null ? "null" : this.getSortOrder()));
    return new Pair<>(title, attributes);
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
    SortNode sortNode = (SortNode) o;
    return Objects.equals(child, sortNode.child)
        && Objects.equals(orderBy, sortNode.orderBy)
        && sortOrder == sortNode.sortOrder;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), child, orderBy, sortOrder);
  }
}
