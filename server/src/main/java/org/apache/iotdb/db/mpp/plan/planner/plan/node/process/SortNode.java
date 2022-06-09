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
package org.apache.iotdb.db.mpp.plan.planner.plan.node.process;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.statement.component.OrderBy;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 * In general, the parameter in sortNode should be pushed down to the upstream operators. In our
 * optimized logical query plan, the sortNode should not appear.
 */
public class SortNode extends ProcessNode {

  private PlanNode child;

  private final OrderBy sortOrder;

  public SortNode(PlanNodeId id, OrderBy sortOrder) {
    super(id);
    this.sortOrder = sortOrder;
  }

  public SortNode(PlanNodeId id, PlanNode child, OrderBy sortOrder) {
    this(id, sortOrder);
    this.child = child;
  }

  public OrderBy getSortOrder() {
    return sortOrder;
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
  public int allowedChildCount() {
    return ONE_CHILD;
  }

  @Override
  public PlanNode clone() {
    return new SortNode(getPlanNodeId(), sortOrder);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return child.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSort(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SORT.serialize(byteBuffer);
    ReadWriteIOUtils.write(sortOrder.ordinal(), byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.SORT.serialize(stream);
    ReadWriteIOUtils.write(sortOrder.ordinal(), stream);
  }

  public static SortNode deserialize(ByteBuffer byteBuffer) {
    OrderBy orderBy = OrderBy.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new SortNode(planNodeId, orderBy);
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
    return child.equals(sortNode.child) && sortOrder == sortNode.sortOrder;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), child, sortOrder);
  }
}
