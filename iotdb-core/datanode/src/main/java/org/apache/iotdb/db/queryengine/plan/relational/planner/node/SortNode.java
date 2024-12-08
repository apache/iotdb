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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class SortNode extends SingleChildProcessNode {
  protected final OrderingScheme orderingScheme;
  protected final boolean partial;
  // when order by all ids and time, this sort node can be eliminated in a way, no need serialize
  // initialize in construction method of StreamSortNode
  protected boolean orderByAllIdsAndTime;

  public SortNode(
      PlanNodeId id,
      PlanNode child,
      OrderingScheme scheme,
      boolean partial,
      boolean orderByAllIdsAndTime) {
    super(id, child);
    this.orderingScheme = scheme;
    this.partial = partial;
    this.orderByAllIdsAndTime = orderByAllIdsAndTime;
  }

  @Override
  public PlanNode clone() {
    return new SortNode(id, null, orderingScheme, partial, orderByAllIdsAndTime);
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSort(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_SORT_NODE.serialize(byteBuffer);
    orderingScheme.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_SORT_NODE.serialize(stream);
    orderingScheme.serialize(stream);
  }

  public static SortNode deserialize(ByteBuffer byteBuffer) {
    OrderingScheme orderingScheme = OrderingScheme.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new SortNode(planNodeId, null, orderingScheme, false, false);
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return child.getOutputSymbols();
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new SortNode(
        id, Iterables.getOnlyElement(newChildren), orderingScheme, partial, orderByAllIdsAndTime);
  }

  public OrderingScheme getOrderingScheme() {
    return orderingScheme;
  }

  public boolean isPartial() {
    return this.partial;
  }

  public boolean isOrderByAllIdsAndTime() {
    return orderByAllIdsAndTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    SortNode sortNode = (SortNode) o;
    return Objects.equal(orderingScheme, sortNode.orderingScheme) && partial == sortNode.partial;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), orderingScheme, partial);
  }

  @Override
  public String toString() {
    return "SortNode-" + this.getPlanNodeId();
  }
}
