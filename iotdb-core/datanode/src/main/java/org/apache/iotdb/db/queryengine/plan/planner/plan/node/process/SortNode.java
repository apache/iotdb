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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.process;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.OrderByParameter;

import com.google.common.base.Objects;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class SortNode extends SingleChildProcessNode {

  private final OrderByParameter orderByParameter;

  public SortNode(PlanNodeId id, PlanNode child, OrderByParameter orderByParameter) {
    super(id, child);
    this.orderByParameter = orderByParameter;
  }

  public SortNode(PlanNodeId id, OrderByParameter orderByParameter) {
    super(id);
    this.orderByParameter = orderByParameter;
  }

  public OrderByParameter getOrderByParameter() {
    return orderByParameter;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.SORT;
  }

  @Override
  public PlanNode clone() {
    return new SortNode(getPlanNodeId(), child, orderByParameter);
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
    orderByParameter.serializeAttributes(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.SORT.serialize(stream);
    orderByParameter.serializeAttributes(stream);
  }

  public static SortNode deserialize(ByteBuffer byteBuffer) {
    OrderByParameter orderByParameter = OrderByParameter.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new SortNode(planNodeId, orderByParameter);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    SortNode sortNode = (SortNode) o;
    return Objects.equal(orderByParameter, sortNode.orderByParameter);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), orderByParameter);
  }

  @Override
  public String toString() {
    return "SortNode-" + this.getPlanNodeId();
  }
}
