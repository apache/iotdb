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
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.base.Objects;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SortNode extends SingleChildProcessNode {

  private final OrderByParameter orderByParameter;

  // the columnNames which will be discarded in projection.
  private final List<String> outputColumnNames;

  public SortNode(PlanNodeId id, PlanNode child, OrderByParameter orderByParameter) {
    super(id, child);
    this.orderByParameter = orderByParameter;
    this.outputColumnNames = new ArrayList<>();
  }

  public SortNode(
      PlanNodeId id,
      PlanNode child,
      OrderByParameter orderByParameter,
      List<String> outputColumnNames) {
    super(id, child);
    this.orderByParameter = orderByParameter;
    this.outputColumnNames = outputColumnNames;
  }

  public SortNode(
      PlanNodeId id, OrderByParameter orderByParameter, List<String> outputColumnNames) {
    super(id);
    this.orderByParameter = orderByParameter;
    this.outputColumnNames = outputColumnNames;
  }

  public OrderByParameter getOrderByParameter() {
    return orderByParameter;
  }

  @Override
  public PlanNode clone() {
    return new SortNode(getPlanNodeId(), child, orderByParameter, outputColumnNames);
  }

  @Override
  public List<String> getOutputColumnNames() {
    if (this.outputColumnNames == null || this.outputColumnNames.isEmpty())
      return child.getOutputColumnNames();
    else return this.outputColumnNames;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSort(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SORT.serialize(byteBuffer);
    orderByParameter.serializeAttributes(byteBuffer);
    ReadWriteIOUtils.write(outputColumnNames.size(), byteBuffer);
    for (String column : outputColumnNames) {
      ReadWriteIOUtils.write(column, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.SORT.serialize(stream);
    orderByParameter.serializeAttributes(stream);
    ReadWriteIOUtils.write(outputColumnNames.size(), stream);
    for (String column : outputColumnNames) {
      ReadWriteIOUtils.write(column, stream);
    }
  }

  public static SortNode deserialize(ByteBuffer byteBuffer) {
    OrderByParameter orderByParameter = OrderByParameter.deserialize(byteBuffer);
    int columnSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> outputColumnNames = new ArrayList<>();
    while (columnSize > 0) {
      outputColumnNames.add(ReadWriteIOUtils.readString(byteBuffer));
      columnSize--;
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new SortNode(planNodeId, orderByParameter, outputColumnNames);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    SortNode sortNode = (SortNode) o;
    return Objects.equal(orderByParameter, sortNode.orderByParameter)
        && Objects.equal(outputColumnNames, sortNode.outputColumnNames);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), orderByParameter, outputColumnNames);
  }
}
