/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** TopNode is optimized for `order by time|expression limit N align by device` query. */
public class TopKNode extends MultiChildProcessNode {

  // when LIMIT value is less this value, can use TopKNode
  public static final int LIMIT_VALUE_USE_TOP_K = 1000000;

  private final int topValue;

  private final OrderByParameter mergeOrderParameter;

  private final List<String> outputColumns;

  public TopKNode(
      PlanNodeId id,
      int topValue,
      OrderByParameter mergeOrderParameter,
      List<String> outputColumns) {
    super(id);
    this.topValue = topValue;
    this.mergeOrderParameter = mergeOrderParameter;
    this.outputColumns = outputColumns;
  }

  public TopKNode(
      PlanNodeId id,
      int topValue,
      List<PlanNode> children,
      OrderByParameter mergeOrderParameter,
      List<String> outputColumns) {
    super(id, children);
    this.topValue = topValue;
    this.mergeOrderParameter = mergeOrderParameter;
    this.outputColumns = outputColumns;
  }

  public OrderByParameter getMergeOrderParameter() {
    return mergeOrderParameter;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.TOP_K;
  }

  @Override
  public PlanNode clone() {
    return new TopKNode(getPlanNodeId(), getTopValue(), getMergeOrderParameter(), outputColumns);
  }

  @Override
  public PlanNode createSubNode(int subNodeId, int startIndex, int endIndex) {
    return new TopKNode(
        new PlanNodeId(String.format("%s-%s", getPlanNodeId(), subNodeId)),
        getTopValue(),
        new ArrayList<>(children.subList(startIndex, endIndex)),
        getMergeOrderParameter(),
        outputColumns);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputColumns;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitTopK(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TOP_K.serialize(byteBuffer);
    ReadWriteIOUtils.write(topValue, byteBuffer);
    mergeOrderParameter.serializeAttributes(byteBuffer);
    ReadWriteIOUtils.write(outputColumns.size(), byteBuffer);
    for (String column : outputColumns) {
      ReadWriteIOUtils.write(column, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TOP_K.serialize(stream);
    ReadWriteIOUtils.write(topValue, stream);
    mergeOrderParameter.serializeAttributes(stream);
    ReadWriteIOUtils.write(outputColumns.size(), stream);
    for (String column : outputColumns) {
      ReadWriteIOUtils.write(column, stream);
    }
  }

  public static TopKNode deserialize(ByteBuffer byteBuffer) {
    int topValue = ReadWriteIOUtils.readInt(byteBuffer);
    OrderByParameter orderByParameter = OrderByParameter.deserialize(byteBuffer);
    int columnSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> outputColumns = new ArrayList<>();
    while (columnSize > 0) {
      outputColumns.add(ReadWriteIOUtils.readString(byteBuffer));
      columnSize--;
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new TopKNode(planNodeId, topValue, orderByParameter, outputColumns);
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
    TopKNode that = (TopKNode) o;
    return topValue == that.getTopValue()
        && Objects.equals(mergeOrderParameter, that.getMergeOrderParameter());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), topValue, mergeOrderParameter);
  }

  @Override
  public String toString() {
    return String.format("TopK-%s", this.getPlanNodeId());
  }

  public int getTopValue() {
    return this.topValue;
  }
}
