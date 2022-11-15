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
package org.apache.iotdb.db.mpp.plan.planner.plan.node.process;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.OrderByParameter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class MergeSortNode extends MultiChildProcessNode {

  private final OrderByParameter mergeOrderParameter;

  public MergeSortNode(PlanNodeId id, OrderByParameter mergeOrderParameter) {
    super(id);
    this.mergeOrderParameter = mergeOrderParameter;
  }

  public OrderByParameter getMergeOrderParameter() {
    return mergeOrderParameter;
  }

  @Override
  public PlanNode clone() {
    return null;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return children.stream()
        .map(PlanNode::getOutputColumnNames)
        .flatMap(List::stream)
        .distinct()
        .collect(Collectors.toList());
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitMergeSort(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.MERGE_SORT.serialize(byteBuffer);
    mergeOrderParameter.serializeAttributes(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.MERGE_SORT.serialize(stream);
    mergeOrderParameter.serializeAttributes(stream);
  }

  public MergeSortNode deserialize(ByteBuffer byteBuffer) {
    OrderByParameter orderByParameter = OrderByParameter.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new MergeSortNode(planNodeId, orderByParameter);
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
    MergeSortNode that = (MergeSortNode) o;
    return Objects.equals(mergeOrderParameter, that.getMergeOrderParameter());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), mergeOrderParameter);
  }

  @Override
  public String toString() {
    return "MergeSort-" + this.getPlanNodeId();
  }
}
