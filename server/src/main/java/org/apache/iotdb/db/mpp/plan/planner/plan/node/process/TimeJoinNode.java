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
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This node is responsible for join two or more TsBlock. The join algorithm is like outer join by
 * timestamp column. It will join two or more TsBlock by Timestamp column. The output result of
 * TimeJoinOperator is sorted by timestamp
 */
public class TimeJoinNode extends MultiChildProcessNode {

  // This parameter indicates the order when executing multiway merge sort.
  private final Ordering mergeOrder;

  public TimeJoinNode(PlanNodeId id, Ordering mergeOrder) {
    super(id, new ArrayList<>());
    this.mergeOrder = mergeOrder;
  }

  public TimeJoinNode(PlanNodeId id, Ordering mergeOrder, List<PlanNode> children) {
    super(id, children);
    this.mergeOrder = mergeOrder;
  }

  public Ordering getMergeOrder() {
    return mergeOrder;
  }

  @Override
  public PlanNode clone() {
    return new TimeJoinNode(getPlanNodeId(), getMergeOrder());
  }

  @Override
  public PlanNode createSubNode(int subNodeId, int startIndex, int endIndex) {
    return new TimeJoinNode(
        new PlanNodeId(String.format("%s-%s", getPlanNodeId(), subNodeId)),
        getMergeOrder(),
        new ArrayList<>(children.subList(startIndex, endIndex)));
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
    return visitor.visitTimeJoin(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TIME_JOIN.serialize(byteBuffer);
    ReadWriteIOUtils.write(mergeOrder.ordinal(), byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TIME_JOIN.serialize(stream);
    ReadWriteIOUtils.write(mergeOrder.ordinal(), stream);
  }

  public static TimeJoinNode deserialize(ByteBuffer byteBuffer) {
    Ordering mergeOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new TimeJoinNode(planNodeId, mergeOrder);
  }

  @Override
  public String toString() {
    return "TimeJoinNode-" + this.getPlanNodeId();
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
    TimeJoinNode that = (TimeJoinNode) o;
    return mergeOrder == that.mergeOrder && children.equals(that.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), mergeOrder, children);
  }
}
