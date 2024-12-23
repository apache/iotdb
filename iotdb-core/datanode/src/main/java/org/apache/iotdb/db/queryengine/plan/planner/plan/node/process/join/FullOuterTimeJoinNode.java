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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This node is responsible for joining two or more TsBlock.
 *
 * <p>The join algorithm is <b>full outer join</b> on timestamp column —— take the <b>union</b> of
 * all child timestamps as the time column of the result. If a timestamp does not exist in one
 * child, it will be null in the corresponding row of the result. The output result is sorted by
 * timestamp.
 *
 * <p>e.g.
 *
 * <pre>
 *   [series1]  [series2]  [series1 join series2]
 *   time, s1   time, s2   time,   s1,   s2
 *      1,  1                 1,    1, null
 *      2,  2      2,  2      2,    2,    2
 *                 3,  3      3, null,    3
 * </pre>
 */
public class FullOuterTimeJoinNode extends MultiChildProcessNode {

  // This parameter indicates the order when executing multiway merge sort.
  private final Ordering mergeOrder;

  public FullOuterTimeJoinNode(PlanNodeId id, Ordering mergeOrder) {
    super(id, new ArrayList<>());
    this.mergeOrder = mergeOrder;
  }

  public FullOuterTimeJoinNode(PlanNodeId id, Ordering mergeOrder, List<PlanNode> children) {
    super(id, children);
    this.mergeOrder = mergeOrder;
  }

  public Ordering getMergeOrder() {
    return mergeOrder;
  }

  @Override
  public PlanNode clone() {
    return new FullOuterTimeJoinNode(getPlanNodeId(), getMergeOrder());
  }

  @Override
  public PlanNode createSubNode(int subNodeId, int startIndex, int endIndex) {
    return new FullOuterTimeJoinNode(
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
    return visitor.visitFullOuterTimeJoin(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.FULL_OUTER_TIME_JOIN.serialize(byteBuffer);
    ReadWriteIOUtils.write(mergeOrder.ordinal(), byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.FULL_OUTER_TIME_JOIN.serialize(stream);
    ReadWriteIOUtils.write(mergeOrder.ordinal(), stream);
  }

  public static FullOuterTimeJoinNode deserialize(ByteBuffer byteBuffer) {
    Ordering mergeOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new FullOuterTimeJoinNode(planNodeId, mergeOrder);
  }

  @Override
  public String toString() {
    return "FullOuterTimeJoinNode-" + this.getPlanNodeId();
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
    FullOuterTimeJoinNode that = (FullOuterTimeJoinNode) o;
    return mergeOrder == that.mergeOrder;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), mergeOrder);
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.FULL_OUTER_TIME_JOIN;
  }
}
