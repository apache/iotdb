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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This node is responsible for merge two or more TsBlocks have been aligned. TsBlocks must have
 * same time column or no time column at all, and we can merge the value column directly without
 * compare.
 */
public class HorizontallyConcatNode extends MultiChildProcessNode {

  public HorizontallyConcatNode(PlanNodeId id) {
    super(id, new ArrayList<>());
  }

  public HorizontallyConcatNode(PlanNodeId id, List<PlanNode> children) {
    super(id, children);
  }

  @Override
  public PlanNode clone() {
    return new HorizontallyConcatNode(getPlanNodeId());
  }

  @Override
  public PlanNode createSubNode(int subNodeId, int startIndex, int endIndex) {
    return new HorizontallyConcatNode(
        new PlanNodeId(String.format("%s-%s", getPlanNodeId(), subNodeId)),
        new ArrayList<>(children.subList(startIndex, endIndex)));
  }

  @Override
  public List<String> getOutputColumnNames() {
    return children.stream()
        .map(PlanNode::getOutputColumnNames)
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitHorizontallyConcat(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.VERTICALLY_CONCAT.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.VERTICALLY_CONCAT.serialize(stream);
  }

  public static HorizontallyConcatNode deserialize(ByteBuffer byteBuffer) {
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new HorizontallyConcatNode(planNodeId);
  }

  @Override
  public String toString() {
    return "HorizontallyConcatNode-" + this.getPlanNodeId();
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
    HorizontallyConcatNode that = (HorizontallyConcatNode) o;
    return children.equals(that.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), children);
  }
}
