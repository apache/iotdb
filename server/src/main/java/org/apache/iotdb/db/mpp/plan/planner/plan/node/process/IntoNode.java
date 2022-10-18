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

import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.IntoPathDescriptor;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class IntoNode extends SingleChildProcessNode {

  private final IntoPathDescriptor intoPathDescriptor;

  public IntoNode(PlanNodeId id, IntoPathDescriptor intoPathDescriptor) {
    super(id);
    this.intoPathDescriptor = intoPathDescriptor;
  }

  public IntoNode(PlanNodeId id, PlanNode child, IntoPathDescriptor intoPathDescriptor) {
    super(id, child);
    this.intoPathDescriptor = intoPathDescriptor;
  }

  public IntoPathDescriptor getIntoPathDescriptor() {
    return intoPathDescriptor;
  }

  @Override
  public PlanNode clone() {
    return new IntoNode(getPlanNodeId(), this.intoPathDescriptor);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return ColumnHeaderConstant.selectIntoColumnHeaders.stream()
        .map(ColumnHeader::getColumnName)
        .collect(Collectors.toList());
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.INTO.serialize(byteBuffer);
    this.intoPathDescriptor.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.INTO.serialize(stream);
    this.intoPathDescriptor.serialize(stream);
  }

  public static IntoNode deserialize(ByteBuffer byteBuffer) {
    IntoPathDescriptor intoPathDescriptor = IntoPathDescriptor.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new IntoNode(planNodeId, intoPathDescriptor);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitInto(this, context);
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
    IntoNode intoNode = (IntoNode) o;
    return intoPathDescriptor.equals(intoNode.intoPathDescriptor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), intoPathDescriptor);
  }

  @Override
  public String toString() {
    return "IntoNode-" + getPlanNodeId();
  }
}
