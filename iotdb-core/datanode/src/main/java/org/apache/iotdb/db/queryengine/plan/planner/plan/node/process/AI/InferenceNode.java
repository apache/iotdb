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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AI;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.model.ModelInferenceDescriptor;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class InferenceNode extends SingleChildProcessNode {

  private final ModelInferenceDescriptor modelInferenceDescriptor;

  // the column order in select item which reflects the real input order
  private final List<String> targetColumnNames;

  public InferenceNode(
      PlanNodeId id,
      PlanNode child,
      ModelInferenceDescriptor modelInferenceDescriptor,
      List<String> targetColumnNames) {
    super(id, child);
    this.modelInferenceDescriptor = modelInferenceDescriptor;
    this.targetColumnNames = targetColumnNames;
  }

  public InferenceNode(
      PlanNodeId id,
      ModelInferenceDescriptor modelInferenceDescriptor,
      List<String> inputColumnNames) {
    super(id);
    this.modelInferenceDescriptor = modelInferenceDescriptor;
    this.targetColumnNames = inputColumnNames;
  }

  public ModelInferenceDescriptor getModelInferenceDescriptor() {
    return modelInferenceDescriptor;
  }

  public List<String> getInputColumnNames() {
    return targetColumnNames;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitInference(this, context);
  }

  @Override
  public PlanNode clone() {
    return new InferenceNode(getPlanNodeId(), child, modelInferenceDescriptor, targetColumnNames);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return modelInferenceDescriptor.getOutputColumnNames();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.INFERENCE.serialize(byteBuffer);
    modelInferenceDescriptor.serialize(byteBuffer);
    ReadWriteIOUtils.writeStringList(targetColumnNames, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.INFERENCE.serialize(stream);
    modelInferenceDescriptor.serialize(stream);
    ReadWriteIOUtils.writeStringList(targetColumnNames, stream);
  }

  public static InferenceNode deserialize(ByteBuffer buffer) {
    ModelInferenceDescriptor modelInferenceDescriptor =
        ModelInferenceDescriptor.deserialize(buffer);
    List<String> inputColumnNames = ReadWriteIOUtils.readStringList(buffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new InferenceNode(planNodeId, modelInferenceDescriptor, inputColumnNames);
  }

  @Override
  public String toString() {
    return "InferenceNode-" + this.getPlanNodeId();
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
    InferenceNode that = (InferenceNode) o;
    return modelInferenceDescriptor.equals(that.modelInferenceDescriptor)
        && targetColumnNames.equals(that.targetColumnNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), modelInferenceDescriptor, targetColumnNames);
  }
}
