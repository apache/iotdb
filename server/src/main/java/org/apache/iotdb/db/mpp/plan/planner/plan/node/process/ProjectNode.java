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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ProjectNode extends SingleChildProcessNode {

  private final List<String> outputColumnNames;

  public ProjectNode(PlanNodeId id, List<String> outputColumnNames) {
    super(id);
    this.outputColumnNames = outputColumnNames;
  }

  public ProjectNode(PlanNodeId id, PlanNode child, List<String> outputColumnNames) {
    super(id, child);
    this.outputColumnNames = outputColumnNames;
  }

  @Override
  public PlanNode clone() {
    return new ProjectNode(getPlanNodeId(), getOutputColumnNames());
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputColumnNames;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitProject(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.PROJECT.serialize(byteBuffer);
    ReadWriteIOUtils.write(outputColumnNames.size(), byteBuffer);
    for (String outputColumnName : outputColumnNames) {
      ReadWriteIOUtils.write(outputColumnName, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.PROJECT.serialize(stream);
    ReadWriteIOUtils.write(outputColumnNames.size(), stream);
    for (String outputColumnName : outputColumnNames) {
      ReadWriteIOUtils.write(outputColumnName, stream);
    }
  }

  public static ProjectNode deserialize(ByteBuffer byteBuffer) {
    int outputColumnNamesSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> outputColumnNames = new ArrayList<>();
    while (outputColumnNamesSize > 0) {
      outputColumnNames.add(ReadWriteIOUtils.readString(byteBuffer));
      outputColumnNamesSize--;
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new ProjectNode(planNodeId, outputColumnNames);
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
    ProjectNode that = (ProjectNode) o;
    return outputColumnNames.equals(that.outputColumnNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), outputColumnNames);
  }
}
