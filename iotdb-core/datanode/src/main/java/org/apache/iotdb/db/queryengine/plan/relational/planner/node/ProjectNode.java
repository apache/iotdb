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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class ProjectNode extends SingleChildProcessNode {
  private final Assignments assignments;

  public ProjectNode(PlanNodeId id, PlanNode child, Assignments assignments) {
    super(id, child);
    this.assignments = assignments;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitProject(this, context);
  }

  @Override
  public PlanNode clone() {
    return new ProjectNode(id, null, assignments);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  public Assignments getAssignments() {
    return assignments;
  }

  public boolean isIdentity() {
    return assignments.isIdentity();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_PROJECT_NODE.serialize(byteBuffer);
    ReadWriteIOUtils.write(assignments.getMap().size(), byteBuffer);
    for (Map.Entry<Symbol, Expression> entry : assignments.getMap().entrySet()) {
      Symbol.serialize(entry.getKey(), byteBuffer);
      Expression.serialize(entry.getValue(), byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_PROJECT_NODE.serialize(stream);
    ReadWriteIOUtils.write(assignments.getMap().size(), stream);
    for (Map.Entry<Symbol, Expression> entry : assignments.getMap().entrySet()) {
      Symbol.serialize(entry.getKey(), stream);
      Expression.serialize(entry.getValue(), stream);
    }
  }

  public static ProjectNode deserialize(ByteBuffer byteBuffer) {
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    Assignments.Builder assignments = Assignments.builder();
    while (size-- > 0) {
      assignments.put(Symbol.deserialize(byteBuffer), Expression.deserialize(byteBuffer));
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new ProjectNode(planNodeId, null, assignments.build());
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return assignments.getOutputs();
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new ProjectNode(id, Iterables.getOnlyElement(newChildren), assignments);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ProjectNode projectNode = (ProjectNode) o;
    return Objects.equal(assignments, projectNode.assignments);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), assignments);
  }

  @Override
  public String toString() {
    return "ProjectNode-" + this.getPlanNodeId();
  }
}
