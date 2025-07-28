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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AssignUniqueId extends SingleChildProcessNode {
  private final Symbol idColumn;

  public AssignUniqueId(PlanNodeId id, PlanNode child, Symbol idColumn) {
    super(id);
    this.child = child;
    this.idColumn = requireNonNull(idColumn, "idColumn is null");
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitAssignUniqueId(this, context);
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return ImmutableList.<Symbol>builder().addAll(child.getOutputSymbols()).add(idColumn).build();
  }

  @Override
  public PlanNode clone() {
    // clone without child
    return new AssignUniqueId(id, null, idColumn);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    checkArgument(newChildren.size() == 1, "expected newChildren to contain 1 node");
    return new AssignUniqueId(getPlanNodeId(), Iterables.getOnlyElement(newChildren), idColumn);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_ASSIGN_UNIQUE_ID.serialize(byteBuffer);
    Symbol.serialize(idColumn, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_ASSIGN_UNIQUE_ID.serialize(stream);
    Symbol.serialize(idColumn, stream);
  }

  public static AssignUniqueId deserialize(ByteBuffer byteBuffer) {
    Symbol idColumn = Symbol.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new AssignUniqueId(planNodeId, null, idColumn);
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new UnsupportedOperationException();
  }

  public Symbol getIdColumn() {
    return idColumn;
  }
}
