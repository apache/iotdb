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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import com.google.common.collect.ListMultimap;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class ExceptNode extends SetOperationNode {

  private final boolean distinct;

  public ExceptNode(
      PlanNodeId id,
      List<PlanNode> children,
      ListMultimap<Symbol, Symbol> outputToInputs,
      List<Symbol> outputs,
      boolean distinct) {

    super(id, children, outputToInputs, outputs);
    this.distinct = distinct;
  }

  private ExceptNode(
      PlanNodeId id,
      ListMultimap<Symbol, Symbol> outputToInputs,
      List<Symbol> outputs,
      boolean distinct) {
    super(id, outputToInputs, outputs);
    this.distinct = distinct;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitExcept(this, context);
  }

  public boolean isDistinct() {
    return distinct;
  }

  @Override
  public PlanNode clone() {
    return new ExceptNode(getPlanNodeId(), getSymbolMapping(), getOutputSymbols(), distinct);
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    throw new UnsupportedOperationException(
        "ExceptNode should never be serialized in current version");
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    throw new UnsupportedOperationException(
        "ExceptNode should never be serialized in current version");
  }

  public static ExceptNode deserialize(ByteBuffer byteBuffer) {
    throw new UnsupportedOperationException(
        "ExceptNode should never be deserialized in current version");
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new ExceptNode(
        getPlanNodeId(), newChildren, getSymbolMapping(), getOutputSymbols(), isDistinct());
  }
}
