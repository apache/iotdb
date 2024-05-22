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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.relational.sql.tree.Expression;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class FilterNode extends SingleChildProcessNode {
  private Expression predicate;

  public FilterNode(PlanNodeId id, PlanNode child, Expression predicate) {
    super(id, child);
    this.predicate = predicate;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitFilter(this, context);
  }

  @Override
  public PlanNode clone() {
    return new FilterNode(id, child, predicate);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {}

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {}

  public static FilterNode deserialize(ByteBuffer byteBuffer) {
    return null;
  }

  public Expression getPredicate() {
    return predicate;
  }

  public void setPredicate(Expression predicate) {
    this.predicate = predicate;
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return child.getOutputSymbols();
  }
}
