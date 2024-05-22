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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class TopKNode extends MultiChildProcessNode {

  private final OrderingScheme orderingScheme;

  private final int count;

  private final List<Symbol> outputSymbols;

  private final boolean childrenDataInOrder;

  public TopKNode(
      PlanNodeId id,
      OrderingScheme scheme,
      int count,
      List<Symbol> outputSymbols,
      boolean childrenDataInOrder) {
    super(id);
    this.orderingScheme = scheme;
    this.count = count;
    this.outputSymbols = outputSymbols;
    this.childrenDataInOrder = childrenDataInOrder;
  }

  @Override
  public PlanNode clone() {
    return new TopKNode(getPlanNodeId(), orderingScheme, count, outputSymbols, childrenDataInOrder);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitTopK(this, context);
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {}

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {}

  public static TopKNode deserialize(ByteBuffer byteBuffer) {
    return null;
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return outputSymbols;
  }

  public OrderingScheme getOrderingScheme() {
    return orderingScheme;
  }

  public int getCount() {
    return count;
  }

  public boolean isChildrenDataInOrder() {
    return childrenDataInOrder;
  }
}
