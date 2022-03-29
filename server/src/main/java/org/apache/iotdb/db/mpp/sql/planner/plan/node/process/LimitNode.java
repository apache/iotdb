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
package org.apache.iotdb.db.mpp.sql.planner.plan.node.process;

import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeIdAllocator;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.List;

/** LimitNode is used to select top n result. It uses the default order of upstream nodes */
public class LimitNode extends ProcessNode {

  // The limit count
  private int limit;
  private PlanNode child;

  public LimitNode(PlanNodeId id, int limit) {
    super(id);
    this.limit = limit;
  }

  public LimitNode(PlanNodeId id, int limit, PlanNode child) {
    this(id, limit);
    this.child = child;
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of(child);
  }

  @Override
  public void addChildren(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new LimitNode(PlanNodeIdAllocator.generateId(), this.limit);
  }

  @Override
  public PlanNode cloneWithChildren(List<PlanNode> children) {
    LimitNode root = (LimitNode) this.clone();
    root.setChild(children.get(0));
    return root;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return child.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitLimit(this, context);
  }

  public static LimitNode deserialize(ByteBuffer byteBuffer) {
    return null;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {}

  public int getLimit() {
    return limit;
  }

  public PlanNode getChild() {
    return child;
  }

  public void setChild(PlanNode child) {
    this.child = child;
  }

  public String toString() {
    return "LimitNode-" + this.getId();
  }
}
