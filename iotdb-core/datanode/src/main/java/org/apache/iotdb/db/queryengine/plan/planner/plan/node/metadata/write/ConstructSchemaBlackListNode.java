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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write;

import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class ConstructSchemaBlackListNode extends PlanNode {

  private final PathPatternTree patternTree;

  public ConstructSchemaBlackListNode(PlanNodeId id, PathPatternTree patternTree) {
    super(id);
    this.patternTree = patternTree;
  }

  public PathPatternTree getPatternTree() {
    return patternTree;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.CONSTRUCT_SCHEMA_BLACK_LIST_NODE;
  }

  @Override
  public PlanNode clone() {
    return new ConstructSchemaBlackListNode(getPlanNodeId(), patternTree);
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitConstructSchemaBlackList(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.CONSTRUCT_SCHEMA_BLACK_LIST_NODE.serialize(byteBuffer);
    patternTree.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.CONSTRUCT_SCHEMA_BLACK_LIST_NODE.serialize(stream);
    patternTree.serialize(stream);
  }

  public static ConstructSchemaBlackListNode deserialize(ByteBuffer buffer) {
    PathPatternTree patternTree = PathPatternTree.deserialize(buffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new ConstructSchemaBlackListNode(planNodeId, patternTree);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ConstructSchemaBlackListNode that = (ConstructSchemaBlackListNode) o;
    return Objects.equals(patternTree, that.patternTree);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), patternTree);
  }
}
