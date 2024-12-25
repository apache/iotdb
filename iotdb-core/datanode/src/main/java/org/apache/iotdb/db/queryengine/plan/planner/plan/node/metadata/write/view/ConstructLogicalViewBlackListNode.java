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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view;

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

public class ConstructLogicalViewBlackListNode extends PlanNode {

  private final PathPatternTree patternTree;

  public ConstructLogicalViewBlackListNode(PlanNodeId id, PathPatternTree patternTree) {
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
    return PlanNodeType.CONSTRUCT_LOGICAL_VIEW_BLACK_LIST;
  }

  @Override
  public PlanNode clone() {
    return new ConstructLogicalViewBlackListNode(getPlanNodeId(), patternTree);
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
    return visitor.visitConstructLogicalViewBlackList(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.CONSTRUCT_LOGICAL_VIEW_BLACK_LIST.serialize(byteBuffer);
    patternTree.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.CONSTRUCT_LOGICAL_VIEW_BLACK_LIST.serialize(stream);
    patternTree.serialize(stream);
  }

  public static ConstructLogicalViewBlackListNode deserialize(ByteBuffer buffer) {
    PathPatternTree patternTree = PathPatternTree.deserialize(buffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new ConstructLogicalViewBlackListNode(planNodeId, patternTree);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ConstructLogicalViewBlackListNode that = (ConstructLogicalViewBlackListNode) o;
    return Objects.equals(patternTree, that.patternTree);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), patternTree);
  }
}
