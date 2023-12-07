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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe;

import org.apache.iotdb.db.consensus.statemachine.schemaregion.SchemaExecutionVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.ConstructSchemaBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.DeactivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.DeleteTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.DeleteLogicalViewNode;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * This class aims to mark the {@link PlanNode} of schema deletion to prevent forwarding pipe
 * deletions. The handling logic is defined only in {@link SchemaExecutionVisitor} to execute
 * deletion logic and mark it as received,
 *
 * <p>
 *
 * <p>Notes: The contents includes all the collected {@link PlanNode}s of schema deletion:
 *
 * <p>- Timeseries: {@link DeleteTimeSeriesNode}
 *
 * <p>- LogicalView: {@link DeleteLogicalViewNode}
 *
 * <p>- Template: {@link DeactivateTemplateNode}
 *
 * <p>Intermediate nodes like {@link ConstructSchemaBlackListNode} will not be included since they
 * will not be collected by pipe.
 */
public class PipeEnrichedDeleteSchemaNode extends PlanNode {

  private final PlanNode deleteSchemaNode;

  public PipeEnrichedDeleteSchemaNode(PlanNode schemaWriteNode) {
    super(schemaWriteNode.getPlanNodeId());
    this.deleteSchemaNode = schemaWriteNode;
  }

  public PlanNode getDeleteSchemaNode() {
    return deleteSchemaNode;
  }

  @Override
  public boolean isGeneratedByPipe() {
    return deleteSchemaNode.isGeneratedByPipe();
  }

  @Override
  public void markAsGeneratedByPipe() {
    deleteSchemaNode.markAsGeneratedByPipe();
  }

  @Override
  public PlanNodeId getPlanNodeId() {
    return deleteSchemaNode.getPlanNodeId();
  }

  @Override
  public void setPlanNodeId(PlanNodeId id) {
    deleteSchemaNode.setPlanNodeId(id);
  }

  @Override
  public List<PlanNode> getChildren() {
    return deleteSchemaNode.getChildren();
  }

  @Override
  public void addChild(PlanNode child) {
    deleteSchemaNode.addChild(child);
  }

  @Override
  public PlanNode clone() {
    return new PipeEnrichedDeleteSchemaNode(deleteSchemaNode.clone());
  }

  @Override
  public PlanNode createSubNode(int subNodeId, int startIndex, int endIndex) {
    return new PipeEnrichedDeleteSchemaNode(
        deleteSchemaNode.createSubNode(subNodeId, startIndex, endIndex));
  }

  @Override
  public PlanNode cloneWithChildren(List<PlanNode> children) {
    return new PipeEnrichedDeleteSchemaNode(deleteSchemaNode.cloneWithChildren(children));
  }

  @Override
  public int allowedChildCount() {
    return deleteSchemaNode.allowedChildCount();
  }

  @Override
  public List<String> getOutputColumnNames() {
    return deleteSchemaNode.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitPipeEnrichedDeleteSchemaNode(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.PIPE_ENRICHED_DELETE_SCHEMA.serialize(byteBuffer);
    deleteSchemaNode.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.PIPE_ENRICHED_DELETE_SCHEMA.serialize(stream);
    deleteSchemaNode.serialize(stream);
  }

  public static PlanNode deserialize(ByteBuffer buffer) {
    return new PipeEnrichedDeleteSchemaNode(PlanNodeType.deserialize(buffer));
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof PipeEnrichedDeleteSchemaNode
        && deleteSchemaNode.equals(((PipeEnrichedDeleteSchemaNode) o).deleteSchemaNode);
  }

  @Override
  public int hashCode() {
    return deleteSchemaNode.hashCode();
  }
}
