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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.consensus.statemachine.dataregion.DataExecutionVisitor;
import org.apache.iotdb.db.queryengine.execution.executor.RegionWriteExecutor;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class aims to mark the {@link DeleteDataNode} to enable selectively forwarding pipe
 * deletion. The handling logic is defined in:
 *
 * <p>1.{@link RegionWriteExecutor}, to serialize and reach the target data region.
 *
 * <p>2.{@link DataExecutionVisitor}, to actually write data on data region and mark it as received
 * from pipe.
 *
 * <p>TODO: support relational deleteNode
 */
public class PipeEnrichedDeleteDataNode extends DeleteDataNode {

  private final DeleteDataNode deleteDataNode;

  public PipeEnrichedDeleteDataNode(DeleteDataNode deleteDataNode) {
    super(
        deleteDataNode.getPlanNodeId(),
        deleteDataNode.getPathList(),
        deleteDataNode.getDeleteStartTime(),
        deleteDataNode.getDeleteEndTime());
    this.deleteDataNode = deleteDataNode;
  }

  public PlanNode getDeleteDataNode() {
    return deleteDataNode;
  }

  @Override
  public boolean isGeneratedByPipe() {
    return deleteDataNode.isGeneratedByPipe();
  }

  @Override
  public void markAsGeneratedByPipe() {
    deleteDataNode.markAsGeneratedByPipe();
  }

  @Override
  public PlanNodeId getPlanNodeId() {
    return deleteDataNode.getPlanNodeId();
  }

  @Override
  public void setPlanNodeId(PlanNodeId id) {
    deleteDataNode.setPlanNodeId(id);
  }

  @Override
  public List<PlanNode> getChildren() {
    return deleteDataNode.getChildren();
  }

  @Override
  public void addChild(PlanNode child) {
    deleteDataNode.addChild(child);
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.PIPE_ENRICHED_DELETE_DATA;
  }

  @Override
  public DeleteDataNode clone() {
    return new PipeEnrichedDeleteDataNode((DeleteDataNode) deleteDataNode.clone());
  }

  @Override
  public DeleteDataNode createSubNode(int subNodeId, int startIndex, int endIndex) {
    return new PipeEnrichedDeleteDataNode(
        (DeleteDataNode) deleteDataNode.createSubNode(subNodeId, startIndex, endIndex));
  }

  @Override
  public PlanNode cloneWithChildren(List<PlanNode> children) {
    return new PipeEnrichedDeleteDataNode(
        (DeleteDataNode) deleteDataNode.cloneWithChildren(children));
  }

  @Override
  public int allowedChildCount() {
    return deleteDataNode.allowedChildCount();
  }

  @Override
  public List<String> getOutputColumnNames() {
    return deleteDataNode.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitPipeEnrichedDeleteDataNode(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.PIPE_ENRICHED_DELETE_DATA.serialize(byteBuffer);
    deleteDataNode.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.PIPE_ENRICHED_DELETE_DATA.serialize(stream);
    deleteDataNode.serialize(stream);
  }

  public static PipeEnrichedDeleteDataNode deserialize(ByteBuffer buffer) {
    return new PipeEnrichedDeleteDataNode((DeleteDataNode) PlanNodeType.deserialize(buffer));
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof PipeEnrichedDeleteDataNode
        && deleteDataNode.equals(((PipeEnrichedDeleteDataNode) o).deleteDataNode);
  }

  @Override
  public int hashCode() {
    return deleteDataNode.hashCode();
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return deleteDataNode.getRegionReplicaSet();
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    return deleteDataNode.splitByPartition(analysis).stream()
        .map(
            plan ->
                plan instanceof PipeEnrichedDeleteDataNode
                    ? plan
                    : new PipeEnrichedDeleteDataNode((DeleteDataNode) plan))
        .collect(Collectors.toList());
  }
}
