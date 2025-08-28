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
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.db.consensus.statemachine.dataregion.DataExecutionVisitor;
import org.apache.iotdb.db.queryengine.execution.executor.RegionWriteExecutor;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.AbstractDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.SearchNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class aims to mark the {@link AbstractDeleteDataNode} to enable selectively forwarding pipe
 * deletion. The handling logic is defined in:
 *
 * <p>1.{@link RegionWriteExecutor}, to serialize and reach the target data region.
 *
 * <p>2.{@link DataExecutionVisitor}, to actually write data on data region and mark it as received
 * from pipe.
 */
public class PipeEnrichedDeleteDataNode extends AbstractDeleteDataNode {

  private final AbstractDeleteDataNode deleteDataNode;

  public PipeEnrichedDeleteDataNode(final AbstractDeleteDataNode deleteDataNode) {
    super(deleteDataNode.getPlanNodeId());
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
  public void setPlanNodeId(final PlanNodeId id) {
    deleteDataNode.setPlanNodeId(id);
  }

  @Override
  public ByteBuffer serializeToDAL() {
    return deleteDataNode.serializeToDAL();
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return deleteDataNode.getProgressIndex();
  }

  @Override
  public void setProgressIndex(ProgressIndex progressIndex) {
    deleteDataNode.setProgressIndex(progressIndex);
  }

  @Override
  public List<PlanNode> getChildren() {
    return deleteDataNode.getChildren();
  }

  @Override
  public void addChild(final PlanNode child) {
    deleteDataNode.addChild(child);
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.PIPE_ENRICHED_DELETE_DATA;
  }

  @Override
  public PlanNode clone() {
    return new PipeEnrichedDeleteDataNode((AbstractDeleteDataNode) deleteDataNode.clone());
  }

  @Override
  public PlanNode createSubNode(final int subNodeId, final int startIndex, final int endIndex) {
    return new PipeEnrichedDeleteDataNode(
        (AbstractDeleteDataNode) deleteDataNode.createSubNode(subNodeId, startIndex, endIndex));
  }

  @Override
  public PlanNode cloneWithChildren(final List<PlanNode> children) {
    return new PipeEnrichedDeleteDataNode(
        (AbstractDeleteDataNode) deleteDataNode.cloneWithChildren(children));
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
  public <R, C> R accept(final PlanVisitor<R, C> visitor, final C context) {
    return visitor.visitPipeEnrichedDeleteDataNode(this, context);
  }

  @Override
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    PlanNodeType.PIPE_ENRICHED_DELETE_DATA.serialize(byteBuffer);
    deleteDataNode.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(final DataOutputStream stream) throws IOException {
    PlanNodeType.PIPE_ENRICHED_DELETE_DATA.serialize(stream);
    deleteDataNode.serialize(stream);
  }

  public static PipeEnrichedDeleteDataNode deserialize(final ByteBuffer buffer) {
    return new PipeEnrichedDeleteDataNode(
        (AbstractDeleteDataNode) PlanNodeType.deserialize(buffer));
  }

  @Override
  public boolean equals(final Object o) {
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
  public List<WritePlanNode> splitByPartition(final IAnalysis analysis) {
    return deleteDataNode.splitByPartition(analysis).stream()
        .map(
            plan ->
                plan instanceof PipeEnrichedDeleteDataNode
                    ? plan
                    : new PipeEnrichedDeleteDataNode((AbstractDeleteDataNode) plan))
        .collect(Collectors.toList());
  }

  @Override
  public void serializeToWAL(final IWALByteBufferView buffer) {
    deleteDataNode.serializeToWAL(buffer);
  }

  @Override
  public int serializedSize() {
    return deleteDataNode.serializedSize();
  }

  @Override
  public SearchNode merge(List<SearchNode> searchNodes) {
    List<SearchNode> unrichedDeleteDataNodes =
        searchNodes.stream()
            .map(
                searchNode ->
                    (SearchNode) ((PipeEnrichedDeleteDataNode) searchNode).getDeleteDataNode())
            .collect(Collectors.toList());
    return new PipeEnrichedDeleteDataNode(
        (AbstractDeleteDataNode) deleteDataNode.merge(unrichedDeleteDataNodes));
  }
}
