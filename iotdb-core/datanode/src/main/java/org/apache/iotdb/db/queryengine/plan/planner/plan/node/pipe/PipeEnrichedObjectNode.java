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

import org.apache.iotdb.calc.utils.IObjectPath;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.consensus.statemachine.dataregion.DataExecutionVisitor;
import org.apache.iotdb.db.queryengine.execution.executor.RegionWriteExecutor;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ObjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.SearchNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.trigger.executor.TriggerFireVisitor;

import org.apache.tsfile.file.metadata.TableSchema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * This class aims to mark the {@link ObjectNode} to prevent forwarding pipe object writes. The
 * handling logic is defined in:
 *
 * <p>1.{@link RegionWriteExecutor}, to serialize and reach the target data region.
 *
 * <p>2.{@link TriggerFireVisitor}, to fire the trigger before writing to data region (if
 * applicable).
 *
 * <p>3.{@link DataExecutionVisitor}, to actually write data on data region and mark it as received
 * from pipe.
 */
public class PipeEnrichedObjectNode extends ObjectNode {

  private final ObjectNode objectNode;

  public PipeEnrichedObjectNode(final ObjectNode objectNode) {
    super(objectNode.getPlanNodeId());
    this.objectNode = objectNode;
  }

  public ObjectNode getObjectNode() {
    return objectNode;
  }

  @Override
  public boolean isGeneratedByPipe() {
    return objectNode.isGeneratedByPipe();
  }

  @Override
  public void markAsGeneratedByPipe() {
    objectNode.markAsGeneratedByPipe();
  }

  @Override
  public PlanNodeId getPlanNodeId() {
    return objectNode.getPlanNodeId();
  }

  @Override
  public void setPlanNodeId(final PlanNodeId id) {
    objectNode.setPlanNodeId(id);
  }

  @Override
  public long getSearchIndex() {
    return objectNode.getSearchIndex();
  }

  @Override
  public SearchNode setSearchIndex(final long searchIndex) {
    objectNode.setSearchIndex(searchIndex);
    return this;
  }

  @Override
  public boolean isEOF() {
    return objectNode.isEOF();
  }

  @Override
  public byte[] getContent() {
    return objectNode.getContent();
  }

  @Override
  public long getOffset() {
    return objectNode.getOffset();
  }

  @Override
  public void setFilePath(final IObjectPath filePath) {
    objectNode.setFilePath(filePath);
  }

  @Override
  public IObjectPath getFilePath() {
    return objectNode.getFilePath();
  }

  @Override
  public String getFilePathString() {
    return objectNode.getFilePathString();
  }

  @Override
  public void serializeToWAL(final IWALByteBufferView buffer) {
    objectNode.serializeToWAL(buffer);
  }

  @Override
  public int serializedSize() {
    return objectNode.serializedSize();
  }

  @Override
  public SearchNode merge(final List<SearchNode> searchNodes) {
    return objectNode.merge(searchNodes);
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return objectNode.getProgressIndex();
  }

  @Override
  public void setProgressIndex(final ProgressIndex progressIndex) {
    objectNode.setProgressIndex(progressIndex);
  }

  @Override
  public List<WritePlanNode> splitByPartition(final IAnalysis analysis) {
    return objectNode.splitByPartition(analysis);
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return objectNode.getRegionReplicaSet();
  }

  @Override
  public void setDataRegionReplicaSet(final TRegionReplicaSet dataRegionReplicaSet) {
    objectNode.setDataRegionReplicaSet(dataRegionReplicaSet);
  }

  @Override
  public List<PlanNode> getChildren() {
    return objectNode.getChildren();
  }

  @Override
  public void addChild(final PlanNode child) {
    objectNode.addChild(child);
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.PIPE_ENRICHED_OBJECT_FILE;
  }

  @Override
  public PlanNode clone() {
    final PlanNode cloned = objectNode.clone();
    return cloned == null ? null : new PipeEnrichedObjectNode((ObjectNode) cloned);
  }

  @Override
  public PlanNode createSubNode(final int subNodeId, final int startIndex, final int endIndex) {
    return new PipeEnrichedObjectNode(
        (ObjectNode) objectNode.createSubNode(subNodeId, startIndex, endIndex));
  }

  @Override
  public PlanNode cloneWithChildren(final List<PlanNode> children) {
    return new PipeEnrichedObjectNode((ObjectNode) objectNode.cloneWithChildren(children));
  }

  @Override
  public int allowedChildCount() {
    return objectNode.allowedChildCount();
  }

  @Override
  public List<String> getOutputColumnNames() {
    return objectNode.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(final IPlanVisitor<R, C> visitor, final C context) {
    return ((PlanVisitor<R, C>) visitor).visitPipeEnrichedObjectNode(this, context);
  }

  @Override
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    PlanNodeType.PIPE_ENRICHED_OBJECT_FILE.serialize(byteBuffer);
    objectNode.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(final DataOutputStream stream) throws IOException {
    PlanNodeType.PIPE_ENRICHED_OBJECT_FILE.serialize(stream);
    objectNode.serialize(stream);
  }

  public static PipeEnrichedObjectNode deserialize(final ByteBuffer buffer) {
    return new PipeEnrichedObjectNode((ObjectNode) PlanNodeType.deserialize(buffer));
  }

  @Override
  public ByteBuffer serialize() {
    return objectNode.serialize();
  }

  @Override
  public RelationalInsertRowNode genValueInsertRowNode(final TableSchema tableSchema)
      throws IllegalPathException {
    return objectNode.genValueInsertRowNode(tableSchema);
  }

  @Override
  public long getMemorySize() {
    return objectNode.getMemorySize();
  }

  @Override
  public void markAsGeneratedByRemoteConsensusLeader() {
    objectNode.markAsGeneratedByRemoteConsensusLeader();
  }

  @Override
  public boolean isGeneratedByRemoteConsensusLeader() {
    return objectNode.isGeneratedByRemoteConsensusLeader();
  }

  @Override
  public boolean equals(final Object o) {
    return o instanceof PipeEnrichedObjectNode
        && objectNode.equals(((PipeEnrichedObjectNode) o).objectNode);
  }

  @Override
  public int hashCode() {
    return objectNode.hashCode();
  }
}
