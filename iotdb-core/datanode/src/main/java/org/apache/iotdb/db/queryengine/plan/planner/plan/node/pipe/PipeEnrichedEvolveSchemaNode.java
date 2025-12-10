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
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.EvolveSchemaNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.SearchNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

public class PipeEnrichedEvolveSchemaNode extends EvolveSchemaNode {

  private final EvolveSchemaNode evolveSchemaNode;

  public PipeEnrichedEvolveSchemaNode(final EvolveSchemaNode evolveSchemaNode) {
    super(evolveSchemaNode.getPlanNodeId(), evolveSchemaNode.getSchemaEvolutions());
    this.evolveSchemaNode = evolveSchemaNode;
  }

  public PlanNode EvolveSchemaNode() {
    return evolveSchemaNode;
  }

  @Override
  public boolean isGeneratedByPipe() {
    return evolveSchemaNode.isGeneratedByPipe();
  }

  @Override
  public void markAsGeneratedByPipe() {
    evolveSchemaNode.markAsGeneratedByPipe();
  }

  @Override
  public PlanNodeId getPlanNodeId() {
    return evolveSchemaNode.getPlanNodeId();
  }

  @Override
  public void setPlanNodeId(final PlanNodeId id) {
    evolveSchemaNode.setPlanNodeId(id);
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return evolveSchemaNode.getProgressIndex();
  }

  @Override
  public void setProgressIndex(ProgressIndex progressIndex) {
    evolveSchemaNode.setProgressIndex(progressIndex);
  }

  @Override
  public List<PlanNode> getChildren() {
    return evolveSchemaNode.getChildren();
  }

  @Override
  public void addChild(final PlanNode child) {
    evolveSchemaNode.addChild(child);
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.PIPE_ENRICHED_EVOLVE_SCHEMA;
  }

  @Override
  public PlanNode clone() {
    return new PipeEnrichedEvolveSchemaNode((EvolveSchemaNode) evolveSchemaNode.clone());
  }

  @Override
  public PlanNode createSubNode(final int subNodeId, final int startIndex, final int endIndex) {
    return new PipeEnrichedEvolveSchemaNode(
        (EvolveSchemaNode) evolveSchemaNode.createSubNode(subNodeId, startIndex, endIndex));
  }

  @Override
  public PlanNode cloneWithChildren(final List<PlanNode> children) {
    return new PipeEnrichedEvolveSchemaNode(
        (EvolveSchemaNode) evolveSchemaNode.cloneWithChildren(children));
  }

  @Override
  public int allowedChildCount() {
    return evolveSchemaNode.allowedChildCount();
  }

  @Override
  public List<String> getOutputColumnNames() {
    return evolveSchemaNode.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(final PlanVisitor<R, C> visitor, final C context) {
    return visitor.visitPipeEnrichedEvolveSchemaNode(this, context);
  }

  @Override
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    PlanNodeType.PIPE_ENRICHED_EVOLVE_SCHEMA.serialize(byteBuffer);
    evolveSchemaNode.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(final DataOutputStream stream) throws IOException {
    PlanNodeType.PIPE_ENRICHED_EVOLVE_SCHEMA.serialize(stream);
    evolveSchemaNode.serialize(stream);
  }

  public static PipeEnrichedEvolveSchemaNode deserialize(final ByteBuffer buffer) {
    return new PipeEnrichedEvolveSchemaNode((EvolveSchemaNode) PlanNodeType.deserialize(buffer));
  }

  @Override
  public boolean equals(final Object o) {
    return o instanceof PipeEnrichedEvolveSchemaNode
        && evolveSchemaNode.equals(((PipeEnrichedEvolveSchemaNode) o).evolveSchemaNode);
  }

  @Override
  public int hashCode() {
    return evolveSchemaNode.hashCode();
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return evolveSchemaNode.getRegionReplicaSet();
  }

  @Override
  public List<WritePlanNode> splitByPartition(final IAnalysis analysis) {
    return evolveSchemaNode.splitByPartition(analysis).stream()
        .map(
            plan ->
                plan instanceof PipeEnrichedEvolveSchemaNode
                    ? plan
                    : new PipeEnrichedEvolveSchemaNode((EvolveSchemaNode) plan))
        .collect(Collectors.toList());
  }

  @Override
  public void serializeToWAL(final IWALByteBufferView buffer) {
    evolveSchemaNode.serializeToWAL(buffer);
  }

  @Override
  public int serializedSize() {
    return evolveSchemaNode.serializedSize();
  }

  @Override
  public SearchNode merge(List<SearchNode> searchNodes) {
    List<SearchNode> unrichedNodes =
        searchNodes.stream()
            .map(
                searchNode ->
                    (SearchNode) ((PipeEnrichedEvolveSchemaNode) searchNode).EvolveSchemaNode())
            .collect(Collectors.toList());
    return new PipeEnrichedEvolveSchemaNode(
        (EvolveSchemaNode) evolveSchemaNode.merge(unrichedNodes));
  }
}
