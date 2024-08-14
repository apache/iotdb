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
import org.apache.iotdb.db.consensus.statemachine.schemaregion.SchemaExecutionVisitor;
import org.apache.iotdb.db.queryengine.execution.executor.RegionWriteExecutor;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.ActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.BatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.CreateLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class aims to mark the {@link WritePlanNode} of schema writing to selectively forward pipe
 * schema operations, {@link Statement}s of which passing through {@link SchemaExecutionVisitor}.
 * The handling logic is defined in:
 *
 * <p>1.{@link RegionWriteExecutor}, to serialize and reach the target schema region.
 *
 * <p>2.{@link SchemaExecutionVisitor}, to actually write data on schema region and mark it as
 * received from pipe.
 *
 * <p>
 *
 * <p>Notes: The content varies in all the {@link WritePlanNode}s of schema writing nodes:
 *
 * <p>- Timeseries(Manual): {@link CreateTimeSeriesNode}, {@link CreateAlignedTimeSeriesNode},
 * {@link CreateMultiTimeSeriesNode}, {@link AlterTimeSeriesNode}
 *
 * <p>- Timeseries(Auto): {@link InternalCreateTimeSeriesNode}, {@link
 * InternalCreateMultiTimeSeriesNode}
 *
 * <p>- Template(Manual): {@link ActivateTemplateNode}, {@link BatchActivateTemplateNode}
 *
 * <p>- Template(Auto): {@link InternalBatchActivateTemplateNode}
 *
 * <p>- LogicalView: {@link CreateLogicalViewNode}
 */
public class PipeEnrichedWritePlanNode extends WritePlanNode {

  private final WritePlanNode writePlanNode;

  public PipeEnrichedWritePlanNode(WritePlanNode schemaWriteNode) {
    super(schemaWriteNode.getPlanNodeId());
    this.writePlanNode = schemaWriteNode;
  }

  public WritePlanNode getWritePlanNode() {
    return writePlanNode;
  }

  @Override
  public boolean isGeneratedByPipe() {
    return writePlanNode.isGeneratedByPipe();
  }

  @Override
  public void markAsGeneratedByPipe() {
    writePlanNode.markAsGeneratedByPipe();
  }

  @Override
  public PlanNodeId getPlanNodeId() {
    return writePlanNode.getPlanNodeId();
  }

  @Override
  public void setPlanNodeId(PlanNodeId id) {
    writePlanNode.setPlanNodeId(id);
  }

  @Override
  public List<PlanNode> getChildren() {
    return writePlanNode.getChildren();
  }

  @Override
  public void addChild(PlanNode child) {
    writePlanNode.addChild(child);
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.PIPE_ENRICHED_WRITE;
  }

  @Override
  public WritePlanNode clone() {
    return new PipeEnrichedWritePlanNode((WritePlanNode) writePlanNode.clone());
  }

  @Override
  public WritePlanNode createSubNode(int subNodeId, int startIndex, int endIndex) {
    return new PipeEnrichedWritePlanNode(
        (WritePlanNode) writePlanNode.createSubNode(subNodeId, startIndex, endIndex));
  }

  @Override
  public PlanNode cloneWithChildren(List<PlanNode> children) {
    return new PipeEnrichedWritePlanNode((WritePlanNode) writePlanNode.cloneWithChildren(children));
  }

  @Override
  public int allowedChildCount() {
    return writePlanNode.allowedChildCount();
  }

  @Override
  public List<String> getOutputColumnNames() {
    return writePlanNode.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitPipeEnrichedWritePlanNode(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.PIPE_ENRICHED_WRITE.serialize(byteBuffer);
    writePlanNode.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.PIPE_ENRICHED_WRITE.serialize(stream);
    writePlanNode.serialize(stream);
  }

  public static PipeEnrichedWritePlanNode deserialize(ByteBuffer buffer) {
    return new PipeEnrichedWritePlanNode((WritePlanNode) PlanNodeType.deserialize(buffer));
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof PipeEnrichedWritePlanNode
        && writePlanNode.equals(((PipeEnrichedWritePlanNode) o).writePlanNode);
  }

  @Override
  public int hashCode() {
    return writePlanNode.hashCode();
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return writePlanNode.getRegionReplicaSet();
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    return writePlanNode.splitByPartition(analysis).stream()
        .map(
            plan ->
                plan instanceof PipeEnrichedWritePlanNode
                    ? plan
                    : new PipeEnrichedWritePlanNode(plan))
        .collect(Collectors.toList());
  }
}
