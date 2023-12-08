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
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.ActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.BatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.CreateLogicalViewNode;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class aims to mark the {@link WritePlanNode} of schema writing to prevent forwarding pipe
 * insertions. The handling logic is defined in:
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
public class PipeEnrichedWriteSchemaNode extends WritePlanNode {

  private final WritePlanNode writeSchemaNode;

  public PipeEnrichedWriteSchemaNode(WritePlanNode schemaWriteNode) {
    super(schemaWriteNode.getPlanNodeId());
    this.writeSchemaNode = schemaWriteNode;
  }

  public WritePlanNode getWriteSchemaNode() {
    return writeSchemaNode;
  }

  @Override
  public boolean isGeneratedByPipe() {
    return writeSchemaNode.isGeneratedByPipe();
  }

  @Override
  public void markAsGeneratedByPipe() {
    writeSchemaNode.markAsGeneratedByPipe();
  }

  @Override
  public PlanNodeId getPlanNodeId() {
    return writeSchemaNode.getPlanNodeId();
  }

  @Override
  public void setPlanNodeId(PlanNodeId id) {
    writeSchemaNode.setPlanNodeId(id);
  }

  @Override
  public List<PlanNode> getChildren() {
    return writeSchemaNode.getChildren();
  }

  @Override
  public void addChild(PlanNode child) {
    writeSchemaNode.addChild(child);
  }

  @Override
  public WritePlanNode clone() {
    return new PipeEnrichedWriteSchemaNode((WritePlanNode) writeSchemaNode.clone());
  }

  @Override
  public WritePlanNode createSubNode(int subNodeId, int startIndex, int endIndex) {
    return new PipeEnrichedWriteSchemaNode(
        (WritePlanNode) writeSchemaNode.createSubNode(subNodeId, startIndex, endIndex));
  }

  @Override
  public PlanNode cloneWithChildren(List<PlanNode> children) {
    return new PipeEnrichedWriteSchemaNode(
        (WritePlanNode) writeSchemaNode.cloneWithChildren(children));
  }

  @Override
  public int allowedChildCount() {
    return writeSchemaNode.allowedChildCount();
  }

  @Override
  public List<String> getOutputColumnNames() {
    return writeSchemaNode.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitPipeEnrichedWriteSchema(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.PIPE_ENRICHED_WRITE_SCHEMA.serialize(byteBuffer);
    writeSchemaNode.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.PIPE_ENRICHED_WRITE_SCHEMA.serialize(stream);
    writeSchemaNode.serialize(stream);
  }

  public static PipeEnrichedWriteSchemaNode deserialize(ByteBuffer buffer) {
    return new PipeEnrichedWriteSchemaNode((WritePlanNode) PlanNodeType.deserialize(buffer));
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof PipeEnrichedWriteSchemaNode
        && writeSchemaNode.equals(((PipeEnrichedWriteSchemaNode) o).writeSchemaNode);
  }

  @Override
  public int hashCode() {
    return writeSchemaNode.hashCode();
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return writeSchemaNode.getRegionReplicaSet();
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    return writeSchemaNode.splitByPartition(analysis).stream()
        .map(
            plan ->
                plan instanceof PipeEnrichedWriteSchemaNode
                    ? plan
                    : new PipeEnrichedWriteSchemaNode(plan))
        .collect(Collectors.toList());
  }
}
