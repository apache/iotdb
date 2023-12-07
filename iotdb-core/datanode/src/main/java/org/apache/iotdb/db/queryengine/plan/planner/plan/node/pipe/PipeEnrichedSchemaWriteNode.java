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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.AlterLogicalViewNode;
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
 * <p>Notes: The contents includes all the {@link WritePlanNode}s of schema writing:
 *
 * <p>- Timeseries: {@link CreateTimeSeriesNode}, {@link AlterTimeSeriesNode}, {@link
 * CreateAlignedTimeSeriesNode}, {@link CreateMultiTimeSeriesNode}
 *
 * <p>- LogicalView: {@link CreateLogicalViewNode}, {@link AlterLogicalViewNode}
 *
 * <p>- Template: {@link ActivateTemplateNode}
 */
public class PipeEnrichedSchemaWriteNode extends WritePlanNode {

  private final WritePlanNode schemaWriteNode;

  public PipeEnrichedSchemaWriteNode(WritePlanNode schemaWriteNode) {
    super(schemaWriteNode.getPlanNodeId());
    this.schemaWriteNode = schemaWriteNode;
  }

  public PlanNode getSchemaWriteNode() {
    return schemaWriteNode;
  }

  @Override
  public boolean isGeneratedByPipe() {
    return schemaWriteNode.isGeneratedByPipe();
  }

  @Override
  public void markAsGeneratedByPipe() {
    schemaWriteNode.markAsGeneratedByPipe();
  }

  @Override
  public PlanNodeId getPlanNodeId() {
    return schemaWriteNode.getPlanNodeId();
  }

  @Override
  public void setPlanNodeId(PlanNodeId id) {
    schemaWriteNode.setPlanNodeId(id);
  }

  @Override
  public List<PlanNode> getChildren() {
    return schemaWriteNode.getChildren();
  }

  @Override
  public void addChild(PlanNode child) {
    schemaWriteNode.addChild(child);
  }

  @Override
  public WritePlanNode clone() {
    return new PipeEnrichedSchemaWriteNode((WritePlanNode) schemaWriteNode.clone());
  }

  @Override
  public WritePlanNode createSubNode(int subNodeId, int startIndex, int endIndex) {
    return new PipeEnrichedSchemaWriteNode(
        (WritePlanNode) schemaWriteNode.createSubNode(subNodeId, startIndex, endIndex));
  }

  @Override
  public PlanNode cloneWithChildren(List<PlanNode> children) {
    return new PipeEnrichedSchemaWriteNode(
        (WritePlanNode) schemaWriteNode.cloneWithChildren(children));
  }

  @Override
  public int allowedChildCount() {
    return schemaWriteNode.allowedChildCount();
  }

  @Override
  public List<String> getOutputColumnNames() {
    return schemaWriteNode.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitPipeEnrichedSchemaWriteNode(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.PIPE_ENRICHED_SCHEMA_WRITE.serialize(byteBuffer);
    schemaWriteNode.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.PIPE_ENRICHED_SCHEMA_WRITE.serialize(stream);
    schemaWriteNode.serialize(stream);
  }

  public static PlanNode deserialize(ByteBuffer buffer) {
    return new PipeEnrichedSchemaWriteNode((WritePlanNode) PlanNodeType.deserialize(buffer));
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof PipeEnrichedSchemaWriteNode
        && schemaWriteNode.equals(((PipeEnrichedSchemaWriteNode) o).schemaWriteNode);
  }

  @Override
  public int hashCode() {
    return schemaWriteNode.hashCode();
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return schemaWriteNode.getRegionReplicaSet();
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    return schemaWriteNode.splitByPartition(analysis).stream()
        .map(
            plan ->
                plan instanceof PipeEnrichedSchemaWriteNode
                    ? plan
                    : new PipeEnrichedSchemaWriteNode(plan))
        .collect(Collectors.toList());
  }
}
