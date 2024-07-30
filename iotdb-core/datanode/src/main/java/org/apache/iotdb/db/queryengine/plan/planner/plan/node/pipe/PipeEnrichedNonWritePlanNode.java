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
import org.apache.iotdb.db.queryengine.execution.executor.RegionWriteExecutor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.ConstructSchemaBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.DeactivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.DeleteTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.DeleteLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * This class aims to mark the {@link PlanNode} of schema operation assigned by configNode to
 * selectively forward the operations, {@link Statement}s of which extending {@link
 * IConfigStatement}. The handling logic is defined only in {@link SchemaExecutionVisitor} to
 * execute deletion logic and mark it as received, since the request is assigned directly to {@link
 * RegionWriteExecutor} and needn't be split by partition.
 *
 * <p>
 *
 * <p>Notes: The contents includes all the collected {@link PlanNode}s of schema deletion:
 *
 * <p>- Timeseries: {@link DeleteTimeSeriesNode}
 *
 * <p>- Template: {@link DeactivateTemplateNode}
 *
 * <p>- LogicalView: {@link AlterLogicalViewNode}, {@link DeleteLogicalViewNode}
 *
 * <p>Intermediate nodes like {@link ConstructSchemaBlackListNode} will not be included since they
 * will not be collected by pipe.
 */
public class PipeEnrichedNonWritePlanNode extends PlanNode {

  private final PlanNode nonWritePlanNode;

  public PipeEnrichedNonWritePlanNode(PlanNode nonWritePlanNode) {
    super(nonWritePlanNode.getPlanNodeId());
    this.nonWritePlanNode = nonWritePlanNode;
  }

  public PlanNode getNonWritePlanNode() {
    return nonWritePlanNode;
  }

  @Override
  public boolean isGeneratedByPipe() {
    return nonWritePlanNode.isGeneratedByPipe();
  }

  @Override
  public void markAsGeneratedByPipe() {
    nonWritePlanNode.markAsGeneratedByPipe();
  }

  @Override
  public PlanNodeId getPlanNodeId() {
    return nonWritePlanNode.getPlanNodeId();
  }

  @Override
  public void setPlanNodeId(PlanNodeId id) {
    nonWritePlanNode.setPlanNodeId(id);
  }

  @Override
  public List<PlanNode> getChildren() {
    return nonWritePlanNode.getChildren();
  }

  @Override
  public void addChild(PlanNode child) {
    nonWritePlanNode.addChild(child);
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.PIPE_ENRICHED_NON_WRITE;
  }

  @Override
  public PlanNode clone() {
    return new PipeEnrichedNonWritePlanNode(nonWritePlanNode.clone());
  }

  @Override
  public PlanNode createSubNode(int subNodeId, int startIndex, int endIndex) {
    return new PipeEnrichedNonWritePlanNode(
        nonWritePlanNode.createSubNode(subNodeId, startIndex, endIndex));
  }

  @Override
  public PlanNode cloneWithChildren(List<PlanNode> children) {
    return new PipeEnrichedNonWritePlanNode(nonWritePlanNode.cloneWithChildren(children));
  }

  @Override
  public int allowedChildCount() {
    return nonWritePlanNode.allowedChildCount();
  }

  @Override
  public List<String> getOutputColumnNames() {
    return nonWritePlanNode.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitPipeEnrichedNonWritePlanNode(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.PIPE_ENRICHED_NON_WRITE.serialize(byteBuffer);
    nonWritePlanNode.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.PIPE_ENRICHED_NON_WRITE.serialize(stream);
    nonWritePlanNode.serialize(stream);
  }

  public static PipeEnrichedNonWritePlanNode deserialize(ByteBuffer buffer) {
    return new PipeEnrichedNonWritePlanNode(PlanNodeType.deserialize(buffer));
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof PipeEnrichedNonWritePlanNode
        && nonWritePlanNode.equals(((PipeEnrichedNonWritePlanNode) o).nonWritePlanNode);
  }

  @Override
  public int hashCode() {
    return nonWritePlanNode.hashCode();
  }
}
