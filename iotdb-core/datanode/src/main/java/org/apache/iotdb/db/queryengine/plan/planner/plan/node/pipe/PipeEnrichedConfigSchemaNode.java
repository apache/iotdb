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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.ConstructSchemaBlackListNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.DeactivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.DeleteTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.DeleteLogicalViewNode;
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
 * execute deletion logic and mark it as received, since the request does not need to pass {@link
 * RegionWriteExecutor} and is assigned directly to regions by configNode.
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
public class PipeEnrichedConfigSchemaNode extends PlanNode {

  private final PlanNode configSchemaNode;

  public PipeEnrichedConfigSchemaNode(PlanNode configSchemaNode) {
    super(configSchemaNode.getPlanNodeId());
    this.configSchemaNode = configSchemaNode;
  }

  public PlanNode getConfigSchemaNode() {
    return configSchemaNode;
  }

  @Override
  public boolean isGeneratedByPipe() {
    return configSchemaNode.isGeneratedByPipe();
  }

  @Override
  public void markAsGeneratedByPipe() {
    configSchemaNode.markAsGeneratedByPipe();
  }

  @Override
  public PlanNodeId getPlanNodeId() {
    return configSchemaNode.getPlanNodeId();
  }

  @Override
  public void setPlanNodeId(PlanNodeId id) {
    configSchemaNode.setPlanNodeId(id);
  }

  @Override
  public List<PlanNode> getChildren() {
    return configSchemaNode.getChildren();
  }

  @Override
  public void addChild(PlanNode child) {
    configSchemaNode.addChild(child);
  }

  @Override
  public PlanNode clone() {
    return new PipeEnrichedConfigSchemaNode(configSchemaNode.clone());
  }

  @Override
  public PlanNode createSubNode(int subNodeId, int startIndex, int endIndex) {
    return new PipeEnrichedConfigSchemaNode(
        configSchemaNode.createSubNode(subNodeId, startIndex, endIndex));
  }

  @Override
  public PlanNode cloneWithChildren(List<PlanNode> children) {
    return new PipeEnrichedConfigSchemaNode(configSchemaNode.cloneWithChildren(children));
  }

  @Override
  public int allowedChildCount() {
    return configSchemaNode.allowedChildCount();
  }

  @Override
  public List<String> getOutputColumnNames() {
    return configSchemaNode.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitPipeEnrichedConfigSchema(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.PIPE_ENRICHED_DELETE_SCHEMA.serialize(byteBuffer);
    configSchemaNode.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.PIPE_ENRICHED_DELETE_SCHEMA.serialize(stream);
    configSchemaNode.serialize(stream);
  }

  public static PipeEnrichedConfigSchemaNode deserialize(ByteBuffer buffer) {
    return new PipeEnrichedConfigSchemaNode(PlanNodeType.deserialize(buffer));
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof PipeEnrichedConfigSchemaNode
        && configSchemaNode.equals(((PipeEnrichedConfigSchemaNode) o).configSchemaNode);
  }

  @Override
  public int hashCode() {
    return configSchemaNode.hashCode();
  }
}
