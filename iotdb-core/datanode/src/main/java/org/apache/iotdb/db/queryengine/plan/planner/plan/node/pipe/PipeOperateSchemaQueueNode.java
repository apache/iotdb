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

import org.apache.iotdb.db.pipe.source.schemaregion.SchemaRegionListeningQueue;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegion;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * {@link PipeOperateSchemaQueueNode} is for pipe to open or close the {@link
 * SchemaRegionListeningQueue}. It is for written to {@link SchemaRegion} consensus layer to ensure
 * the identity of the {@link SchemaRegionListeningQueue} in all peers.
 */
public class PipeOperateSchemaQueueNode extends PlanNode {

  private final boolean isOpen;

  public PipeOperateSchemaQueueNode(PlanNodeId id, boolean isOpen) {
    super(id);
    this.isOpen = isOpen;
  }

  public boolean isOpen() {
    return isOpen;
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(PlanNode child) {
    // Do nothing
  }

  @Override
  public PlanNode clone() {
    return new PipeOperateSchemaQueueNode(id, isOpen);
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitPipeOperateSchemaQueueNode(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.PIPE_OPERATE_SCHEMA_QUEUE_REFERENCE.serialize(byteBuffer);
    ReadWriteIOUtils.write(isOpen, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.PIPE_OPERATE_SCHEMA_QUEUE_REFERENCE.serialize(stream);
    ReadWriteIOUtils.write(isOpen, stream);
  }

  public static PipeOperateSchemaQueueNode deserialize(ByteBuffer byteBuffer) {
    boolean isOpen = ReadWriteIOUtils.readBool(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new PipeOperateSchemaQueueNode(planNodeId, isOpen);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PipeOperateSchemaQueueNode that = (PipeOperateSchemaQueueNode) o;

    return Objects.equals(getPlanNodeId(), that.getPlanNodeId())
        && Objects.equals(isOpen, that.isOpen);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getPlanNodeId(), isOpen);
  }
}
