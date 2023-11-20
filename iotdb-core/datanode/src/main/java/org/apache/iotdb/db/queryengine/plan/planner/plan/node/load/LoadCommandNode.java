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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.load;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler.LoadCommand;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class LoadCommandNode extends PlanNode {
  private LoadCommand loadCommand;
  private String uuid;
  private ConsensusGroupId consensusGroupId;
  private boolean isGeneratedByPipe;

  public LoadCommandNode(PlanNodeId id) {
    super(id);
  }

  public LoadCommandNode(
      PlanNodeId id,
      LoadCommand loadCommand,
      String uuid,
      ConsensusGroupId consensusGroupId,
      boolean isGeneratedByPipe) {
    super(id);
    this.loadCommand = loadCommand;
    this.uuid = uuid;
    this.consensusGroupId = consensusGroupId;
    this.isGeneratedByPipe = isGeneratedByPipe;
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(PlanNode child) {
    // no children for this plan
  }

  @SuppressWarnings({"java:S2975", "java:S1182"})
  @Override
  public PlanNode clone() {
    return new LoadCommandNode(getPlanNodeId(), loadCommand, uuid, consensusGroupId, isGeneratedByPipe);
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
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.LOAD_COMMAND.serialize(byteBuffer);
    byteBuffer.put((byte) loadCommand.ordinal());
    ReadWriteIOUtils.write(uuid, byteBuffer);
    if (consensusGroupId == null) {
      ReadWriteIOUtils.write(-1, byteBuffer);
    } else {
      ReadWriteIOUtils.write(consensusGroupId.getType().getValue(), byteBuffer);
      ReadWriteIOUtils.write(consensusGroupId.getId(), byteBuffer);
    }
    ReadWriteIOUtils.write(isGeneratedByPipe, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.LOAD_COMMAND.serialize(stream);
    stream.write((byte) loadCommand.ordinal());
    ReadWriteIOUtils.write(uuid, stream);
    if (consensusGroupId == null) {
      ReadWriteIOUtils.write(-1, stream);
    } else {
      ReadWriteIOUtils.write(consensusGroupId.getType().getValue(), stream);
      ReadWriteIOUtils.write(consensusGroupId.getId(), stream);
    }
    ReadWriteIOUtils.write(isGeneratedByPipe, stream);
  }

  public static LoadCommandNode deserialize(ByteBuffer byteBuffer) {
    LoadCommand loadCommand = LoadCommand.values()[byteBuffer.get()];
    String uuid = ReadWriteIOUtils.readString(byteBuffer);
    int consensusType = byteBuffer.getInt();
    ConsensusGroupId consensusGroupId = null;
    if (consensusType != -1) {
      int consensusId = byteBuffer.getInt();
      consensusGroupId = ConsensusGroupId.Factory.create(consensusType, consensusId);
    }
    boolean isGeneratedByPipe = ReadWriteIOUtils.readBool(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new LoadCommandNode(planNodeId, loadCommand, uuid, consensusGroupId, isGeneratedByPipe);
  }

  public LoadCommand getLoadCommand() {
    return loadCommand;
  }

  public String getUuid() {
    return uuid;
  }

  public ConsensusGroupId getConsensusGroupId() {
    return consensusGroupId;
  }

  public boolean isGeneratedByPipe() {
    return isGeneratedByPipe;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitLoadCommand(this, context);
  }
}
