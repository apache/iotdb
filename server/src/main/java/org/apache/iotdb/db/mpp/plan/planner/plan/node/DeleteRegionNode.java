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

package org.apache.iotdb.db.mpp.plan.planner.plan.node;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.List;

public class DeleteRegionNode extends WritePlanNode implements IConsensusRequest {

  protected ConsensusGroupId consensusGroupId;

  public DeleteRegionNode(PlanNodeId id) {
    super(id);
  }

  public DeleteRegionNode(PlanNodeId id, ConsensusGroupId consensusGroupId) {
    super(id);
    this.consensusGroupId = consensusGroupId;
  }

  public ConsensusGroupId getConsensusGroupId() {
    return consensusGroupId;
  }

  public void setConsensusGroupId(ConsensusGroupId consensusGroupId) {
    this.consensusGroupId = consensusGroupId;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return null;
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return null;
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    return null;
  }

  public static DeleteRegionNode deserialize(ByteBuffer byteBuffer) {
    TConsensusGroupType type =
        TConsensusGroupType.findByValue(ReadWriteIOUtils.readInt(byteBuffer));
    ConsensusGroupId consensusGroupId = ConsensusGroupId.Factory.createEmpty(type);
    consensusGroupId.setId(ReadWriteIOUtils.readInt(byteBuffer));
    String id = ReadWriteIOUtils.readString(byteBuffer);
    return new DeleteRegionNode(new PlanNodeId(id), consensusGroupId);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DELETE_REGION.serialize(byteBuffer);
    ReadWriteIOUtils.write(consensusGroupId.getType().getValue(), byteBuffer);
    ReadWriteIOUtils.write(consensusGroupId.getId(), byteBuffer);
  }

  @Override
  public void serializeRequest(ByteBuffer buffer) {
    super.serialize(buffer);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C schemaRegion) {
    return visitor.visitDeleteSchemaRegion(this, schemaRegion);
  }
}
