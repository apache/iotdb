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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.exception.runtime.SerializationRunTimeException;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class DeleteRegionNode extends WritePlanNode implements IConsensusRequest {

  private final Logger logger = LoggerFactory.getLogger(DeleteRegionNode.class);

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

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C schemaRegion) {
    return visitor.visitDeleteRegion(this, schemaRegion);
  }

  public static DeleteRegionNode deserialize(ByteBuffer byteBuffer) {
    int type = ReadWriteIOUtils.readInt(byteBuffer);
    int id = ReadWriteIOUtils.readInt(byteBuffer);
    ConsensusGroupId consensusGroupId = ConsensusGroupId.Factory.create(type, id);
    String planNodeId = ReadWriteIOUtils.readString(byteBuffer);
    return new DeleteRegionNode(new PlanNodeId(planNodeId), consensusGroupId);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DELETE_REGION.serialize(byteBuffer);
    ReadWriteIOUtils.write(consensusGroupId.getType().getValue(), byteBuffer);
    ReadWriteIOUtils.write(consensusGroupId.getId(), byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.DELETE_REGION.serialize(stream);
    ReadWriteIOUtils.write(consensusGroupId.getType().getValue(), stream);
    ReadWriteIOUtils.write(consensusGroupId.getId(), stream);
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    try (PublicBAOS publicBAOS = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(publicBAOS)) {
      super.serialize(outputStream);
      return ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());
    } catch (IOException e) {
      logger.error("Unexpected error occurs when serializing this DeleteRegionNode.", e);
      throw new SerializationRunTimeException(e);
    }
  }
}
