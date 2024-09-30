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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TableDeviceAttributeCommitNode extends PlanNode {

  private final long version;
  private final byte[] commitBuffer;
  private final Set<Integer> shrunkNodes;

  protected TableDeviceAttributeCommitNode(
      final PlanNodeId id,
      final long version,
      final byte[] commitBuffer,
      final Set<Integer> shrunkNodes) {
    super(id);
    this.version = version;
    this.commitBuffer = commitBuffer;
    this.shrunkNodes = shrunkNodes;
  }

  public long getVersion() {
    return version;
  }

  public byte[] getCommitBuffer() {
    return commitBuffer;
  }

  public Set<Integer> getShrunkNodes() {
    return shrunkNodes;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(final PlanNode child) {
    // Do nothing
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.TABLE_DEVICE_ATTRIBUTE_COMMIT;
  }

  @Override
  public PlanNode clone() {
    return new TableDeviceAttributeCommitNode(id, version, commitBuffer, shrunkNodes);
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
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    getType().serialize(byteBuffer);
    ReadWriteIOUtils.write(version, byteBuffer);
    ReadWriteIOUtils.write(commitBuffer.length, byteBuffer);
    byteBuffer.put(commitBuffer);
    ReadWriteIOUtils.write(shrunkNodes.size(), byteBuffer);
    for (final Integer nodeId : shrunkNodes) {
      ReadWriteIOUtils.write(nodeId, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(final DataOutputStream stream) throws IOException {
    getType().serialize(stream);
    ReadWriteIOUtils.write(version, stream);
    ReadWriteIOUtils.write(commitBuffer.length, stream);
    stream.write(commitBuffer);
    ReadWriteIOUtils.write(shrunkNodes.size(), stream);
    for (final Integer nodeId : shrunkNodes) {
      ReadWriteIOUtils.write(nodeId, stream);
    }
  }

  public static PlanNode deserialize(final ByteBuffer buffer) {
    final long version = ReadWriteIOUtils.readLong(buffer);
    final byte[] commitBuffer = new byte[ReadWriteIOUtils.readInt(buffer)];
    buffer.get(commitBuffer);
    final int size = ReadWriteIOUtils.readInt(buffer);
    final Set<Integer> shrunkNodes = new HashSet<>();
    for (int i = 0; i < size; ++i) {
      shrunkNodes.add(ReadWriteIOUtils.readInt(buffer));
    }
    final PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new TableDeviceAttributeCommitNode(planNodeId, version, commitBuffer, shrunkNodes);
  }
}
