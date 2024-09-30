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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanVisitor;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableDeviceAttributeCommitUpdateNode extends PlanNode implements ISchemaRegionPlan {

  private final long version;
  private final Map<TDataNodeLocation, byte[]> commitMap;
  private final Set<Integer> shrunkNodes;

  public static final TableDeviceAttributeCommitUpdateNode MOCK_INSTANCE =
      new TableDeviceAttributeCommitUpdateNode(new PlanNodeId(""), 0L, null, null);

  public TableDeviceAttributeCommitUpdateNode(
      final PlanNodeId id,
      final long version,
      final Map<TDataNodeLocation, byte[]> commitMap,
      final Set<Integer> shrunkNodes) {
    super(id);
    this.version = version;
    this.commitMap = commitMap;
    this.shrunkNodes = shrunkNodes;
  }

  public long getVersion() {
    return version;
  }

  public Map<TDataNodeLocation, byte[]> getCommitMap() {
    return commitMap;
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
    return new TableDeviceAttributeCommitUpdateNode(id, version, commitMap, shrunkNodes);
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
    ReadWriteIOUtils.write(commitMap.size(), byteBuffer);
    for (final Map.Entry<TDataNodeLocation, byte[]> entry : commitMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey().getDataNodeId(), byteBuffer);
      ThriftCommonsSerDeUtils.serializeTEndPoint(entry.getKey().getInternalEndPoint(), byteBuffer);
      ReadWriteIOUtils.write(entry.getValue().length, byteBuffer);
      byteBuffer.put(entry.getValue());
    }
    ReadWriteIOUtils.write(shrunkNodes.size(), byteBuffer);
    for (final Integer nodeId : shrunkNodes) {
      ReadWriteIOUtils.write(nodeId, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(final DataOutputStream stream) throws IOException {
    getType().serialize(stream);
    ReadWriteIOUtils.write(version, stream);
    ReadWriteIOUtils.write(commitMap.size(), stream);
    for (final Map.Entry<TDataNodeLocation, byte[]> entry : commitMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey().getDataNodeId(), stream);
      ThriftCommonsSerDeUtils.serializeTEndPoint(entry.getKey().getInternalEndPoint(), stream);
      ReadWriteIOUtils.write(entry.getValue().length, stream);
      stream.write(entry.getValue());
    }
    ReadWriteIOUtils.write(shrunkNodes.size(), stream);
    for (final Integer nodeId : shrunkNodes) {
      ReadWriteIOUtils.write(nodeId, stream);
    }
  }

  public static PlanNode deserialize(final ByteBuffer buffer) {
    final long version = ReadWriteIOUtils.readLong(buffer);
    int size = ReadWriteIOUtils.readInt(buffer);
    final Map<TDataNodeLocation, byte[]> commitMap = new HashMap<>(size);
    for (int i = 0; i < size; ++i) {
      final TDataNodeLocation location =
          new TDataNodeLocation(
              ReadWriteIOUtils.readInt(buffer),
              null,
              ThriftCommonsSerDeUtils.deserializeTEndPoint(buffer),
              null,
              null,
              null);
      final byte[] commitBuffer = new byte[ReadWriteIOUtils.readInt(buffer)];
      buffer.get(commitBuffer);
      commitMap.put(location, commitBuffer);
    }
    size = ReadWriteIOUtils.readInt(buffer);
    final Set<Integer> shrunkNodes = new HashSet<>();
    for (int i = 0; i < size; ++i) {
      shrunkNodes.add(ReadWriteIOUtils.readInt(buffer));
    }
    final PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new TableDeviceAttributeCommitUpdateNode(planNodeId, version, commitMap, shrunkNodes);
  }

  @Override
  public SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.COMMIT_UPDATE_TABLE_DEVICE_ATTRIBUTE;
  }

  @Override
  public <R, C> R accept(final SchemaRegionPlanVisitor<R, C> visitor, final C context) {
    return visitor.visitCommitUpdateTableDeviceAttribute(this, context);
  }
}
