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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.attribute.update.DeviceAttributeCacheUpdater;

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
  private final Set<TDataNodeLocation> shrunkNodes;
  private final TDataNodeLocation leaderLocation;

  public static final TableDeviceAttributeCommitUpdateNode MOCK_INSTANCE =
      new TableDeviceAttributeCommitUpdateNode(new PlanNodeId(""), 0L, null, null, null);

  public TableDeviceAttributeCommitUpdateNode(
      final PlanNodeId id,
      final long version,
      final Map<TDataNodeLocation, byte[]> commitMap,
      final Set<TDataNodeLocation> shrunkNodes,
      final TDataNodeLocation leaderLocation) {
    super(id);
    this.version = version;
    this.commitMap = commitMap;
    this.shrunkNodes = shrunkNodes;
    this.leaderLocation = leaderLocation;
  }

  public long getVersion() {
    return version;
  }

  public Map<TDataNodeLocation, byte[]> getCommitMap() {
    return commitMap;
  }

  public Set<TDataNodeLocation> getShrunkNodes() {
    return shrunkNodes;
  }

  public TDataNodeLocation getLeaderLocation() {
    return leaderLocation;
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
    return new TableDeviceAttributeCommitUpdateNode(
        id, version, commitMap, shrunkNodes, leaderLocation);
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
  public <R, C> R accept(final PlanVisitor<R, C> visitor, final C context) {
    return visitor.visitTableDeviceAttributeCommit(this, context);
  }

  @Override
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    getType().serialize(byteBuffer);
    ReadWriteIOUtils.write(version, byteBuffer);
    ReadWriteIOUtils.write(commitMap.size(), byteBuffer);

    for (final Map.Entry<TDataNodeLocation, byte[]> entry : commitMap.entrySet()) {
      DeviceAttributeCacheUpdater.serializeNodeLocation4AttributeUpdate(entry.getKey(), byteBuffer);
      ReadWriteIOUtils.write(entry.getValue().length, byteBuffer);
      byteBuffer.put(entry.getValue());
    }

    ReadWriteIOUtils.write(shrunkNodes.size(), byteBuffer);
    for (final TDataNodeLocation location : shrunkNodes) {
      DeviceAttributeCacheUpdater.serializeNodeLocation4AttributeUpdate(location, byteBuffer);
    }

    DeviceAttributeCacheUpdater.serializeNodeLocation4AttributeUpdate(leaderLocation, byteBuffer);
  }

  @Override
  protected void serializeAttributes(final DataOutputStream stream) throws IOException {
    getType().serialize(stream);
    ReadWriteIOUtils.write(version, stream);
    ReadWriteIOUtils.write(commitMap.size(), stream);

    for (final Map.Entry<TDataNodeLocation, byte[]> entry : commitMap.entrySet()) {
      DeviceAttributeCacheUpdater.serializeNodeLocation4AttributeUpdate(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue().length, stream);
      stream.write(entry.getValue());
    }

    ReadWriteIOUtils.write(shrunkNodes.size(), stream);
    for (final TDataNodeLocation location : shrunkNodes) {
      DeviceAttributeCacheUpdater.serializeNodeLocation4AttributeUpdate(location, stream);
    }

    DeviceAttributeCacheUpdater.serializeNodeLocation4AttributeUpdate(leaderLocation, stream);
  }

  public static PlanNode deserialize(final ByteBuffer buffer) {
    final long version = ReadWriteIOUtils.readLong(buffer);

    int size = ReadWriteIOUtils.readInt(buffer);
    final Map<TDataNodeLocation, byte[]> commitMap = new HashMap<>(size);
    for (int i = 0; i < size; ++i) {
      final TDataNodeLocation location =
          DeviceAttributeCacheUpdater.deserializeNodeLocationForAttributeUpdate(buffer);
      final byte[] commitBuffer = new byte[ReadWriteIOUtils.readInt(buffer)];
      buffer.get(commitBuffer);
      commitMap.put(location, commitBuffer);
    }

    size = ReadWriteIOUtils.readInt(buffer);
    final Set<TDataNodeLocation> shrunkNodes = new HashSet<>();
    for (int i = 0; i < size; ++i) {
      shrunkNodes.add(
          DeviceAttributeCacheUpdater.deserializeNodeLocationForAttributeUpdate(buffer));
    }

    final TDataNodeLocation localLocation =
        DeviceAttributeCacheUpdater.deserializeNodeLocationForAttributeUpdate(buffer);
    final PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new TableDeviceAttributeCommitUpdateNode(
        planNodeId, version, commitMap, shrunkNodes, localLocation);
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
