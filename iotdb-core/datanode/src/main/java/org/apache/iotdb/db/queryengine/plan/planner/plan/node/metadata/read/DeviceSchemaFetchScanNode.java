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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DeviceSchemaFetchScanNode extends SchemaFetchScanNode {

  private final PathPatternTree authorityScope;

  public DeviceSchemaFetchScanNode(
      PlanNodeId id,
      PartialPath storageGroup,
      PathPatternTree patternTree,
      PathPatternTree authorityScope) {
    super(id, storageGroup, patternTree);
    this.authorityScope = authorityScope;
    this.authorityScope.constructTree();
  }

  public PathPatternTree getAuthorityScope() {
    return authorityScope;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.DEVICE_SCHEMA_FETCH_SCAN;
  }

  @Override
  public PlanNode clone() {
    return new DeviceSchemaFetchScanNode(
        getPlanNodeId(), storageGroup, patternTree, authorityScope);
  }

  @Override
  public String toString() {
    return String.format(
        "DeviceSchemaFetchScanNode-%s:[StorageGroup: %s, DataRegion: %s]",
        this.getPlanNodeId(),
        storageGroup,
        PlanNodeUtil.printRegionReplicaSet(getRegionReplicaSet()));
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DEVICE_SCHEMA_FETCH_SCAN.serialize(byteBuffer);
    storageGroup.serialize(byteBuffer);
    patternTree.serialize(byteBuffer);
    authorityScope.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.DEVICE_SCHEMA_FETCH_SCAN.serialize(stream);
    storageGroup.serialize(stream);
    patternTree.serialize(stream);
    authorityScope.serialize(stream);
  }

  public static DeviceSchemaFetchScanNode deserialize(ByteBuffer byteBuffer) {
    PartialPath storageGroup = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
    PathPatternTree patternTree = PathPatternTree.deserialize(byteBuffer);
    PathPatternTree authorityScope = PathPatternTree.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new DeviceSchemaFetchScanNode(planNodeId, storageGroup, patternTree, authorityScope);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitDeviceSchemaFetchScan(this, context);
  }
}
