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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.write;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.path.PathDeserializeUtil;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.IPartitionRelatedNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DeleteDataNode extends PlanNode implements IPartitionRelatedNode {

  private final QueryId queryId;
  private final List<PartialPath> pathList;
  private final List<String> storageGroups;

  private TRegionReplicaSet regionReplicaSet;

  public DeleteDataNode(
      PlanNodeId id, QueryId queryId, List<PartialPath> pathList, List<String> storageGroups) {
    super(id);
    this.pathList = pathList;
    this.queryId = queryId;
    this.storageGroups = storageGroups;
  }

  public List<PartialPath> getPathList() {
    return pathList;
  }

  @Override
  public List<PlanNode> getChildren() {
    return new ArrayList<>();
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new DeleteDataNode(getPlanNodeId(), queryId, pathList, storageGroups);
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DELETE_DATA.serialize(byteBuffer);
    queryId.serialize(byteBuffer);
    ReadWriteIOUtils.write(pathList.size(), byteBuffer);
    for (PartialPath path : pathList) {
      path.serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(storageGroups.size(), byteBuffer);
    for (String storageGroup : storageGroups) {
      ReadWriteIOUtils.write(storageGroup, byteBuffer);
    }
  }

  public static DeleteDataNode deserialize(ByteBuffer byteBuffer) {
    QueryId queryId = QueryId.deserialize(byteBuffer);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<PartialPath> pathList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      pathList.add((PartialPath) PathDeserializeUtil.deserialize(byteBuffer));
    }
    size = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> storageGroups = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      storageGroups.add(ReadWriteIOUtils.readString(byteBuffer));
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new DeleteDataNode(planNodeId, queryId, pathList, storageGroups);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitDeleteData(this, context);
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  public void setRegionReplicaSet(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public List<String> getStorageGroups() {
    return storageGroups;
  }

  public String toString() {
    return String.format(
        "DeleteDataNode-%s[ Paths: %s, StorageGroups: %s, Region: %s ]",
        getPlanNodeId(),
        pathList,
        storageGroups,
        regionReplicaSet == null ? "Not Assigned" : regionReplicaSet.getRegionId());
  }
}
