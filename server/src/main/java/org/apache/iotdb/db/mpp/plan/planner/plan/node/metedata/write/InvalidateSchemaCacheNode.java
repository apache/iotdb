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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class InvalidateSchemaCacheNode extends PlanNode {

  private final QueryId queryId;

  private final List<PartialPath> pathList;

  private final List<String> storageGroups;

  public InvalidateSchemaCacheNode(
      PlanNodeId id, QueryId queryId, List<PartialPath> pathList, List<String> storageGroups) {
    super(id);
    this.queryId = queryId;
    this.pathList = pathList;
    this.storageGroups = storageGroups;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public List<PartialPath> getPathList() {
    return pathList;
  }

  public List<String> getStorageGroups() {
    return storageGroups;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new InvalidateSchemaCacheNode(getPlanNodeId(), queryId, pathList, storageGroups);
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
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.INVALIDATE_SCHEMA_CACHE.serialize(byteBuffer);
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

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.INVALIDATE_SCHEMA_CACHE.serialize(stream);
    queryId.serialize(stream);
    ReadWriteIOUtils.write(pathList.size(), stream);
    for (PartialPath path : pathList) {
      path.serialize(stream);
    }
    ReadWriteIOUtils.write(storageGroups.size(), stream);
    for (String storageGroup : storageGroups) {
      ReadWriteIOUtils.write(storageGroup, stream);
    }
  }

  public static InvalidateSchemaCacheNode deserialize(ByteBuffer byteBuffer) {
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
    return new InvalidateSchemaCacheNode(planNodeId, queryId, pathList, storageGroups);
  }
}
