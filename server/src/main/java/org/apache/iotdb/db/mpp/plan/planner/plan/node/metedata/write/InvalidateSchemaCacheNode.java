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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.path.PathDeserializeUtil;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class InvalidateSchemaCacheNode extends WritePlanNode {

  private final List<PartialPath> pathList;

  private TRegionReplicaSet regionReplicaSet;

  public InvalidateSchemaCacheNode(PlanNodeId id, List<PartialPath> pathList) {
    super(id);
    this.pathList = pathList;
  }

  private InvalidateSchemaCacheNode(
      PlanNodeId id, List<PartialPath> pathList, TRegionReplicaSet regionReplicaSet) {
    super(id);
    this.pathList = pathList;
    this.regionReplicaSet = regionReplicaSet;
  }

  public List<PartialPath> getPathList() {
    return pathList;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new InvalidateSchemaCacheNode(getPlanNodeId(), pathList);
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
    ReadWriteIOUtils.write(pathList.size(), byteBuffer);
    for (PartialPath path : pathList) {
      path.serialize(byteBuffer);
    }
  }

  public static InvalidateSchemaCacheNode deserialize(ByteBuffer byteBuffer) {
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<PartialPath> pathList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      pathList.add((PartialPath) PathDeserializeUtil.deserialize(byteBuffer));
    }

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new InvalidateSchemaCacheNode(planNodeId, pathList);
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return this.regionReplicaSet;
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    return analysis.getRegionRequestList().stream()
        .map(
            pair ->
                new InvalidateSchemaCacheNode(
                    getPlanNodeId(), pair.right, pair.left.getRegionReplicaSet()))
        .collect(Collectors.toList());
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitInvalidateSchemaCache(this, context);
  }
}
