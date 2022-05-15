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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write.DeleteTimeSeriesSchemaNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.DeleteTimeSeriesDataNode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DeleteTimeSeriesNode extends WritePlanNode {

  private final List<PartialPath> deletedPaths;
  private final Map<PartialPath, PlanNode> schemaRegionSpiltMap;
  private final Map<PartialPath, List<PlanNode>> dataRegionSplitMap;

  public DeleteTimeSeriesNode(PlanNodeId id, List<PartialPath> deletedPath) {
    super(id);
    this.deletedPaths = deletedPath;
    this.schemaRegionSpiltMap = new HashMap<>();
    this.dataRegionSplitMap = new HashMap<>();
  }

  public List<PartialPath> getDeletedPaths() {
    return deletedPaths;
  }

  public Map<PartialPath, PlanNode> getSchemaRegionSpiltMap() {
    return schemaRegionSpiltMap;
  }

  public Map<PartialPath, List<PlanNode>> getDataRegionSplitMap() {
    return dataRegionSplitMap;
  }

  @Override
  protected void serializeAttributes(ByteBuffer buffer) {
    PlanNodeType.DELETE_TIMESERIES.serialize(buffer);
    ReadWriteIOUtils.writeStringList(
        deletedPaths.stream().map(PartialPath::getFullPath).collect(Collectors.toList()), buffer);
  }

  public static PlanNode deserialize(ByteBuffer buffer) {
    List<PartialPath> deletedPath = new ArrayList<>();
    ReadWriteIOUtils.readStringList(buffer).stream()
        .map(
            fullPath -> {
              try {
                return new PartialPath(fullPath);
              } catch (IllegalPathException e) {
                throw new IllegalArgumentException(
                    "Cannot deserialize DeleteTimeSeriesSchemaNode", e);
              }
            })
        .forEach(deletedPath::add);
    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new DeleteTimeSeriesNode(planNodeId, deletedPath);
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return null;
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    for (PartialPath deletedPath : deletedPaths) {
      String device = deletedPath.getDevice();
      TRegionReplicaSet schemaRegionReplicaSet =
          analysis.getSchemaPartitionInfo().getSchemaRegionReplicaSet(device);
      DeleteTimeSeriesSchemaNode deleteTimeSeriesSchemaNode =
          new DeleteTimeSeriesSchemaNode(this.getPlanNodeId(), deletedPath);
      deleteTimeSeriesSchemaNode.setRegionReplicaSet(schemaRegionReplicaSet);
      schemaRegionSpiltMap.putIfAbsent(deletedPath, deleteTimeSeriesSchemaNode);
      List<TRegionReplicaSet> dataRegionReplicaSets =
          analysis.getDataPartitionInfo().getDataRegionReplicaSet(device, null);
      DeleteTimeSeriesDataNode deleteTimeSeriesDataNode =
          new DeleteTimeSeriesDataNode(this.getPlanNodeId(), deletedPath);
      for (TRegionReplicaSet dataRegionReplicaSet : dataRegionReplicaSets) {
        deleteTimeSeriesDataNode.setRegionReplicaSet(dataRegionReplicaSet);
        dataRegionSplitMap
            .computeIfAbsent(deletedPath, k -> new ArrayList<>())
            .add(deleteTimeSeriesDataNode);
      }
    }
    return Collections.singletonList(this);
  }
}
