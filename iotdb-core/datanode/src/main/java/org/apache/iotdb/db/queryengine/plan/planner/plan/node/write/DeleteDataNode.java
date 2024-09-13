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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.write;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.queryengine.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.queryengine.common.schematree.ISchemaTree;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;

import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;

public class DeleteDataNode extends SearchNode implements WALEntryValue {
  /** byte: type, integer: pathList.size(), long: deleteStartTime, deleteEndTime, searchIndex */
  private static final int FIXED_SERIALIZED_SIZE = Short.BYTES + Integer.BYTES + Long.BYTES * 3;

  private final List<MeasurementPath> pathList;
  private final long deleteStartTime;
  private final long deleteEndTime;

  private TRegionReplicaSet regionReplicaSet;

  public DeleteDataNode(
      PlanNodeId id, List<MeasurementPath> pathList, long deleteStartTime, long deleteEndTime) {
    super(id);
    this.pathList = pathList;
    this.deleteStartTime = deleteStartTime;
    this.deleteEndTime = deleteEndTime;
  }

  public DeleteDataNode(
      PlanNodeId id,
      List<MeasurementPath> pathList,
      long deleteStartTime,
      long deleteEndTime,
      TRegionReplicaSet regionReplicaSet) {
    super(id);
    this.pathList = pathList;
    this.deleteStartTime = deleteStartTime;
    this.deleteEndTime = deleteEndTime;
    this.regionReplicaSet = regionReplicaSet;
  }

  public List<MeasurementPath> getPathList() {
    return pathList;
  }

  public long getDeleteStartTime() {
    return deleteStartTime;
  }

  public long getDeleteEndTime() {
    return deleteEndTime;
  }

  @Override
  public List<PlanNode> getChildren() {
    return new ArrayList<>();
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.DELETE_DATA;
  }

  @Override
  public PlanNode clone() {
    return new DeleteDataNode(getPlanNodeId(), pathList, deleteStartTime, deleteEndTime);
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
  public int serializedSize() {
    int size = FIXED_SERIALIZED_SIZE;
    for (PartialPath path : pathList) {
      size += ReadWriteIOUtils.sizeToWrite(path.getFullPath());
    }
    return size;
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    buffer.putShort(PlanNodeType.DELETE_DATA.getNodeType());
    buffer.putLong(searchIndex);
    buffer.putInt(pathList.size());
    for (PartialPath path : pathList) {
      WALWriteUtils.write(path.getFullPath(), buffer);
    }
    buffer.putLong(deleteStartTime);
    buffer.putLong(deleteEndTime);
  }

  public static DeleteDataNode deserializeFromWAL(DataInputStream stream) throws IOException {
    long searchIndex = stream.readLong();
    int size = stream.readInt();
    List<MeasurementPath> pathList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      try {
        pathList.add(new MeasurementPath(ReadWriteIOUtils.readString(stream)));
      } catch (IllegalPathException e) {
        throw new IllegalArgumentException("Cannot deserialize InsertRowNode", e);
      }
    }
    long deleteStartTime = stream.readLong();
    long deleteEndTime = stream.readLong();

    DeleteDataNode deleteDataNode =
        new DeleteDataNode(new PlanNodeId(""), pathList, deleteStartTime, deleteEndTime);
    deleteDataNode.setSearchIndex(searchIndex);
    return deleteDataNode;
  }

  public static DeleteDataNode deserializeFromWAL(ByteBuffer buffer) {
    long searchIndex = buffer.getLong();
    int size = buffer.getInt();
    List<MeasurementPath> pathList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      try {
        pathList.add(new MeasurementPath(ReadWriteIOUtils.readString(buffer)));
      } catch (IllegalPathException e) {
        throw new IllegalArgumentException("Cannot deserialize InsertRowNode", e);
      }
    }
    long deleteStartTime = buffer.getLong();
    long deleteEndTime = buffer.getLong();

    DeleteDataNode deleteDataNode =
        new DeleteDataNode(new PlanNodeId(""), pathList, deleteStartTime, deleteEndTime);
    deleteDataNode.setSearchIndex(searchIndex);
    return deleteDataNode;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DELETE_DATA.serialize(byteBuffer);
    ReadWriteIOUtils.write(pathList.size(), byteBuffer);
    for (PartialPath path : pathList) {
      path.serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(deleteStartTime, byteBuffer);
    ReadWriteIOUtils.write(deleteEndTime, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.DELETE_DATA.serialize(stream);
    ReadWriteIOUtils.write(pathList.size(), stream);
    for (PartialPath path : pathList) {
      path.serialize(stream);
    }
    ReadWriteIOUtils.write(deleteStartTime, stream);
    ReadWriteIOUtils.write(deleteEndTime, stream);
  }

  public static DeleteDataNode deserialize(ByteBuffer byteBuffer) {
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<MeasurementPath> pathList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      pathList.add((MeasurementPath) PathDeserializeUtil.deserialize(byteBuffer));
    }
    long deleteStartTime = ReadWriteIOUtils.readLong(byteBuffer);
    long deleteEndTime = ReadWriteIOUtils.readLong(byteBuffer);

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);

    // DeleteDataNode has no child
    int ignoredChildrenSize = ReadWriteIOUtils.readInt(byteBuffer);
    return new DeleteDataNode(planNodeId, pathList, deleteStartTime, deleteEndTime);
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

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final DeleteDataNode that = (DeleteDataNode) obj;
    return this.getPlanNodeId().equals(that.getPlanNodeId())
        && Objects.equals(this.pathList, that.pathList)
        && Objects.equals(this.deleteStartTime, that.deleteStartTime)
        && Objects.equals(this.deleteEndTime, that.deleteEndTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getPlanNodeId(), pathList, deleteStartTime, deleteEndTime);
  }

  public String toString() {
    return String.format(
        "DeleteDataNode-%s[ Paths: %s, Region: %s ]",
        getPlanNodeId(),
        pathList,
        regionReplicaSet == null ? "Not Assigned" : regionReplicaSet.getRegionId());
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    ISchemaTree schemaTree = ((Analysis) analysis).getSchemaTree();
    DataPartition dataPartition = analysis.getDataPartitionInfo();

    Map<TRegionReplicaSet, List<MeasurementPath>> regionToPatternMap = new HashMap<>();

    for (MeasurementPath pathPattern : pathList) {
      if (pathPattern.getTailNode().equals(MULTI_LEVEL_PATH_WILDCARD)) {
        splitPathPatternByDevice(
            pathPattern, pathPattern, schemaTree, dataPartition, regionToPatternMap);
      }
      splitPathPatternByDevice(
          pathPattern.getDevicePath(), pathPattern, schemaTree, dataPartition, regionToPatternMap);
    }

    return regionToPatternMap.keySet().stream()
        .map(
            o ->
                new DeleteDataNode(
                    getPlanNodeId(),
                    // Pick the smaller path list to execute the deletion.
                    // E.g. There is only one path(root.sg.**) in pathList and two paths(root.sg.d1,
                    // root.sg.d2) in a map entry in regionToPatternMap. Choose the original path is
                    // better.
                    this.pathList.size() < regionToPatternMap.get(o).size()
                        ? this.pathList
                        : regionToPatternMap.get(o),
                    deleteStartTime,
                    deleteEndTime,
                    o))
        .collect(Collectors.toList());
  }

  private void splitPathPatternByDevice(
      PartialPath devicePattern,
      MeasurementPath pathPattern,
      ISchemaTree schemaTree,
      DataPartition dataPartition,
      Map<TRegionReplicaSet, List<MeasurementPath>> regionToPatternMap) {
    for (DeviceSchemaInfo deviceSchemaInfo : schemaTree.getMatchedDevices(devicePattern)) {
      PartialPath devicePath = deviceSchemaInfo.getDevicePath();
      // regionId is null when data region of devicePath not existed
      dataPartition
          .getDataRegionReplicaSetWithTimeFilter(
              devicePath.getIDeviceIDAsFullDevice(),
              TimeFilterApi.between(deleteStartTime, deleteEndTime))
          .stream()
          .filter(regionReplicaSet -> regionReplicaSet.getRegionId() != null)
          .forEach(
              regionReplicaSet ->
                  regionToPatternMap
                      .computeIfAbsent(regionReplicaSet, o -> new ArrayList<>())
                      .addAll(
                          pathPattern.alterPrefixPath(devicePath).stream()
                              .map(d -> (MeasurementPath) d)
                              .collect(Collectors.toList())));
    }
  }
}
