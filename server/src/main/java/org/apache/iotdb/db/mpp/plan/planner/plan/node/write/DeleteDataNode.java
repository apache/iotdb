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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.wal.utils.WALWriteUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode.NO_CONSENSUS_INDEX;

public class DeleteDataNode extends WritePlanNode implements WALEntryValue {
  /** byte: type, integer: pathList.size(), long: deleteStartTime, deleteEndTime, searchIndex */
  private static final int FIXED_SERIALIZED_SIZE = Short.BYTES + Integer.BYTES + Long.BYTES * 3;

  private final List<PartialPath> pathList;
  private final long deleteStartTime;
  private final long deleteEndTime;

  /**
   * this index is used by wal search, its order should be protected by the upper layer, and the
   * value should start from 1
   */
  protected long searchIndex = NO_CONSENSUS_INDEX;

  private TRegionReplicaSet regionReplicaSet;

  public DeleteDataNode(
      PlanNodeId id, List<PartialPath> pathList, long deleteStartTime, long deleteEndTime) {
    super(id);
    this.pathList = pathList;
    this.deleteStartTime = deleteStartTime;
    this.deleteEndTime = deleteEndTime;
  }

  public DeleteDataNode(
      PlanNodeId id,
      List<PartialPath> pathList,
      long deleteStartTime,
      long deleteEndTime,
      TRegionReplicaSet regionReplicaSet) {
    super(id);
    this.pathList = pathList;
    this.deleteStartTime = deleteStartTime;
    this.deleteEndTime = deleteEndTime;
    this.regionReplicaSet = regionReplicaSet;
  }

  public List<PartialPath> getPathList() {
    return pathList;
  }

  public long getDeleteStartTime() {
    return deleteStartTime;
  }

  public long getDeleteEndTime() {
    return deleteEndTime;
  }

  public long getSearchIndex() {
    return searchIndex;
  }

  /** Search index should start from 1 */
  public void setSearchIndex(long searchIndex) {
    this.searchIndex = searchIndex;
  }

  @Override
  public List<PlanNode> getChildren() {
    return new ArrayList<>();
  }

  @Override
  public void addChild(PlanNode child) {}

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
    List<PartialPath> pathList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      try {
        pathList.add(new PartialPath(ReadWriteIOUtils.readString(stream)));
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
    List<PartialPath> pathList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      try {
        pathList.add(new PartialPath(ReadWriteIOUtils.readString(buffer)));
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
    List<PartialPath> pathList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      pathList.add((PartialPath) PathDeserializeUtil.deserialize(byteBuffer));
    }
    long deleteStartTime = ReadWriteIOUtils.readLong(byteBuffer);
    long deleteEndTime = ReadWriteIOUtils.readLong(byteBuffer);

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
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

  public String toString() {
    return String.format(
        "DeleteDataNode-%s[ Paths: %s, Region: %s ]",
        getPlanNodeId(),
        pathList,
        regionReplicaSet == null ? "Not Assigned" : regionReplicaSet.getRegionId());
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    ISchemaTree schemaTree = analysis.getSchemaTree();
    DataPartition dataPartition = analysis.getDataPartitionInfo();

    Map<TRegionReplicaSet, List<PartialPath>> regionToPatternMap = new HashMap<>();

    for (PartialPath pathPattern : pathList) {
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
                    getPlanNodeId(), regionToPatternMap.get(o), deleteStartTime, deleteEndTime, o))
        .collect(Collectors.toList());
  }

  private void splitPathPatternByDevice(
      PartialPath devicePattern,
      PartialPath pathPattern,
      ISchemaTree schemaTree,
      DataPartition dataPartition,
      Map<TRegionReplicaSet, List<PartialPath>> regionToPatternMap) {
    for (DeviceSchemaInfo deviceSchemaInfo : schemaTree.getMatchedDevices(devicePattern)) {
      PartialPath devicePath = deviceSchemaInfo.getDevicePath();
      // todo implement time slot
      for (TRegionReplicaSet regionReplicaSet :
          dataPartition.getDataRegionReplicaSet(
              devicePath.getFullPath(), Collections.emptyList())) {
        // regionId is null when data region of devicePath not existed
        if (regionReplicaSet.getRegionId() != null) {
          regionToPatternMap
              .computeIfAbsent(regionReplicaSet, o -> new ArrayList<>())
              .addAll(pathPattern.alterPrefixPath(devicePath));
        }
      }
    }
  }
}
