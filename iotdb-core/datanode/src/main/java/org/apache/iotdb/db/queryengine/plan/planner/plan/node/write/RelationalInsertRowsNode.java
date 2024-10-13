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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RelationalInsertRowsNode extends InsertRowsNode {
  // deviceId cache for Table-view insertion
  private IDeviceID[] deviceIDs;

  public RelationalInsertRowsNode(PlanNodeId id) {
    super(id);
  }

  public RelationalInsertRowsNode(
      PlanNodeId id, List<Integer> insertRowNodeIndexList, List<InsertRowNode> insertRowNodeList) {
    super(id);
    this.setInsertRowNodeIndexList(insertRowNodeIndexList);
    this.setInsertRowNodeList(insertRowNodeList);
  }

  public IDeviceID getDeviceID(int rowIdx) {
    if (deviceIDs == null) {
      deviceIDs = new IDeviceID[getInsertRowNodeList().size()];
    }
    if (deviceIDs[rowIdx] == null) {
      String[] deviceIdSegments = new String[idColumnIndices.size() + 1];
      deviceIdSegments[0] = this.getTableName();
      for (int i = 0; i < idColumnIndices.size(); i++) {
        final Integer columnIndex = idColumnIndices.get(i);
        deviceIdSegments[i + 1] =
            ((Object[]) getInsertRowNodeList().get(i).getValues()[columnIndex])[rowIdx].toString();
      }
      deviceIDs[rowIdx] = Factory.DEFAULT_FACTORY.create(deviceIdSegments);
    }

    return deviceIDs[rowIdx];
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitRelationalInsertRows(this, context);
  }

  public static RelationalInsertRowsNode deserialize(ByteBuffer byteBuffer) {
    PlanNodeId planNodeId;
    List<InsertRowNode> insertRowNodeList = new ArrayList<>();
    List<Integer> insertRowNodeIndex = new ArrayList<>();

    int size = byteBuffer.getInt();
    for (int i = 0; i < size; i++) {
      RelationalInsertRowNode insertRowNode = new RelationalInsertRowNode(new PlanNodeId(""));
      insertRowNode.subDeserialize(byteBuffer);
      insertRowNodeList.add(insertRowNode);
    }
    for (int i = 0; i < size; i++) {
      insertRowNodeIndex.add(byteBuffer.getInt());
    }

    planNodeId = PlanNodeId.deserialize(byteBuffer);
    for (InsertRowNode insertRowNode : insertRowNodeList) {
      insertRowNode.setPlanNodeId(planNodeId);
    }

    RelationalInsertRowsNode insertRowsNode = new RelationalInsertRowsNode(planNodeId);
    insertRowsNode.setInsertRowNodeList(insertRowNodeList);
    insertRowsNode.setInsertRowNodeIndexList(insertRowNodeIndex);
    return insertRowsNode;
  }

  /**
   * Deserialize from wal.
   *
   * @param stream - DataInputStream
   * @return InsertRowNode
   * @throws IOException - If an I/O error occurs.
   * @throws IllegalArgumentException - If meets illegal argument.
   */
  public static RelationalInsertRowsNode deserializeFromWAL(DataInputStream stream)
      throws IOException {
    // we do not store plan node id in wal entry
    RelationalInsertRowsNode insertRowsNode = new RelationalInsertRowsNode(new PlanNodeId(""));
    long searchIndex = stream.readLong();
    int listSize = stream.readInt();
    for (int i = 0; i < listSize; i++) {
      RelationalInsertRowNode insertRowNode = RelationalInsertRowNode.subDeserializeFromWAL(stream);
      insertRowsNode.addOneInsertRowNode(insertRowNode, i);
    }
    insertRowsNode.setSearchIndex(searchIndex);
    return insertRowsNode;
  }

  /**
   * Deserialize from wal.
   *
   * @param buffer - ByteBuffer
   * @return InsertRowNode
   * @throws IllegalArgumentException - If meets illegal argument
   */
  public static RelationalInsertRowsNode deserializeFromWAL(ByteBuffer buffer) {
    // we do not store plan node id in wal entry
    RelationalInsertRowsNode insertRowsNode = new RelationalInsertRowsNode(new PlanNodeId(""));
    long searchIndex = buffer.getLong();
    int listSize = buffer.getInt();
    for (int i = 0; i < listSize; i++) {
      RelationalInsertRowNode insertRowNode = RelationalInsertRowNode.subDeserializeFromWAL(buffer);
      insertRowsNode.addOneInsertRowNode(insertRowNode, i);
    }
    insertRowsNode.setSearchIndex(searchIndex);
    return insertRowsNode;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.RELATIONAL_INSERT_ROWS;
  }

  public String getTableName() {
    if (targetPath != null) {
      return targetPath.getFullPath();
    }
    return getInsertRowNodeList().get(0).getTableName();
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    Map<TRegionReplicaSet, RelationalInsertRowsNode> splitMap = new HashMap<>();
    List<TEndPoint> redirectInfo = new ArrayList<>();
    for (int i = 0; i < getInsertRowNodeList().size(); i++) {
      InsertRowNode insertRowNode = getInsertRowNodeList().get(i);
      // Data region for insert row node
      // each row may belong to different database, pass null for auto-detection
      TRegionReplicaSet dataRegionReplicaSet =
          analysis
              .getDataPartitionInfo()
              .getDataRegionReplicaSetForWriting(
                  insertRowNode.getDeviceID(),
                  TimePartitionUtils.getTimePartitionSlot(insertRowNode.getTime()),
                  analysis.getDatabaseName());
      // Collect redirectInfo
      redirectInfo.add(dataRegionReplicaSet.getDataNodeLocations().get(0).getClientRpcEndPoint());
      RelationalInsertRowsNode tmpNode = splitMap.get(dataRegionReplicaSet);
      if (tmpNode != null) {
        tmpNode.addOneInsertRowNode(insertRowNode, i);
      } else {
        tmpNode = new RelationalInsertRowsNode(this.getPlanNodeId());
        tmpNode.setDataRegionReplicaSet(dataRegionReplicaSet);
        tmpNode.addOneInsertRowNode(insertRowNode, i);
        splitMap.put(dataRegionReplicaSet, tmpNode);
      }
    }
    analysis.setRedirectNodeList(redirectInfo);

    return new ArrayList<>(splitMap.values());
  }

  public RelationalInsertRowsNode emptyClone() {
    return new RelationalInsertRowsNode(this.getPlanNodeId());
  }
}
