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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;

import org.apache.tsfile.exception.NotImplementedException;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InsertRowsNode extends InsertNode implements WALEntryValue {

  /**
   * Suppose there is an InsertRowsNode, which contains 5 InsertRowNodes,
   * insertRowNodeList={InsertRowNode_0, InsertRowNode_1, InsertRowNode_2, InsertRowNode_3,
   * InsertRowNode_4}, then the insertRowNodeIndexList={0, 1, 2, 3, 4} respectively. But when the
   * InsertRowsNode is split into two InsertRowsNodes according to different database in cluster
   * version, suppose that the InsertRowsNode_1's insertRowNodeList = {InsertRowNode_0,
   * InsertRowNode_3, InsertRowNode_4}, then InsertRowsNode_1's insertRowNodeIndexList = {0, 3, 4};
   * InsertRowsNode_2's insertRowNodeList = {InsertRowNode_1, * InsertRowNode_2} then
   * InsertRowsNode_2's insertRowNodeIndexList= {1, 2} respectively;
   */
  private List<Integer> insertRowNodeIndexList;

  /** The {@link InsertRowNode} list */
  private List<InsertRowNode> insertRowNodeList;

  private boolean isMixingAlignment = false;

  public InsertRowsNode(PlanNodeId id) {
    super(id);
    insertRowNodeList = new ArrayList<>();
    insertRowNodeIndexList = new ArrayList<>();
  }

  public InsertRowsNode(
      PlanNodeId id, List<Integer> insertRowNodeIndexList, List<InsertRowNode> insertRowNodeList) {
    super(id);
    this.insertRowNodeIndexList = insertRowNodeIndexList;
    this.insertRowNodeList = insertRowNodeList;
  }

  /** Record the result of insert rows */
  private Map<Integer, TSStatus> results = new HashMap<>();

  public List<Integer> getInsertRowNodeIndexList() {
    return insertRowNodeIndexList;
  }

  public void setInsertRowNodeIndexList(List<Integer> insertRowNodeIndexList) {
    this.insertRowNodeIndexList = insertRowNodeIndexList;
  }

  public List<InsertRowNode> getInsertRowNodeList() {
    return insertRowNodeList;
  }

  public void setInsertRowNodeList(List<InsertRowNode> insertRowNodeList) {
    this.insertRowNodeList = insertRowNodeList;
  }

  public void addOneInsertRowNode(InsertRowNode node, int index) {
    insertRowNodeList.add(node);
    insertRowNodeIndexList.add(index);
  }

  public boolean isMixingAlignment() {
    return isMixingAlignment;
  }

  public void setMixingAlignment(boolean mixingAlignment) {
    isMixingAlignment = mixingAlignment;
  }

  @Override
  public void setSearchIndex(long index) {
    searchIndex = index;
    insertRowNodeList.forEach(plan -> plan.setSearchIndex(index));
  }

  public Map<Integer, TSStatus> getResults() {
    return results;
  }

  public void clearResults() {
    results.clear();
  }

  public TSStatus[] getFailingStatus() {
    return StatusUtils.getFailingStatus(results, insertRowNodeList.size());
  }

  @Override
  public void addChild(PlanNode child) {
    // Do nothing
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.INSERT_ROWS;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    InsertRowsNode that = (InsertRowsNode) o;
    return Objects.equals(insertRowNodeIndexList, that.insertRowNodeIndexList)
        && Objects.equals(insertRowNodeList, that.insertRowNodeList);
  }

  @Override
  public String toString() {
    return "InsertRowsNode{"
        + "insertRowNodeIndexList="
        + insertRowNodeIndexList
        + ", insertRowNodeList="
        + insertRowNodeList
        + '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), insertRowNodeIndexList, insertRowNodeList);
  }

  @Override
  public PlanNode clone() {
    throw new NotImplementedException("clone of Insert is not implemented");
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return Collections.emptyList();
  }

  public static InsertRowsNode deserialize(ByteBuffer byteBuffer) {
    PlanNodeId planNodeId;
    List<InsertRowNode> insertRowNodeList = new ArrayList<>();
    List<Integer> insertRowNodeIndex = new ArrayList<>();

    int size = byteBuffer.getInt();
    for (int i = 0; i < size; i++) {
      InsertRowNode insertRowNode = new InsertRowNode(new PlanNodeId(""));
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

    InsertRowsNode insertRowsNode = new InsertRowsNode(planNodeId);
    insertRowsNode.setInsertRowNodeList(insertRowNodeList);
    insertRowsNode.setInsertRowNodeIndexList(insertRowNodeIndex);
    return insertRowsNode;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    getType().serialize(byteBuffer);

    ReadWriteIOUtils.write(insertRowNodeList.size(), byteBuffer);

    for (InsertRowNode node : insertRowNodeList) {
      node.subSerialize(byteBuffer);
    }
    for (Integer index : insertRowNodeIndexList) {
      ReadWriteIOUtils.write(index, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    getType().serialize(stream);

    ReadWriteIOUtils.write(insertRowNodeList.size(), stream);

    for (InsertRowNode node : insertRowNodeList) {
      node.subSerialize(stream);
    }
    for (Integer index : insertRowNodeIndexList) {
      ReadWriteIOUtils.write(index, stream);
    }
  }

  @Override
  public void markAsGeneratedByPipe() {
    isGeneratedByPipe = true;
    insertRowNodeList.forEach(InsertRowNode::markAsGeneratedByPipe);
  }

  @Override
  public void markAsGeneratedByRemoteConsensusLeader() {
    super.markAsGeneratedByRemoteConsensusLeader();
    insertRowNodeList.forEach(InsertRowNode::markAsGeneratedByRemoteConsensusLeader);
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    Map<TRegionReplicaSet, InsertRowsNode> splitMap = new HashMap<>();
    List<TEndPoint> redirectInfo = new ArrayList<>();
    for (int i = 0; i < insertRowNodeList.size(); i++) {
      InsertRowNode insertRowNode = insertRowNodeList.get(i);
      // Data region for insert row node
      // each row may belong to different database, pass null for auto-detection
      TRegionReplicaSet dataRegionReplicaSet =
          analysis
              .getDataPartitionInfo()
              .getDataRegionReplicaSetForWriting(
                  insertRowNode.targetPath.getIDeviceIDAsFullDevice(),
                  TimePartitionUtils.getTimePartitionSlot(insertRowNode.getTime()),
                  null);
      // Collect redirectInfo
      redirectInfo.add(dataRegionReplicaSet.getDataNodeLocations().get(0).getClientRpcEndPoint());
      InsertRowsNode tmpNode = splitMap.get(dataRegionReplicaSet);
      if (tmpNode != null) {
        tmpNode.addOneInsertRowNode(insertRowNode, i);
      } else {
        tmpNode = new InsertRowsNode(this.getPlanNodeId());
        tmpNode.setDataRegionReplicaSet(dataRegionReplicaSet);
        tmpNode.addOneInsertRowNode(insertRowNode, i);
        splitMap.put(dataRegionReplicaSet, tmpNode);
      }
    }
    analysis.setRedirectNodeList(redirectInfo);

    return new ArrayList<>(splitMap.values());
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitInsertRows(this, context);
  }

  @Override
  public long getMinTime() {
    return insertRowNodeList.stream()
        .map(InsertRowNode::getMinTime)
        .reduce(Long::min)
        .orElse(Long.MAX_VALUE);
  }

  @Override
  public void setProgressIndex(ProgressIndex progressIndex) {
    this.progressIndex = progressIndex;
    insertRowNodeList.forEach(insertRowNode -> insertRowNode.setProgressIndex(progressIndex));
  }

  public void updateProgressIndex(ProgressIndex progressIndex) {
    if (progressIndex == null) {
      return;
    }

    this.progressIndex =
        (this.progressIndex == null)
            ? progressIndex
            : this.progressIndex.updateToMinimumEqualOrIsAfterProgressIndex(progressIndex);
  }

  // region serialize & deserialize methods for WAL
  /** Serialized size for wal. */
  @Override
  public int serializedSize() {
    return Short.BYTES + Long.BYTES + subSerializeSize();
  }

  private int subSerializeSize() {
    int size = Integer.BYTES;
    for (InsertRowNode insertRowNode : insertRowNodeList) {
      size += insertRowNode.subSerializeSize();
    }
    return size;
  }

  /**
   * Compared with {@link this#serialize(ByteBuffer)}, more info: search index, less info:
   * isNeedInferType
   */
  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    buffer.putShort(getType().getNodeType());
    buffer.putLong(searchIndex);
    subSerialize(buffer);
  }

  private void subSerialize(IWALByteBufferView buffer) {
    buffer.putInt(insertRowNodeList.size());
    for (InsertRowNode insertRowNode : insertRowNodeList) {
      insertRowNode.subSerialize(buffer);
    }
  }

  /**
   * Deserialize from wal.
   *
   * @param stream - DataInputStream
   * @return InsertRowNode
   * @throws IOException - If an I/O error occurs.
   * @throws IllegalArgumentException - If meets illegal argument.
   */
  public static InsertRowsNode deserializeFromWAL(DataInputStream stream) throws IOException {
    // we do not store plan node id in wal entry
    InsertRowsNode insertRowsNode = new InsertRowsNode(new PlanNodeId(""));
    long searchIndex = stream.readLong();
    int listSize = stream.readInt();
    for (int i = 0; i < listSize; i++) {
      InsertRowNode insertRowNode = InsertRowNode.subDeserializeFromWAL(stream);
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
  public static InsertRowsNode deserializeFromWAL(ByteBuffer buffer) {
    // we do not store plan node id in wal entry
    InsertRowsNode insertRowsNode = new InsertRowsNode(new PlanNodeId(""));
    long searchIndex = buffer.getLong();
    int listSize = buffer.getInt();
    for (int i = 0; i < listSize; i++) {
      InsertRowNode insertRowNode = InsertRowNode.subDeserializeFromWAL(buffer);
      insertRowsNode.addOneInsertRowNode(insertRowNode, i);
    }
    insertRowsNode.setSearchIndex(searchIndex);
    return insertRowsNode;
  }

  public InsertRowsNode emptyClone() {
    return new InsertRowsNode(this.getPlanNodeId());
  }

  // endregion
}
