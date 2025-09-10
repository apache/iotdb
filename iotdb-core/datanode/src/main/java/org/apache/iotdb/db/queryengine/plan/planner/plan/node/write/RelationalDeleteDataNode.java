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
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;
import org.apache.iotdb.commons.exception.runtime.SerializationRunTimeException;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@SuppressWarnings({"java:S1854", "unused"})
public class RelationalDeleteDataNode extends AbstractDeleteDataNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(RelationalDeleteDataNode.class);

  /** byte: type */
  private static final int FIXED_SERIALIZED_SIZE = Short.BYTES;

  private final List<TableDeletionEntry> modEntries;

  private Collection<TRegionReplicaSet> replicaSets;

  private final String databaseName;

  public RelationalDeleteDataNode(final PlanNodeId id, final Delete delete) {
    super(id);
    this.modEntries = delete.getTableDeletionEntries();
    this.replicaSets = delete.getReplicaSets();
    this.databaseName = delete.getDatabaseName();
  }

  public RelationalDeleteDataNode(
      final PlanNodeId id, final TableDeletionEntry entry, final String databaseName) {
    super(id);
    this.modEntries = Collections.singletonList(entry);
    this.databaseName = databaseName;
  }

  public RelationalDeleteDataNode(
      final PlanNodeId id, final List<TableDeletionEntry> entries, final String databaseName) {
    super(id);
    this.modEntries = entries;
    this.databaseName = databaseName;
  }

  public RelationalDeleteDataNode(
      final PlanNodeId id, final Delete delete, final ProgressIndex progressIndex) {
    this(id, delete);
    this.progressIndex = progressIndex;
  }

  public RelationalDeleteDataNode(
      final PlanNodeId id, final Delete delete, final TRegionReplicaSet regionReplicaSet) {
    this(id, delete);
    this.regionReplicaSet = regionReplicaSet;
  }

  public RelationalDeleteDataNode(
      final PlanNodeId id,
      final TableDeletionEntry delete,
      final TRegionReplicaSet regionReplicaSet,
      final String databaseName) {
    this(id, delete, databaseName);
    this.regionReplicaSet = regionReplicaSet;
  }

  public RelationalDeleteDataNode(
      PlanNodeId id,
      List<TableDeletionEntry> deletes,
      TRegionReplicaSet regionReplicaSet,
      String databaseName) {
    this(id, deletes, databaseName);
    this.regionReplicaSet = regionReplicaSet;
  }

  public static RelationalDeleteDataNode deserializeFromWAL(DataInputStream stream)
      throws IOException {
    long searchIndex = stream.readLong();

    int entryNum = ReadWriteForEncodingUtils.readVarInt(stream);
    List<TableDeletionEntry> modEntries = new ArrayList<>(entryNum);
    for (int i = 0; i < entryNum; i++) {
      modEntries.add((TableDeletionEntry) ModEntry.createFrom(stream));
    }
    String databaseName = ReadWriteIOUtils.readVarIntString(stream);

    RelationalDeleteDataNode deleteDataNode =
        new RelationalDeleteDataNode(new PlanNodeId(""), modEntries, databaseName);
    deleteDataNode.setSearchIndex(searchIndex);
    return deleteDataNode;
  }

  public static RelationalDeleteDataNode deserializeFromWAL(ByteBuffer buffer) {
    long searchIndex = buffer.getLong();
    int entryNum = ReadWriteForEncodingUtils.readVarInt(buffer);
    List<TableDeletionEntry> modEntries = new ArrayList<>(entryNum);
    for (int i = 0; i < entryNum; i++) {
      modEntries.add((TableDeletionEntry) ModEntry.createFrom(buffer));
    }
    String databaseName = ReadWriteIOUtils.readVarIntString(buffer);

    RelationalDeleteDataNode deleteDataNode =
        new RelationalDeleteDataNode(new PlanNodeId(""), modEntries, databaseName);
    deleteDataNode.setSearchIndex(searchIndex);
    return deleteDataNode;
  }

  public static RelationalDeleteDataNode deserialize(ByteBuffer byteBuffer) {
    int entryNum = ReadWriteForEncodingUtils.readVarInt(byteBuffer);
    List<TableDeletionEntry> modEntries = new ArrayList<>(entryNum);
    for (int i = 0; i < entryNum; i++) {
      modEntries.add((TableDeletionEntry) ModEntry.createFrom(byteBuffer));
    }
    String databaseName = ReadWriteIOUtils.readVarIntString(byteBuffer);

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);

    // DeleteDataNode has no child
    int ignoredChildrenSize = ReadWriteIOUtils.readInt(byteBuffer);
    RelationalDeleteDataNode relationalDeleteDataNode =
        new RelationalDeleteDataNode(planNodeId, modEntries, databaseName);
    return relationalDeleteDataNode;
  }

  public static RelationalDeleteDataNode deserializeFromDAL(ByteBuffer byteBuffer) {
    // notice that the type is deserialized here, may move it outside
    short nodeType = byteBuffer.getShort();
    int entryNum = ReadWriteForEncodingUtils.readVarInt(byteBuffer);
    List<TableDeletionEntry> modEntries = new ArrayList<>(entryNum);
    for (int i = 0; i < entryNum; i++) {
      modEntries.add((TableDeletionEntry) ModEntry.createFrom(byteBuffer));
    }
    String databaseName = ReadWriteIOUtils.readVarIntString(byteBuffer);

    ProgressIndex deserializedIndex = ProgressIndexType.deserializeFrom(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);

    // DeleteDataNode has no child
    int ignoredChildrenSize = ReadWriteIOUtils.readInt(byteBuffer);
    RelationalDeleteDataNode relationalDeleteDataNode =
        new RelationalDeleteDataNode(planNodeId, modEntries, databaseName);
    relationalDeleteDataNode.setProgressIndex(deserializedIndex);
    return relationalDeleteDataNode;
  }

  @Override
  public ByteBuffer serializeToDAL() {
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      DeleteNodeType.RELATIONAL_DELETE_NODE.serialize(outputStream);
      serializeAttributes(outputStream);
      progressIndex.serialize(outputStream);
      id.serialize(outputStream);
      // write children nodes size
      ReadWriteIOUtils.write(0, outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    } catch (IOException e) {
      throw new SerializationRunTimeException(e);
    }
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.RELATIONAL_DELETE_DATA;
  }

  @SuppressWarnings({"java:S2975", "java:S1182"})
  @Override
  public PlanNode clone() {
    return new RelationalDeleteDataNode(getPlanNodeId(), modEntries, databaseName);
  }

  @Override
  public int serializedSize() {
    int size = FIXED_SERIALIZED_SIZE + ReadWriteForEncodingUtils.varIntSize(modEntries.size());
    for (TableDeletionEntry modEntry : modEntries) {
      size += modEntry.serializedSize();
    }
    return size;
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    buffer.putShort(PlanNodeType.RELATIONAL_DELETE_DATA.getNodeType());
    buffer.putLong(searchIndex);
    try {
      ReadWriteForEncodingUtils.writeVarInt(modEntries.size(), buffer);
      for (TableDeletionEntry modEntry : modEntries) {
        modEntry.serialize(buffer);
      }
    } catch (IOException e) {
      LOGGER.error("Failed to serialize modEntry to WAL", e);
    }
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.RELATIONAL_DELETE_DATA.serialize(byteBuffer);
    ReadWriteForEncodingUtils.writeVarInt(modEntries.size(), byteBuffer);
    modEntries.forEach(entry -> entry.serialize(byteBuffer));
    ReadWriteIOUtils.writeVar(databaseName, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.RELATIONAL_DELETE_DATA.serialize(stream);
    ReadWriteForEncodingUtils.writeVarInt(modEntries.size(), stream);
    for (TableDeletionEntry modEntry : modEntries) {
      modEntry.serialize(stream);
    }
    ReadWriteIOUtils.writeVar(databaseName, stream);
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
    final RelationalDeleteDataNode that = (RelationalDeleteDataNode) obj;
    return this.getPlanNodeId().equals(that.getPlanNodeId())
        && Objects.equals(this.modEntries, that.modEntries);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getPlanNodeId(), modEntries, progressIndex);
  }

  public String toString() {
    return String.format(
        "RelationalDeleteDataNode-%s[ Deletion: %s, Region: %s, ProgressIndex: %s, SearchIndex: %d]",
        getPlanNodeId(),
        modEntries,
        regionReplicaSet == null ? "Not Assigned" : regionReplicaSet.getRegionId(),
        progressIndex == null ? "Not Assigned" : progressIndex,
        searchIndex);
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    return replicaSets.stream()
        .map(r -> new RelationalDeleteDataNode(getPlanNodeId(), modEntries, r, databaseName))
        .collect(Collectors.toList());
  }

  public List<TableDeletionEntry> getModEntries() {
    return modEntries;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public SearchNode merge(List<SearchNode> searchNodes) {
    List<RelationalDeleteDataNode> relationalDeleteDataNodeList =
        searchNodes.stream()
            .map(searchNode -> (RelationalDeleteDataNode) searchNode)
            .collect(Collectors.toList());
    if (relationalDeleteDataNodeList.stream()
        .anyMatch(
            relationalDeleteDataNode ->
                this.getDatabaseName() != null
                    && !this.getDatabaseName()
                        .equals(relationalDeleteDataNode.getDatabaseName()))) {
      throw new IllegalArgumentException("All database name need to be same");
    }
    List<TableDeletionEntry> allTableDeletionEntries =
        relationalDeleteDataNodeList.stream()
            .map(RelationalDeleteDataNode::getModEntries)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    return new RelationalDeleteDataNode(this.getPlanNodeId(), allTableDeletionEntries, databaseName)
        .setSearchIndex(getSearchIndex());
  }
}
